import json
import logging
import mimetypes
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone

import jmespath
import requests

from starlette import status

from src.acp.bridge import Bridge, TargetDataModel
from src.acp.commons import (
    app_settings,
    transform,
    handle_deposit_exceptions, dmz_dataverse_headers, zip_a_zipfile_with_progress, transform_xml,
    processed_metadata_handler, validate_json, delete_symlink_and_target, calculate_sha1_checksum
)
from src.acp.db.dbz import StateVersion, DataFile, DepositStatus, MetadataType, AccessLevel, DataFileState, \
    IngestFileStatus
from src.acp.models.bridge_output_model import IdentifierItem, IdentifierProtocol, TargetResponse, ResponseContentType


class DataverseIngester(Bridge):
    """
    Class for ingesting metadata and files into Dataverse.
    The following is an example of the configuration for the Dataverse ingester in Repository Assistant:
    "dataset-metadata.json" is the default name for the dataverse metadata. It corresponds to the repoassistant configuration.
    "transformed-metadata": [
                        {
                           "name": "dataset-metadata.json",
                            "transformer-url": "http://localhost:1745/transform/ohsmart-form-metadata-to-DV-metadata-v4.xsl",
                            "target-dir": "metadata"
                        }

    "dataset-metadata.json" is the default name for the dataverse metadata. It corresponds to the repoassistant configuration.
    "transformed-metadata": [
                         {
                             "name": "dataset-files.json",
                          "transformer-url": "http://localhost:1745/transform/ohsmart-form-metadata-to-DV-metadata-v4.xsl",
                             "target-dir": "metadata"
                        }
    Methods:
        execute(): Executes the ingestion process and returns the result as a BridgeOutputDataModel.
    """

    @handle_deposit_exceptions
    def job(self) -> TargetDataModel:

        target_repo_response = TargetResponse(url=self.target.target_url)
        tdm = TargetDataModel(response=target_repo_response)
        dv_headers = dmz_dataverse_headers('API_KEY', self.target.password)
        if self.dataset_rec.metadata_type == MetadataType.JSON:
            md_json = json.loads(self.dataset_rec.metadata_content)

            if self.target.input:
                logging.info(f'Processing input: {self.target.input}')
                prev_target = self.db_manager.find_target_repo(self.dataset_id, self.target.input.from_target_name)
                md_json[self.target.input.from_target_name] = json.loads(prev_target.target_service_response)['response']['identifiers'][0]['value']
                self.dataset_rec.metadata_content = json.dumps(md_json, indent=2)
                self.db_manager.update_dataset(self.dataset_rec)

            md_json, str_updated_metadata = self.__handle_metadata_transformation(md_json)

        else:
            str_updated_metadata = self.dataset_rec.metadata_content

        logging.debug(f"str_updated_metadata_json: {str_updated_metadata}")

        # The metadata will be transformed if name is "dataset-metadata.json" and the transformed metadata is available.
        try:
            str_dv_metadata = self.__transform_metadata_to_dataverse_json(str_updated_metadata,
                                                                          app_settings.get("DV_METADATA", "dataset-metadata.json"),
                                                                          self.dataset_rec.metadata_type)
        except ValueError as e:
            tdm.deposited_metadata = str(e)
            tdm.deposit_status = DepositStatus.ERROR
            return tdm
        if self.dataset_rec.metadata_type == MetadataType.JSON:
            tdm.payload = json.loads(str_dv_metadata)
            # TOOD tdm.payload for other than json metadata

        logging.info(f'Deposit to "{self.target.target_url}"')
        # If the target URL has parameters, replace the placeholder with the dataset PID. It corresponds to the repoassistant configuration.
        # "target-url-params": "pid=$PID&release=no",
        if self.target.target_url_params:
            self.target.target_url += "?" + self.target.target_url_params.replace("$PID", md_json["datasetVersion"]["datasetPersistentId"])

        if self.dataset_rec.status == StateVersion.RESUBMIT:
            dv_response, pid = self.__process_resubmit_dataset(dv_headers, str_dv_metadata, str_updated_metadata, tdm)
            self.db_manager.delete_dataset_backups_by_dataset_id(self.dataset_id)
        else:
            dv_response, pid = self.__process_submit_dataset(dv_headers, str_dv_metadata, str_updated_metadata,
                                                             target_repo_response, tdm)

        self.db_manager.update_dataset_metadata_content(self.dataset_id, str_updated_metadata)
        current_time = datetime.now(timezone.utc).isoformat()
        tdm.deposit_time = current_time
        target_repo_response.content = dv_response.json()
        target_repo_response.content_type = ResponseContentType.JSON
        target_repo_response.status_code = dv_response.status_code

        dv_resp_deposited = requests.get(f'{self.target.base_url}/api/datasets/:persistentId/?persistentId={pid}',
                                   headers=dv_headers, data=str_dv_metadata)
        if dv_resp_deposited.status_code == 200:
            tdm.deposited_metadata = dv_resp_deposited.json()
        else:
            logging.error(f"Error: {dv_resp_deposited.text} status code: {dv_resp_deposited.status_code}")
            pass #TODO: Handle this case

        tdm.response = target_repo_response
        return tdm

    def __handle_metadata_transformation(self, md_json):
        # Handle metadata transformations and updates
        if self.target.metadata:
            files_metadata = jmespath.search('"file-metadata"[*]', md_json)

            # When transformed metadata is available, transform the metadata
            # Add generated files to the metadata
            if self.target.metadata.transformed_metadata:
                generated_files, files_metadata = self.__create_generated_files(files_metadata)
                files_metadata.extend({"name": gf.name,
                                       "mimetype": gf.mime_type,
                                       "private": gf.access_level == AccessLevel.PRIVATE,
                                       "size": gf.size,
                                       "state": "generated"
                                       } for gf in generated_files)

                if generated_files:
                    self.db_manager.insert_datafiles(self.dataset_id, generated_files)

                md_json["file-metadata"] = files_metadata

            if self.target.metadata.processed_metadata:
                # When processed metadata is available, process the metadata
                md_json = processed_metadata_handler(self.target.metadata.processed_metadata, md_json)

            for file in self.db_manager.find_non_registered_files(dataset_id=self.dataset_id):
                escaped_file_name = file.name.replace('"', '\\"')
                file_metadata = jmespath.search(f'[?name == `{escaped_file_name}`]', files_metadata)
                file_metadata[0].update({"mimetype": file.mime_type, "size": file.size})
        str_updated_metadata = json.dumps(md_json)
        self.dataset_rec.metadata_content = str_updated_metadata
        self.db_manager.update_dataset(self.dataset_rec)
        return md_json, str_updated_metadata

    def __process_submit_dataset(self, dv_headers, str_dv_metadata, str_updated_metadata, target_repo_response, tdm):
        logging.info(f'Process submitting metadata {self.dataset_id} to {self.target.target_url}')
        dv_response = requests.post(self.target.target_url, headers=dv_headers, data=str_dv_metadata)
        # TODO: Check status code, then handle the case.  It must be 201
        if dv_response.status_code != status.HTTP_201_CREATED:
            msg = f"Error: {dv_response.status_code} {dv_response.text}"
            logging.error(msg)
            tdm.deposit_status = DepositStatus.ERROR
            tdm.deposit_status_message = msg
            return dv_response, None

        logging.debug(f"dv_response.status_code: {dv_response.status_code} dv_response.text: {dv_response.text}")
        identifier_items = []
        dv_response_json = dv_response.json()
        logging.debug(f"Data ingest successfully! {json.dumps(dv_response_json)}")
        pid = dv_response_json["data"]["persistentId"]
        self.__set_repo_identifiers(identifier_items, pid, target_repo_response)
        tdm.external_identifiers = identifier_items#json.dumps([i.model_dump() for i in identifier_items])
        tdm.deposited_version = "DRAFT" # TODO: Check the version:https://guides.dataverse.org/en/latest/api/native-api.html#datasets

        if self.target.metadata and self.target.metadata.transformed_metadata and self.dataset_rec.metadata_type == MetadataType.JSON:
            self.__ingest_files(pid, str_updated_metadata, dv_headers)
            logging.info('The dataset and its file is successfully ingested.')

        if self.target.initial_release_version == StateVersion.PUBLISHED:
            logging.info('Publishing the dataset...')
            target_repo_response.status_code = self.__publish_dataset(pid, dv_headers)
            tdm.deposited_version = StateVersion.PUBLISHED

        tdm.deposit_status = DepositStatus.FINISH
        return dv_response, pid

    def __process_resubmit_dataset(self, dv_headers, str_dv_metadata, str_updated_metadata, tdm):
        logging.info('processing resubmit dataset')
        target_repo_rec = self.db_manager.find_target_repo(dataset_id=self.dataset_id,
                                                           target_name=self.target.repo_name)
        #Update status to submit:
        self.db_manager.update_dataset_status(dataset_id=self.dataset_id, state=StateVersion.SUBMIT)
        pid = jmespath.search("[?protocol=='doi'].value | [0]", target_repo_rec.external_identifiers)
        md_json = json.loads(str_dv_metadata)
        md_block_only = md_json["datasetVersion"]["metadataBlocks"]
        term_of_access = md_json["datasetVersion"]["termsOfAccess"]
        license_only = md_json["datasetVersion"]["license"]
        construct_new_dv = {"license": license_only, "termsOfAccess": term_of_access, "fileAccessRequest": True,
                            "metadataBlocks": md_block_only}
        str_updated_new_dv = json.dumps(construct_new_dv)
        update_url = f'{self.target.base_url}/api/datasets/:persistentId/versions/:draft?persistentId={pid}'
        logging.debug(f"Update {update_url} with dv_json: {str_updated_new_dv}")
        dv_response = requests.put(update_url, headers=dv_headers, data=str_updated_new_dv)
        # TODO: Check status code, then handle the case
        # This is Resubmit
        dv_response_json = dv_response.json()
        if dv_response.status_code != status.HTTP_200_OK:
            msg = f"Error: {dv_response.status_code} {dv_response.text}"
            logging.error(msg)
            tdm.deposit_status = DepositStatus.ERROR
            tdm.deposit_status_message = msg
            raise ValueError(dv_response.json())

        logging.info(f"Metadata Resubmit of '{self.dataset_id}' successfully!")
        tdm.deposit_status = StateVersion.DRAFT
        tdm.deposit_status = DepositStatus.FINISH
        if self.target.metadata and self.target.metadata.transformed_metadata:
            self.__resubmit_handle_files(pid, str_updated_metadata, dv_headers)
        return dv_response, pid

    def __set_repo_identifiers(self, identifier_items, pid, target_repo):
        logging.debug('Getting repository identifiers')
        identifier_items.append(
            IdentifierItem(value=pid, url=f'{self.target.base_url}/dataset.xhtml?persistentId={pid}',
                           protocol=IdentifierProtocol('doi'), api_url=f'{self.target.base_url}/api/datasets/:persistentId?persistentId={pid}'))
        logging.info(f"pid: {pid}")
        target_repo.identifiers = identifier_items

    # When the transformation is done successfully, the transformed metadata is returned otherwise an error message is returned.
    def __transform_metadata_to_dataverse_json(self, str_updated_metadata_json, json_data_name: str, metadata_type: MetadataType = MetadataType.JSON) -> str:
        logging.debug('Transforming metadata to Dataverse JSON')
        if self.target.metadata and self.target.metadata.transformed_metadata:
            transformer = [metadata for metadata in self.target.metadata.transformed_metadata if
                           metadata.name == json_data_name]
            if not transformer or len(transformer) != 1:
                logging.error("Error: Transformer not found or more than one transformer")
                #Skip transformation
                return str_updated_metadata_json
                # raise ValueError(f"Error: Transformer '{json_data_name}' not found or more than one transformer")
            if metadata_type == MetadataType.XML:
                str_dv_metadata = transform_xml(
                    transformer_url=f'{transformer[0].transformer_url}?app_name={self.app_name}',
                    str_tobe_transformed=str_updated_metadata_json
                )
            elif metadata_type == MetadataType.JSON:
                str_dv_metadata = transform(
                    transformer_url=f'{transformer[0].transformer_url}?app_name={self.app_name}',
                    str_tobe_transformed=str_updated_metadata_json
                )

                logging.debug(f"TRANSFORMED str_dv_metadata: {str_updated_metadata_json}")

                str_dv_metadata = validate_json(str_dv_metadata)
                if not str_dv_metadata:
                    logging.error(f"Error: Not valid json: {str_dv_metadata}")
                    raise ValueError("Error: Not valid json")
            else:
                str_dv_metadata = str_updated_metadata_json
        else:
            str_dv_metadata = str_updated_metadata_json

        return str_dv_metadata

    def __create_generated_files(self, files_metadata) -> [DataFile]:
        logging.debug('Creating generated files')
        generated_files = []
        # Remove the existing generated files
        self.db_manager.delete_generated_files(self.dataset_id)
        for tm in self.target.metadata.transformed_metadata:
            if tm.generate_file:
                gf_path = os.path.join(self.dataset_dir, tm.name)
                content = transform(f'{tm.transformer_url}?app_name={self.app_name}',
                                    self.dataset_rec.metadata_content) if tm.transformer_url else self.dataset_rec.metadata_content
                with open(gf_path, "wt") as f:
                    f.write(content)
                gf_mimetype = mimetypes.guess_type(gf_path)[0]
                access_levels = AccessLevel.PRIVATE if tm.restricted else AccessLevel.PUBLIC
                generated_files.append(DataFile(
                    dataset_id=self.dataset_id, name=tm.name, path=gf_path,
                    size=os.path.getsize(gf_path), mime_type=gf_mimetype,
                    checksum=calculate_sha1_checksum(gf_path),
                    added_at=datetime.now(timezone.utc), access_level=access_levels,
                    state=DataFileState.GENERATED))

                name_to_remove = tm.name
                files_metadata = [file for file in files_metadata if file.get("name") != name_to_remove]

        return generated_files, files_metadata

    def __resubmit_handle_files(self, pid: str, str_updated_metadata_json: str, headers) -> int:
        logging.info(f'Ingesting files to {pid}')
        str_dv_file = self.__transform_metadata_to_dataverse_json(str_updated_metadata_json,
                                                                  app_settings.get("DV_FILES", "dataset-files.json"))
        dv_file_json = json.loads(str_dv_file)
        #Get the files metadata from the latest version:
        dv_latest_version = requests.get(f'{self.target.base_url}/api/datasets/:persistentId/versions/:latest?persistentId={pid}',
                                     headers=headers)
        #TODO: Check status code
        dv_latest_version_json = dv_latest_version.json()
        files_in_dv_latest_version_json = dv_latest_version_json["data"]["files"]
        logging.info(f'Found {len(files_in_dv_latest_version_json)} files in Remote Dataverse Target.')

        non_generated_file_in_dv_target = self.__resubmit_collect_non_generated_files_in_dv_latest_version_json(dv_file_json, files_in_dv_latest_version_json,
                                                                                                                headers, pid, str_dv_file)

        self.__resubmit_handle_non_generated_files_on_dv_target(dv_file_json, headers, non_generated_file_in_dv_target,
                                                                pid, str_dv_file)

        self.__resubmit_ingest_new_files(dv_file_json, headers, pid, str_dv_file)

    def __resubmit_handle_non_generated_files_on_dv_target(self, dv_file_json, headers, non_generated_file_in_dv_target,
                                                           pid, str_dv_file):
        for file in non_generated_file_in_dv_target:
            jsonData = json.loads(str_dv_file).get(file["dataFile"]["filename"])
            file_id = file["dataFile"]["id"]
            file_rec = self.db_manager.find_file_by_name(self.dataset_id, file["dataFile"]["filename"])

            if not file_rec:
                logging.info(f'File {file["dataFile"]["filename"]} deleted in the database. Deleting in Dataverse.')
                delete_response = requests.delete(f'{self.target.base_url}/api/files/{file_id}', headers=headers)
                logging.debug(f"Delete response: {delete_response.status_code} - {delete_response.text}")
            else:
                dv_file_json[file["dataFile"]["filename"]]["processed"] = True
                if file_rec.checksum != file["dataFile"]["checksum"]["value"]:
                    logging.info(f'File {file["dataFile"]["filename"]} updated in the database. Re-ingesting.')
                    self.replace_file_dv_target(file_id, file_rec, headers, pid, jsonData)
                else:
                    logging.debug(f'The checksum is the same. So, no need to re-ingest the file {file_rec.name}')
                    logging.debug(f'Updating metadata for {file_rec.name}')
                    data = {"jsonData": json.dumps(jsonData)}
                    response_update_file = requests.post(f"{self.target.base_url}/api/files/{file_id}/metadata",
                                                         files=data, headers=headers)
                    if response_update_file.status_code != status.HTTP_200_OK:
                        logging.error(
                            f'Failed to update metadata for {file_rec.name}. Response: {response_update_file.reason}')
                        raise ValueError(response_update_file.json())
                    logging.info(f'Metadata updated for {file_rec.name}.')

                logging.info(f'Handling embargo for {file_rec.name}')
                embargo_remote = file.get("dataFile", {}).get("embargo", {}).get('dateAvailable')
                embargo_form = jsonData.get("embargo")
                if embargo_form != embargo_remote:
                    logging.info(f'The embargo date is different. embargo_form: {embargo_form}, embargo_remote: {embargo_remote}')
                    if embargo_form=='':
                        if embargo_remote:
                            logging.info(f'Embargo removed for {file_rec.name}.')
                            self.__remove_dv_file_embargo(headers, pid, file)
                    else:
                        logging.debug(f'Embargo updated for {file_rec.name}.')
                        self.__modify_file_embargo(headers, jsonData, pid, file)
                else:
                    logging.info(f'The embargo date is the same. So, no need to update the file {file_rec.name}')


    def __resubmit_collect_non_generated_files_in_dv_latest_version_json(self, dv_file_json, files_in_dv_latest_version_json, headers
                                                                         , pid, str_dv_file):
        # Re-ingest generated files
        non_generated_file_in_dv_target = []
        for file_in_dv_latest_version_json in files_in_dv_latest_version_json:
            jsonData = json.loads(str_dv_file).get(file_in_dv_latest_version_json["dataFile"]["filename"])
            if "__generated__files" in file_in_dv_latest_version_json.get("categories", []):
                dv_file_json[file_in_dv_latest_version_json["dataFile"]["filename"]]["processed"] = True
                file_id = file_in_dv_latest_version_json["dataFile"]["id"]
                file_rec = self.db_manager.find_file_by_name(self.dataset_id,
                                                             file_in_dv_latest_version_json["dataFile"]["filename"])
                if file_rec.checksum != file_in_dv_latest_version_json["dataFile"]["checksum"]["value"]:
                    self.replace_file_dv_target(file_id, file_rec, headers, pid, jsonData)
                else:
                    logging.info(f"The checksum is the same. So, no need to re-ingest the file {file_rec.name}")
                    self.db_manager.restore_data_file(self.dataset_id, file_in_dv_latest_version_json["dataFile"]["filename"])
                    self.db_manager.delete_pending_data_file(self.dataset_id, file_in_dv_latest_version_json["dataFile"]["filename"],
                                                             file_in_dv_latest_version_json["dataFile"]["checksum"]["value"])
            else:
                non_generated_file_in_dv_target.append(file_in_dv_latest_version_json)
        return non_generated_file_in_dv_target

    def __resubmit_ingest_new_files(self, dv_file_json, headers, pid, str_dv_file):
        # now ingest new files
        for file_element in dv_file_json:
            already_processed = dv_file_json[file_element].get("processed", False)
            if not already_processed:
                file_rec = self.db_manager.find_file_by_name(self.dataset_id, file_element)
                if file_rec:
                    logging.info(f'File {file_element} is not ingested. So ingest it')
                    jsonData = json.loads(str_dv_file).get(file_element)
                    data = {"jsonData": json.dumps(jsonData)}
                    url_base = f"{self.target.base_url}/api/datasets/:persistentId/add?persistentId={pid}"
                    if file_rec.mime_type == "application/zip":
                        self.__handle_zip_file(file_rec, time.perf_counter())
                    start_timer = time.perf_counter()
                    file_rec.ingest_status = IngestFileStatus.IN_PROGRESS
                    file_rec.ingested_at = datetime.now(timezone.utc)
                    self.db_manager.update_file(file_rec)
                    with open(file_rec.path, 'rb') as f:
                        logging.debug(
                            f"file_rec.path: {file_rec.path}. file_rec.name: {file_rec.name}.headers: {headers}")
                        files = {'file': (file_rec.name, f)}
                        response_ingest_file = requests.post(url_base, files=files, data=data, headers=headers,
                                                             timeout=app_settings.get("DATAVERSE_RESPONSE_TIMEOUT",
                                                                                      360000))
                        if response_ingest_file.status_code != status.HTTP_200_OK:
                            logging.error(
                                f'File {file_rec.name} is FAIL ingested. Response: {response_ingest_file.json()}')
                            raise ValueError(response_ingest_file.json())

                        msg = f'File {file_rec.name} is successfully ingested to {pid}, small file - using python'
                        logging.info(msg)
                        file_rec.ingest_status = IngestFileStatus.SUCCESS
                        file_rec.ingest_status_message = msg
                        file_rec.ingest_duration = round(time.perf_counter() - start_timer, 2)
                        self.db_manager.update_file(file_rec)
                        logging.debug(
                            f'File {file_rec.name} is successfully ingested. Response: {response_ingest_file.json()}')
                        logging.info(
                            f'File {file_rec.name} is successfully ingested.')
                        if 'embargo' in jsonData:
                            self.__add_file_embargo(headers, jsonData, pid, response_ingest_file.json())
                    self.__delete_file(file_rec)

    def replace_file_dv_target(self, file_id, file_rec, headers, pid, jsonData):
        logging.debug(f'Replacing file in Dataverse, file_id: {file_id}, file_rec: {file_rec.path}')
        start_timer = time.perf_counter()
        file_rec.ingest_status = IngestFileStatus.IN_PROGRESS
        file_rec.ingested_at = datetime.now(timezone.utc)
        self.db_manager.update_file(file_rec)
        jsonData["forceReplace"] = True
        url_base = f"{self.target.base_url}/api/files/{file_id}/replace"
        timeout_seconds = app_settings.get("DATAVERSE_RESPONSE_TIMEOUT", 360000)

        logging.info(f'Start ingesting file {file_rec.name}. Size: {file_rec.size}. Ingest to {url_base}')
        with open(file_rec.path, 'rb') as f:
            response = requests.post(
                url_base,
                files={'file': (file_rec.name, f)},
                data={"jsonData": json.dumps(jsonData)},
                headers=headers,
                timeout=timeout_seconds
            )

        if response.status_code != status.HTTP_200_OK:
            logging.error(f'File {file_rec.name} FAILED to REPLACE. Response: {response.json()}')
            raise ValueError(response.json())
        msg = f'File {file_rec.name} is successfully ingested to {pid}, small file - using python'
        logging.info(msg)
        file_rec.ingest_status = IngestFileStatus.SUCCESS
        file_rec.ingest_status_message = msg
        file_rec.ingest_duration = round(time.perf_counter() - start_timer, 2)
        self.db_manager.update_file(file_rec)
        self.__delete_file(file_rec)

    def __delete_file(self, file_rec):
        logging.debug(f'Deleting file: {file_rec.path}')

        if file_rec.state == DataFileState.GENERATED:
            os.remove(file_rec.path)
            logging.debug(f'Deleted GENERATED file: {file_rec.path}')
        else:
            tus_file_path = delete_symlink_and_target(file_rec.path)
            logging.debug(f'Actual file path: {tus_file_path}')
            if tus_file_path:
                self.__remove_tus_lock_file(file_rec.dataset_id, tus_file_path)

    def __remove_tus_lock_file(self, dataset_id, tus_file_path):
        logging.debug(f'Removing file {tus_file_path}')
        tus_file_lock = tus_file_path.replace(f'-{dataset_id}.{self.app_name}', '.lock')
        logging.debug(f'Lock file: {tus_file_lock}')
        if os.path.exists(tus_file_lock):
            os.remove(tus_file_lock)
            logging.debug(f'Deleted lock file: {tus_file_lock}')
        else:
            logging.warning(f'Lock file not found: {tus_file_lock}')

    def __ingest_files(self, pid: str, str_updated_metadata_json: str, headers) -> int:
        logging.info(f'Ingesting files to {pid}')
        str_dv_file = self.__transform_metadata_to_dataverse_json(str_updated_metadata_json, app_settings.get("DV_FILES", "dataset-files.json"))

        for file_rec in self.db_manager.find_non_registered_files(dataset_id=self.dataset_id):
            logging.info(f'Ingesting file {file_rec.name}. Size: {file_rec.size} Path: {file_rec.path}')
            jsonData = json.loads(str_dv_file).get(file_rec.name)
            if not jsonData:
                logging.warning(f"File {file_rec.name} not found in transformed metadata. Skipping...")
                continue

            if file_rec.state == DataFileState.GENERATED:
                jsonData["description"] += f" (Generated by ACP v{self.dataset_rec.acp_version})"

            start_timer = time.perf_counter()
            data = {"jsonData": json.dumps(jsonData)}
            if file_rec.mime_type == "application/zip":
                self.__handle_zip_file(file_rec, start_timer)

            url_base = f"{self.target.base_url}/api/datasets/:persistentId/add?persistentId={pid}"
            logging.info(f'Start ingesting file {file_rec.name}. Size: {file_rec.size}. Ingest to {url_base}')
            file_rec.ingest_status = IngestFileStatus.IN_PROGRESS
            file_rec.ingested_at = datetime.now(timezone.utc)
            self.db_manager.update_file(file_rec)
            if file_rec.size < app_settings.get("MAX_INGEST_SIZE_USING_PYTHON", 100000000):
                logging.info(f'Ingest SMALL FILE using python: {file_rec.name}')
                with open(file_rec.path, 'rb') as f:
                    response = requests.post(
                        url_base,
                        files={'file': (file_rec.name, f)},
                        data=data,
                        headers=headers,
                        timeout=app_settings.get("DATAVERSE_RESPONSE_TIMEOUT", 360000)
                    )

                if response.status_code != status.HTTP_200_OK:
                    logging.error(f'File {file_rec.name} failed to ingest. Response: {response.json()}')
                    file_rec.ingest_status = IngestFileStatus.FAILED
                    file_rec.ingest_status_message = f'File {file_rec.name} - Ingest status code: {response.status_code} - {response.json()}'
                    raise ValueError(response.json())

                response_data = response.json()
                msg = f'File {file_rec.name} is successfully ingested, small file - using python'
                logging.info(msg)
            else:
                logging.info(f'Ingest LARGE FILE using script: {file_rec.name}')
                jsonData_str = json.dumps(jsonData)
                try:
                    output = f'{app_settings.DATA_TMP_BASE_DIR}/{self.app_name}/{self.dataset_id}/{str(uuid.uuid4().int)}.txt'
                    logging.info(f'Output: {output}')
                    result = subprocess.run(
                        [app_settings.SHELL_SCRIPT_PATH, file_rec.path, url_base, jsonData_str, self.target.password, output],
                        check=True, text=True, capture_output=True
                    )
                    logging.info(f'File {file_rec.name} is successfully ingested')
                    response_data = json.loads(result.stdout)
                except subprocess.CalledProcessError as e:
                    logging.error(f'File {file_rec.name} is FAIL ingested. Response: {e.stderr}')
                    raise ValueError(str(e.stderr))
                except Exception as e:
                    logging.error(f'File {file_rec.name} is FAIL ingested. Response: {e}')
                    raise ValueError(str(e))
                msg = f'File {file_rec.name} is successfully ingested, large file - using script'
            file_rec.ingest_status = IngestFileStatus.SUCCESS
            file_rec.ingest_status_message = msg
            file_rec.ingest_duration = round(time.perf_counter() - start_timer, 2)
            self.db_manager.update_file(file_rec)
            logging.info(f'Finish ingesting file {file_rec.name} to {pid} in {round(time.perf_counter() - start_timer, 2)} seconds.')
            self.__delete_file(file_rec)
            if 'embargo' in jsonData:
                self.__add_file_embargo(headers, jsonData, pid, response_data)

    def __handle_zip_file(self, file, start):
        logging.debug(f'Handling zip file: {file.path}')
        tus_real_file_path = os.readlink(file.path)
        zip_file_name = os.path.join(os.path.dirname(tus_real_file_path), file.name)

        # Replace symlink with the actual file
        os.remove(file.path)
        os.rename(tus_real_file_path, zip_file_name)

        # Zip the file and clean up
        logging.info(f'Starting to zip file: {zip_file_name}')
        zip_a_zipfile_with_progress(zip_file_name, file.path)
        os.remove(zip_file_name)

        logging.info(f'Finished zipping file {file.name} in {round(time.perf_counter() - start, 2)} seconds')
        self.__remove_tus_lock_file(file.dataset_id, tus_real_file_path)

    def __add_file_embargo(self, headers, jsonData, pid, response_data):
        file_id = response_data['data']['files'][0]['dataFile']['id']
        logging.debug(f'add file embargo: {file_id}')
        json_data = {'dateAvailable': jsonData['embargo'], 'reason': '', 'fileIds': [file_id]}
        response = requests.post(
            f"{self.target.base_url}/api/datasets/:persistentId/files/actions/:set-embargo?persistentId={pid}",
            headers=headers,
            json=json_data
        )
        if response.status_code != status.HTTP_200_OK:
            raise ValueError(response.text)

    def __modify_file_embargo(self, headers, jsonData, pid, response_data):
        file_id = response_data['dataFile']['id']
        logging.debug(f'Modify file embargo: {file_id}')
        json_data = {'dateAvailable': jsonData['embargo'], 'reason': '', 'fileIds': [file_id]}
        response = requests.post(
            f"{self.target.base_url}/api/datasets/:persistentId/files/actions/:set-embargo?persistentId={pid}",
            headers=headers,
            json=json_data
        )
        if response.status_code != status.HTTP_200_OK:
            raise ValueError(response.text)

    def __publish_dataset(self, pid, headers) -> int:
        logging.debug(f'Publishing dataset {pid}')
        return requests.post(
            f"{self.target.base_url}/api/datasets/:persistentId/actions/:publish?persistentId={pid}&type=major",
            headers=headers,
        ).status_code

    def __remove_dv_file_embargo(self, headers, pid, file):
        file_id = file['dataFile']['id']
        logging.debug(f'Removing embargo for file: {file_id}')

        if file.get("dataFile", {}).get("embargo", {}).get('dateAvailable'):
            data = {"fileIds": [file_id]}
            response = requests.post(
                f"{self.target.base_url}/api/datasets/:persistentId/files/actions/:unset-embargo?persistentId={pid}",
                headers=headers,
                data=json.dumps(data),
            )
            if response.status_code == status.HTTP_200_OK:
                logging.info(f'Embargo successfully removed for file {file_id}')
                logging.debug(f'Embargo successfully removed for file {file_id}. Response: {response.text}')
            else:
                logging.error(f'Failed to remove embargo for file {file_id}. Response: {response.text}')
                raise ValueError(response.text)
        else:
            logging.info(f'No embargo found for file {file_id}. No action taken.')
