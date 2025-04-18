# Import necessary plugins and packages
# Import necessary libraries and plugins
import hashlib
import json
import logging
import mimetypes
import os
import shutil
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable, Awaitable, Optional

import requests
import jmespath
from fastapi import APIRouter, Request, HTTPException

from src.acp.commons import (app_settings, data,
                             get_class, handle_ps_exceptions, \
                             send_mail, delete_symlink_and_target, retrieve_targets_configuration, get_repo_assistant,
                             create_asset, compare_dv_json, dmz_dataverse_headers, base_dir)
from src.acp.db.dbz import TargetRepo, DataFile, Dataset, StateVersion, DepositStatus, \
    MetadataType, AccessLevel, DataFileState
from src.acp.models.app_model import ResponseDataModel, InboxDatasetDataModel
# Import custom plugins and classes
from src.acp.models.assistant_datamodel import RepoAssistantDataModel, Target
from src.acp.models.bridge_output_model import TargetsCredentialsModel

# Create an API router instance
router = APIRouter()


# Helper function to process inbox dataset metadata
@handle_ps_exceptions
async def get_inbox_dataset_dc(request: Request, status: StateVersion) -> (
        Callable)[[Request, StateVersion], Awaitable[InboxDatasetDataModel]]:
    """
    Process inbox dataset metadata.

    This function processes the metadata of an inbox dataset for the given release version.
    It extracts necessary information from the request headers and body to create an
    InboxDatasetDataModel instance.

    Args:
        request (Request): The request object containing the dataset metadata.
        status (StateVersion): The release version of the dataset.

    Returns:
        Callable[[Request, ReleaseVersion], Awaitable[InboxDatasetDataModel]]:
        An awaitable function that returns an InboxDatasetDataModel instance.
    """
    ct = request.headers.get('content-type', MetadataType.JSON)
    title = request.headers.get('title', 'no-title') if ct == MetadataType.XML else jmespath.search('title', await request.json())
    req_body = (await request.body()).decode('utf-8') if ct == MetadataType.XML else await request.json()

    return InboxDatasetDataModel(
        assistant_name=request.headers.get('assistant-config-name'),
        status=status,
        owner_id=request.headers.get('user-id'),
        metadata_type=MetadataType(ct),
        title=title,
        target_creds=request.headers.get('targets-credentials'),
        metadata_content=req_body
    )

@router.post("/inbox/dataset/{status}")
async def process_inbox_dataset_metadata(request: Request, status: Optional[StateVersion] = None) -> {}:
    """
    Endpoint to process inbox dataset metadata for a specific release version.

    This endpoint processes the metadata of an inbox dataset for the given release version.

    Args:
        request (Request): The request object containing the dataset metadata.
        status (Optional[StateVersion]): The release version of the dataset. Defaults to None.

    Returns:
        dict: A dictionary representation of the processed dataset metadata.

    Raises:
        HTTPException: If there is an error during the processing of the dataset metadata.
    """
    logging.info(f'Process inbox dataset metadata for release version: {status}')
    rdm = await process_inbox(status, request)
    return rdm.model_dump(by_alias=True)


async def process_inbox(status, request):
    """
    Process the inbox dataset metadata.

    This function processes the metadata of an inbox dataset for the given release version.
    It validates the dataset, retrieves the repository configuration, processes target repositories,
    metadata records, and database records. It also checks if the dataset is ready for submission.

    Args:
        status (StateVersion): The release version of the dataset.
        request (Request): The request object containing the dataset metadata.

    Returns:
        ResponseDataModel: A data model containing the status and dataset ID.

    Raises:
        HTTPException: If the dataset is already submitted.
    """

    idh = await get_inbox_dataset_dc(request, status)
    repo_assistant = await get_repo_assistant(request)
    db_manager = data[repo_assistant.app_name]

    dataset_id = (
        request.headers.get('dataset_id') or
        (jmespath.search("id", idh.metadata_content) if idh.metadata_type == MetadataType.JSON else uuid.uuid4().hex)
    )

    logging.info(f'Start inbox for metadata id: {dataset_id} - release version: {dataset_id} - assistant name: '
           f'{idh.assistant_name}')

    if not db_manager.find_dataset_only_by_id(dataset_id):
        logging.info(f'Dataset does not exist: {dataset_id}')
        dataset = db_manager.create_initial_dataset_record(dataset_id, idh.owner_id, idh.title)
        logging.info(f'Dataset ID: {dataset.id}')

    else:
        logging.info(f'Dataset already exist: {dataset_id}')
        dataset = db_manager.find_dataset_only_by_id(dataset_id)#TODO: Check if resubmit and the dataset is not found: error!

    dataset_submission_ready = status in [StateVersion.SUBMIT, StateVersion.RESUBMIT]
    dataset_status = status if status in [StateVersion.DRAFT_RESUBMIT, StateVersion.SUBMIT, StateVersion.RESUBMIT] else dataset.status

    if idh.metadata_type == MetadataType.JSON:
        logging.info('Processing json metadata')
        metadata = json.dumps(idh.metadata_content)
    else:
        logging.info('Processing xml metadata')
        metadata = idh.metadata_content

    if status in [StateVersion.DRAFT_RESUBMIT, StateVersion.RESUBMIT]:
        # Backup dataset and check for changes on the server
        db_manager.backup_dataset_by_id(dataset_id)
        target_repo_recs = db_manager.find_target_repos_by_dataset_id(
            dataset_id=dataset.id, status_not_in=[StateVersion.DRAFT]
        )
        for repo_rec in target_repo_recs:
            deposited_metadata = json.loads(repo_rec.target_service_response or "{}").get('deposited_metadata')
            if deposited_metadata:
                api_url = json.loads(repo_rec.deposited_identifiers)[0]['api-url']
                diff = await compare_dv_json(deposited_metadata, repo_rec.name, json.loads(idh.target_creds), api_url)
                if diff:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Dataset {dataset_id} has changed on the server. Please check the diff: {diff}"
                    )

    db_record_metadata = Dataset(
        id=dataset_id,
        title=idh.title,
        owner_id=idh.owner_id,
        status=dataset_status,
        metadata_content=metadata,
        submission_ready=dataset_submission_ready,
        metadata_type=MetadataType(idh.metadata_type)
    )

    dataset = db_manager.update_dataset(db_record_metadata)

    if status in {StateVersion.DRAFT, StateVersion.SUBMIT}:
        db_manager.replace_targets_record(
            dataset.id, process_target_repos(repo_assistant, idh.target_creds)
        )

    dataset_dir = os.path.join(app_settings.DATA_TMP_BASE_DIR, repo_assistant.app_name, str(dataset.id))
    os.makedirs(dataset_dir, exist_ok=True)

    registered_files, file_submission_ready = process_registered_files(db_manager, dataset.id, idh, dataset_dir)
    if dataset_submission_ready and file_submission_ready:
        db_manager.set_dataset_ready_for_ingest(dataset.id, True)

    logging.info(f"Registered files: {registered_files}")
    logging.info(f"Dataset ready: {dataset_submission_ready}, File state: {file_submission_ready}")
    process_db_records_registered_files(db_manager, dataset.id, registered_files)

    if status != StateVersion.DRAFT and db_manager.is_dataset_ready(dataset.id) and db_manager.are_files_uploaded(dataset.id):
        logging.info(f'SUBMIT DATASET with version {status.name} is_dataset_ready {dataset.id}')
        bridge_job(db_manager, repo_assistant.app_name, dataset.id, f"/inbox/dataset/{idh.status}")
    else:
        logging.info(f'NOT READY to submit dataset with version {status.name} dataset_id: {dataset.id} '
               f'\nNumber still registered: {len(db_manager.find_registered_files(dataset.id))}')
        # Create the ResponseDataModel with the required dataset_id
    rdm = ResponseDataModel(status="OK")
    rdm.dataset_id = str(dataset.id)
    rdm.start_process = db_manager.is_dataset_ready(dataset.id)
    return rdm


@router.delete("/inbox/dataset/{dataset_id:path}")#TODO: Ask Daan to implement this
async def delete_dataset_metadata(request: Request, dataset_id: str, status: Optional[StateVersion] = StateVersion.DRAFT) -> {}:
    """
    Endpoint to delete dataset metadata.

    This endpoint deletes the metadata of a dataset identified by the given metadata ID.
    It checks if the user is authorized and if the dataset can be deleted based on its deposit status.

    Args:
        request (Request): The request object containing headers with user information.
        dataset_id (str): The ID of the dataset metadata to be deleted.

    Returns:
        dict: A dictionary containing the status of the deletion.

    Raises:
        HTTPException: If the user ID is not provided in the request headers.
        HTTPException: If the dataset is not found for the given user ID.
        HTTPException: If the dataset cannot be deleted based on its deposit status.
    """
    logging.info(f'Delete dataset: {dataset_id}')

    user_id = request.headers.get('user-id')
    if not user_id:
        raise HTTPException(status_code=401, detail='No user id provided')

    repo_assistant = await get_repo_assistant(request)
    app_name = repo_assistant.app_name
    db_manager = data[app_name]
    dataset = db_manager.find_dataset_by_id(dataset_id)

    if dataset.id not in db_manager.find_dataset_ids_by_owner(user_id):
        raise HTTPException(status_code=404, detail='No Dataset found')

    if dataset.status == StateVersion.DRAFT_RESUBMIT:

       db_manager.restore_from_backup(dataset.id)

       return delete_dataset_and_its_folder(db_manager, dataset.id, app_name, False)

    target_repos = db_manager.find_target_repos_by_dataset_id(
        dataset_id=dataset.id, status_not_in=[StateVersion.SUBMITTED, StateVersion.RESUBMIT, StateVersion.DRAFT_RESUBMIT])
    if not target_repos:
        logging.info(f'Delete dataset: {dataset.id}, NOT target_repos')
        return delete_dataset_and_its_folder(db_manager, dataset.id, app_name)

    if target_repos:
        can_be_deleted = False
        for target_repo in target_repos:
            if target_repo.deposit_status not in (DepositStatus.ACCEPTED, DepositStatus.DEPOSITED, DepositStatus.FINISH):
                can_be_deleted = True
                logging.info(f'Delete of {dataset.id} is allowed. Deposit status: {target_repo.deposit_status}')
                break

        if can_be_deleted:
            return delete_dataset_and_its_folder(db_manager, dataset.id, app_name)

    raise HTTPException(status_code=404, detail=f'Delete of {dataset.id} is not allowed.')


def delete_dataset_and_its_folder(db_manager, dataset_id: str, app_name: str, delete_dataset: bool = True) -> {}:
    """
    Delete a dataset and its associated folder.

    This function deletes the dataset identified by the given metadata ID and its associated folder.
    It first checks if the dataset folder exists and deletes it if found. It then deletes the dataset
    record from the database and checks again if the folder exists to ensure it is removed.

    Args:
        dataset_id (str): The ID of the dataset metadata to be deleted.

    Returns:
        dict: A dictionary containing the status of the deletion and the metadata ID.
    """
    dataset_folder = os.path.join(app_settings.DATA_TMP_BASE_DIR, app_name, str(dataset_id))
    logging.info(f'Delete dataset folder: {dataset_folder}')
    if os.path.exists(dataset_folder):
        delete_symlink_and_target(dataset_folder)
    else:
        logging.info(f'Dataset folder: {dataset_folder} not found')
    if delete_dataset:
        logging.info(f'Delete dataset: {dataset_id}')
        db_manager.delete_by_dataset_id(dataset_id)

    if os.path.exists(dataset_folder):
        logging.info(f'Delete dataset folder: {dataset_folder}')
        shutil.rmtree(dataset_folder)
    else:
        logging.info(f'Dataset folder: {dataset_folder} not found')
    return {"status": "ok", "dataset-id": dataset_id}


@handle_ps_exceptions
def process_db_records_registered_files(db_manager, dataset_id, registered_files):

    if registered_files:
        logging.info(f'Insert datafiles records for {dataset_id}')
        logging.info(f'Number registered_files: {len(registered_files)}')
        try:
            db_manager.insert_datafiles(dataset_id, registered_files)
            logging.info(f'SUCCESSFUL  INSERT datafiles records for {dataset_id}, number of files: {len(registered_files)}')
        except ValueError as e:
            logging.error(f'Error inserting datafiles: {e}')


@handle_ps_exceptions
def process_registered_files(db_manager, dataset_id, idh, tmp_dir):
    file_submission_ready = True
    registered_files = []
    if idh.metadata_type == MetadataType.JSON:
        logging.info('Processing json metadata')
        file_names = []
        file_names_from_input = jmespath.search('"file-metadata"[*].name', idh.metadata_content)
        if file_names_from_input:
            for file_name in file_names_from_input:
                data_file = db_manager.find_file_by_name(dataset_id, file_name)
                if data_file:
                    logging.info(f'File {file_name} already exist')
                    escaped_file_name = file_name.replace('"', '\\"')
                    f_permission = jmespath.search(f'"file-metadata"[?name == `{escaped_file_name}`].private', idh.metadata_content)
                    permission = AccessLevel.PRIVATE if f_permission[0] else AccessLevel.PUBLIC
                    db_manager.update_file_access_level(dataset_id, file_name, permission)
                    continue
                else:
                    file_names.append(file_name)
        else:
            file_names_from_input = []

        logging.info(f'Number of file_names: {len(file_names)}')
        already_uploaded_files_name = db_manager.execute_l(dataset_id)
        logging.info(f'Number of already_uploaded_files: {len(already_uploaded_files_name)}')

        files_name_to_be_deleted = set(already_uploaded_files_name) - set(file_names_from_input)
        logging.info(
            f'Number of files_name_to_be_deleted: {len(files_name_to_be_deleted)} --LIST:  {files_name_to_be_deleted}')
        files_name_to_be_added = set(file_names) - set(already_uploaded_files_name)
        logging.info(f'Number of files_name_to_be_added: {len(files_name_to_be_added)}')

        for f_name in files_name_to_be_deleted:
            file_path = os.path.join(tmp_dir, f_name)
            if os.path.exists(file_path):
                delete_symlink_and_target(file_path)
                logging.info(f'{file_path} is deleted')
            else:
                logging.info(f'{file_path} not found')
            db_manager.delete_datafile(dataset_id, f_name)

        for f_name in files_name_to_be_added:
            file_path = os.path.join(tmp_dir, f_name)
            # Escape special characters in the filename
            escaped_filename = f_name.replace('"', '\\"')

            f_permission = jmespath.search(f'"file-metadata"[?name == `{escaped_filename}`].private', idh.metadata_content)
            permission = AccessLevel.PRIVATE if f_permission[0] else AccessLevel.PUBLIC
            registered_files.append(DataFile(name=f_name, path=file_path, permissions=permission))

        logging.info(f'registered_files: {registered_files}')

        # Update file permission
        already_uploaded_files = db_manager.find_uploaded_files(dataset_id)
        logging.info(f'Number of already_uploaded_files: {len(already_uploaded_files)}')
        file_submission_ready = True if not files_name_to_be_added else False

    return registered_files, file_submission_ready

@handle_ps_exceptions
def process_target_repos(repo_assistant, target_creds) -> [TargetRepo]:
    """
    Process target repositories for a given assistant.

    This function processes the target repositories for the given assistant by validating
    the target credentials and updating the repository configuration.

    Args:
        repo_assistant (RepoAssistantDataModel): The assistant data model containing target repository information.
        target_creds (str): A JSON string containing the target credentials.

    Returns:
        list[TargetRepo]: A list of TargetRepo objects representing the processed target repositories.

    Raises:
        HTTPException: If a specified bridge plugin class is not found in the data keys.
    """
    db_recs_target_repo = []
    tgc = {"targets-credentials": json.loads(target_creds)}
    input_target_cred_model = TargetsCredentialsModel.model_validate(tgc)
    for repo_target in repo_assistant.targets:
        if repo_target.bridge_plugin_name not in data.keys():
            raise HTTPException(status_code=404, detail=f'Module "{repo_target.bridge_plugin_name}" not found.',
                                headers={})
        target_repo_name = repo_target.repo_name
        logging.info(f'target_repo_name: {target_repo_name}')
        for depositor_cred in input_target_cred_model.targets_credentials:
            if (depositor_cred.target_repo_name == repo_target.repo_name and depositor_cred.credentials and
                    depositor_cred.credentials.username):
                repo_target.username = depositor_cred.credentials.username
            if (depositor_cred.target_repo_name == repo_target.repo_name and depositor_cred.credentials and
                    depositor_cred.credentials.password):
                repo_target.password = depositor_cred.credentials.password

        db_recs_target_repo.append(TargetRepo(name=repo_target.repo_name, url=repo_target.target_url,
                                              display_name=repo_target.repo_display_name,
                                              configuration=repo_target.model_dump_json(by_alias=True, exclude_none=True)))
    return db_recs_target_repo


async def delete_file(file_id: str):
    logging.info(f"Deleting file {file_id}")
    url = f'{app_settings.TUS_BASE_URL}/files/{file_id}'
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {app_settings.ACP_SERVICE_API_KEY}"
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.delete(url, headers=headers, timeout=10)
            if response.status_code == 204:
                logging.info(f"TUS File  successfully deleted. {file_id}")
            else:
                logging.info(f"File id: {file_id} Failed to delete file. Status code: {response.status_code}")

            return response.status_code
        except Exception as e:
            logging.error(f"File id: {file_id} Error deleting file: {e}")

        return 500


@router.patch("/inbox/files/{dataset_id}/{file_uuid}")
async def upload_file(dataset_id: str, file_uuid: str, req: Request) -> {}:
    """
    Endpoint to update file metadata.

    This endpoint updates the metadata of a file identified by the given metadata ID and file UUID.
    It processes the file, creates a symlink, and updates the database with the new file information.

    Args:
        dataset_id (str): The ID of the dataset metadata.
        file_uuid (str): The UUID of the file.

    Returns:
        dict: A dictionary containing the status and the dataset ID.

    Raises:
        HTTPException: If the file info or file is not found.
        HTTPException: If the file size is 0.
        HTTPException: If there is a file size mismatch.
    """
    logging.info(f'find_metadata_by_metadata_id - dataset_id: {dataset_id}')
    repo_assistant= await get_repo_assistant(req)
    app_name = repo_assistant.app_name
    db_manager = data[app_name]
    dataset = db_manager.find_dataset_by_id(dataset_id)
    logging.info(f'PATCH file metadata for metadata_id: {dataset_id}, dataset_id: {dataset_id} and file_uuid: {file_uuid}' )
    tus_file = os.path.join(app_settings.DATA_TMP_BASE_TUS_FILES_DIR, file_uuid)
    file_info_path = f'{tus_file}.info'
    if not os.path.exists(file_info_path):
        logging.error(f'File info NOT FOUND for {file_uuid}: {file_info_path}')
        raise HTTPException(status_code=404, detail='File not found')
    if not os.path.exists(tus_file):
        logging.error(f'File NOT FOUND for {file_uuid}: {tus_file}')
        raise HTTPException(status_code=404, detail='File not found')
    if os.path.getsize(tus_file) == 0:
        logging.error(f'File SIZE IS 0 for {file_uuid}: {tus_file}')
        raise HTTPException(status_code=400, detail='File size is 0')

    with open(file_info_path, "r") as file:
        file_metadata = json.load(file)
    file_name = file_metadata['metadata']['fileName']
    if file_metadata.get('size', 0) == 0:
        logging.error(f'File SIZE IS 0 for {file_uuid}: {tus_file}')
        raise HTTPException(status_code=400, detail='File size is 0')

    logging.info(f'file_name: {file_name}')
    if os.path.getsize(tus_file) != file_metadata.get('size', 0):
        logging.error(f'FILE SIZE MISMATCH for {file_uuid}: {tus_file}')
        raise HTTPException(status_code=400, detail='File size mismatch')

    dataset_folder = os.path.join(app_settings.DATA_TMP_BASE_DIR, app_name, str(dataset.id))
    source_file_path = os.path.join(app_settings.DATA_TMP_BASE_TUS_FILES_DIR, file_uuid)
    dest_file_path = os.path.join(dataset_folder, file_name)
    # Process the files
    logging.info(f'Processing using symlink {source_file_path} to {dest_file_path}')
    target = source_file_path
    link_name = dest_file_path
    try:
        sha1_hash = calculate_sha1_checksum(source_file_path)
        # md5_hash = ""
        # if app_settings.get("use_md5_hash", True):
        #     with open(source_file_path, 'rb') as file:
        #         md5_hash = hashlib.md5(file.read()).hexdigest()
            # with open(source_file_path, "rb") as f:
            #     file_hash = hashlib.md5()
            #     while chunk := f.read(8192):
            #         file_hash.update(chunk)
            # md5_hash = file_hash.hexdigest()

        file_type = file_metadata['metadata'].get('filetype', mimetypes.guess_type(dest_file_path)[0])
        db_manager.update_file(DataFile(dataset_id=dataset.id, name=file_name, checksum=sha1_hash,
                                        size=os.path.getsize(source_file_path), mime_type=file_type,
                                        path=dest_file_path, state=DataFileState.UPLOADED))
        new_name = f'{target}-{dataset_id}.{app_name}'
        os.rename(target, new_name)
        os.symlink(new_name, link_name)
        logging.info(f'Symlink created: {link_name} -> {target}')
        logging.info(f'Deleting {source_file_path}.info')
        deleted_status = await delete_file(file_uuid)
        logging.info(f'Deleted status of file uuid: {file_uuid} is {deleted_status}')
    except FileExistsError:
        logging.error(f'The symlink {link_name} already exists.')
    except FileNotFoundError:
        logging.error(f'The target {target} does not exist.')
    except OSError as e:
        logging.error(f'Error creating symlink: {e}')
    all_files_uploaded = len(db_manager.find_registered_files(dataset.id)) == 0
    if all_files_uploaded:
        logging.info(f'All files are UPLOADED for {dataset.id}')
        db_manager.set_dataset_ready_for_ingest(dataset_id, True)
    else:
        db_manager.set_dataset_ready_for_ingest(dataset_id, False)
        logging.info(f'Not all files uploaded for metadata_id: {dataset_id} dataset_id: {dataset_id}')

    start_process = db_manager.is_dataset_ready(dataset_id)
    if start_process:
        logging.info(f'Start Bridge task for {dataset_id} from the PATCH file endpoint')
        bridge_job(db_manager, app_name, dataset.id, f'/inbox/files/{dataset.id}/{file_uuid}')
        logging.info(f'Bridge task for {dataset.id} started successfully')
    else:
        logging.info(f'Bridge task for {dataset.id} NOT started')

    registerd_files = db_manager.find_registered_files(dataset.id)
    logging.info(f'Number of registered files: {len(registerd_files)}')
    rdm = ResponseDataModel(status="OK")
    rdm.dataset_id = str(dataset.id)
    rdm.start_process = start_process
    return rdm.model_dump(by_alias=True)

def calculate_sha1_checksum(file_path: str) -> str:
    """
    Calculate the SHA-1 checksum of a file.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: The SHA-1 checksum as a hexadecimal string.
    """
    with open(file_path, 'rb') as f:
        sha1 = hashlib.sha1()
        for chunk in iter(lambda: f.read(4096), b''):
            sha1.update(chunk)
    return sha1.hexdigest()


def bridge_job(db_manager, app_name, dataset_id: str, msg: str) -> None:
    """
    Start a new thread to follow the bridge process for a dataset.

    This function starts a new thread to execute the `follow_bridge` function for the given dataset ID.
    It logs the start of the threading process and handles any exceptions that occur.

    Args:
        datasetId (str): The ID of the dataset to follow the bridge process for.
        msg (str): A message to log when starting the threading process.

    Returns:
        None
    """
    logging.info(f"Starting threading for {msg} with datasetId: {dataset_id}")
    try:
        threading.Thread(target=follow_bridge, args=(db_manager, app_name, dataset_id,)).start()
        logging.info(f"Threading for {dataset_id} started successfully.")
    except Exception as e:
        logging.error(f"Error starting thread for {dataset_id}: {e}")


def follow_bridge(db_manager, app_name, dataset_id: str) -> type(None):
    """
    Follow the bridge process for a dataset.

    This function logs the start time of the thread, marks the dataset as submitted,
    retrieves the target repositories associated with the dataset, and executes the bridge process.

    Args:
        dataset_id (str): The ID of the dataset to follow the bridge process for.

    Returns:
        None
    """
    # Log the start time of the thread
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Thread for datasetId: {dataset_id} started at {start_time}")

    logging.info("Follow bridge")
    logging.info(f">>> EXECUTE follow_bridge for datasetId: {dataset_id}")
    db_manager.submitted_now(dataset_id)
    # target_repo_recs = db_manager.find_target_repos_by_dataset_id(dataset_id)
    execute_bridges(db_manager, app_name, dataset_id)


def execute_bridges(db_manager, app_name, dataset_id:int) -> None:
    """
    Execute the bridge process for a dataset.

    This function iterates over the target repositories associated with the dataset,
    executes the bridge process for each target, and handles the results.

    Args:
        datasetId (str): The ID of the dataset to execute the bridge process for.
        targets (list): A list of target repositories to process.

    Returns:
        None
    """
    logging.info(f"execute_bridges for datasetId: {dataset_id}")
    results = []
    targets = db_manager.find_dataset_by_id(dataset_id).target_repos
    for target_repo_rec in targets:
        bridge_class = data[Target(**json.loads(target_repo_rec.configuration)).bridge_plugin_name]
        logging.info(f'EXECUTING {bridge_class} for target_repo_id: {target_repo_rec}')

        start = time.perf_counter()
        bridge_instance = get_class(bridge_class)(db_manager= db_manager, app_name= app_name, dataset_id=dataset_id,
                                                  target=Target(**json.loads(target_repo_rec.configuration)))
        deposit_result = bridge_instance.job()
        deposit_result.response.duration = round(time.perf_counter() - start, 2)

        logging.info(f'Result from Deposit: {deposit_result.model_dump_json()}')
        bridge_instance.save_state(deposit_result)

        if deposit_result.deposit_status in [DepositStatus.FINISH, DepositStatus.ACCEPTED, DepositStatus.SUCCESS, DepositStatus.DEPOSITED]:
            logging.info(f'Deposit status: {deposit_result.deposit_status} for {dataset_id}')
            results.append(deposit_result)
        else:
            send_mail(f'Executing {bridge_class} is FAILED.', f'Resp:\n {deposit_result}')
            break

    if len(results) == len(targets):
        logging.info(f'All targets are SUCCESSFULLY executed for datasetId: {dataset_id}, now trying to delete the dataset folder')
        dataset_folder = os.path.join(app_settings.DATA_TMP_BASE_DIR, app_name,
                                      str(dataset_id))
        logging.info(f'Ingest SUCCESSFULL, DELETED {dataset_folder}')

        for file in Path(dataset_folder).glob('*'):
            if file.is_file():
                delete_symlink_and_target(file)
        if os.path.exists(dataset_folder):
            shutil.rmtree(dataset_folder)
        logging.info(f'All related files are DELETED successfully: {dataset_folder}')
        db_manager.update_dataset_status(dataset_id, StateVersion.SUBMITTED)
    else:
        logging.info(f'Ingest FAILED for datasetId: {dataset_id}')
        db_manager.update_dataset_status(dataset_id, StateVersion.FAILED)


#
@router.delete("/inbox/{dataset_id}", include_in_schema=False)
def delete_inbox(dataset_id: str, req: Request):
    """
    Endpoint to delete an inbox dataset.

    This endpoint deletes the dataset identified by the given dataset ID from the database.

    Args:
        datasetId (str): The ID of the dataset to be deleted.

    Returns:
        dict: A dictionary containing the status of the deletion and the number of rows deleted.
    """
    assistant_name = req.headers.get('assistant-config-name')
    if assistant_name:
        raise HTTPException(status_code=400, detail="assistant-config-name is missing")

    repo_config = retrieve_targets_configuration(assistant_name)
    repo_assistant = RepoAssistantDataModel.model_validate_json(repo_config)
    app_name = repo_assistant.app_name
    db_manager = data[app_name]
    num_rows_deleted = db_manager.delete_by_dataset_id(dataset_id)
    return {"Deleted": "OK", "num-row-deleted": num_rows_deleted}


def remove_files_and_directories(dir_path):
    """
    Remove all files and directories within the specified directory.

    This function iterates through all items in the given directory path and removes them.
    It logs the removal of each file and directory.

    Args:
        dir_path (str): The path of the directory to be cleaned.
    """
    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        if os.path.isfile(item_path):
            os.remove(item_path)
            logging.info(f"File {item_path} has been removed")
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)
            logging.info(f"Directory {item_path} and all its contents have been removed")


@router.get("/dataset/{dataset_id}/metadata", include_in_schema=False)
def get_md(dataset_id: str, req: Request):
    """
    Endpoint to retrieve the metadata of a dataset.

    This endpoint retrieves the metadata of the dataset identified by the given dataset ID.

    Args:
        datasetId (str): The ID of the dataset to retrieve the metadata for.

    Returns:
        dict: A dictionary containing the metadata of the dataset.

    Raises:
        HTTPException: If the dataset is not found.
    """
    logging.info(f'find_metadata_by_metadata_id - metadata_id: {dataset_id}')
    assistant_name = req.headers.get('assistant-config-name')
    if assistant_name is None:
        raise HTTPException(status_code=400, detail="'assistant-config-name' is missing")

    # Attempt to parse the 'targets-credentials' header as JSON

    repo_config = retrieve_targets_configuration(assistant_name)
    repo_assistant = RepoAssistantDataModel.model_validate_json(repo_config)
    app_name = repo_assistant.app_name
    db_manager = data[app_name]
    dataset = db_manager.get_decrypted_md(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    return json.loads(dataset.metadata_content)


@router.get("/dataset/{dataset_id}/status")
async def is_modified(dataset_id: str, req: Request):
    """
    Endpoint to retrieve the difference between two dataset versions.

    This endpoint retrieves the difference between two versions of a dataset identified by the given dataset ID.

    Args:
        datasetId (str): The ID of the dataset to retrieve the difference for.

    Returns:
        dict: A dictionary containing the difference between the two versions of the dataset.

    Raises:
        HTTPException: If the dataset is not found.
    """
    logging.info(f'find_metadata_by_metadata_id - metadata_id: {dataset_id}')
    tc_header = req.headers.get('targets-credentials')
    assistant_name = req.headers.get('assistant-config-name')
    if not tc_header or not assistant_name:
        raise HTTPException(status_code=400, detail="Targets credentials are missing and/or assistant-config-name is missing")

    # Attempt to parse the 'targets-credentials' header as JSON

    try:
        target_creds = json.loads(tc_header)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid json format of targets-credentials")

    repo_assistant = await get_repo_assistant(req)
    db_manager = data[repo_assistant.app_name]
    dataset = db_manager.find_dataset_only_by_id(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")

    if dataset.status is not StateVersion.SUBMITTED:
        raise HTTPException(status_code=400, detail=f"Dataset {dataset_id} is not submitted")

    asset = await create_asset(dataset, db_manager, target_creds)
    return asset

@router.get("/dataset/{dataset_id}/diff")
async def dataset_diff(dataset_id: str, req: Request):
    """
    Endpoint to retrieve the difference between two dataset versions.

    This endpoint retrieves the difference between two versions of a dataset identified by the given dataset ID.

    Args:
        datasetId (str): The ID of the dataset to retrieve the difference for.

    Returns:
        dict: A dictionary containing the difference between the two versions of the dataset.

    Raises:
        HTTPException: If the dataset is not found.
    """
    logging.info(f'find_metadata_by_metadata_id - metadata_id: {dataset_id}')
    tc_header = req.headers.get('targets-credentials')
    assistant_name = req.headers.get('assistant-config-name')
    if not tc_header or not assistant_name:
        raise HTTPException(status_code=400, detail="Targets credentials are missing and/or assistant-config-name is missing")

    # Attempt to parse the 'targets-credentials' header as JSON

    try:
        target_creds = json.loads(tc_header)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid json format of targets-credentials")

    repo_assistant = await get_repo_assistant(req)
    db_manager = data[repo_assistant.app_name]
    dataset = db_manager.find_dataset_only_by_id(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")

    if dataset.status is not StateVersion.SUBMITTED:
        raise HTTPException(status_code=400, detail=f"Dataset {dataset_id} is not submitted")

    asset = await create_asset(dataset, db_manager, target_creds)
    return asset

import httpx
@router.post("/dataset/prefill/{url:path}")
async def create_dataset_form(url: str, req: Request):
    # The full_path will contain everything after /create-dataset-form/
    # including any slashes, but still not the query parameters
    tc_header = req.headers.get('targets-credentials')
    if not tc_header:
        raise HTTPException(status_code=400,
                            detail="Targets credentials are missing and/or assistant-config-name is missing")

    try:
        target_creds = json.loads(tc_header)
        if len(target_creds) != 1:
            raise HTTPException(status_code=501, detail="Only one target repo is supported")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid json format of targets-credentials")

    repo_assistant = await get_repo_assistant(req)
    db_manager = data[repo_assistant.app_name]

    persistent_id = req.query_params.get('persistentId')
    target_repo_rec =  db_manager.find_target_repo_by_indentifier(persistent_id)
    if target_repo_rec:
        target_repo_identifiers_json = json.loads(target_repo_rec.deposited_identifiers)
        target_service_response_json = json.loads(
            target_repo_rec.target_service_response) if target_repo_rec.target_service_response else {}
        target_service_response_deposited_metadata = target_service_response_json.get('deposited_metadata')
        if target_service_response_deposited_metadata:
            api_url = target_repo_identifiers_json[0]['api-url']
            diff = await compare_dv_json(target_service_response_deposited_metadata, target_repo_rec.name, target_creds,
                                         api_url)
            if diff:
                raise HTTPException(status_code=501,
                                    detail=f"Dataset {persistent_id} has changed on the server. Not implemented yet")
            return {"dataset-id": target_repo_rec.dataset_id}
        else:
            public_url = f"http://localhost:10124/dataset/{target_repo_rec.dataset_id}"


    else:

        if len(target_creds) != 1:
            raise HTTPException(status_code=501, detail="Only one target repo is supported")

        assistant_targets_repo = repo_assistant.targets

        if len(assistant_targets_repo) != 1 or assistant_targets_repo[0].bridge_plugin_name != 'DataverseIngester':
            raise HTTPException(status_code=501, detail="Only Dataverse is supported")

        if target_creds[0]["target-repo-name"] == assistant_targets_repo[0].repo_name:

            response = await fetch_dataverse(req, target_creds, url)

            if response.status_code == 200:

                return response.json()

            elif response.status_code == 404:

                with open(f'{base_dir}/resources/examples/minimal-form.json', 'r') as f:

                    json_data = json.load(f)

                    json_data["id"] = f'{repo_assistant.app_name}-{uuid.uuid4().hex}'

                return json_data

            raise HTTPException(status_code=400,
                                detail=f"Failed to fetch dataset {persistent_id} from Dataverse: {response.text}")

        raise HTTPException(status_code=501, detail=f"{assistant_targets_repo[0].repo_name} is not supported)")

    headers = {key: value for key, value in req.headers.items() if key.lower() != 'content-length'}

    async with httpx.AsyncClient() as client:

        response = await client.get(public_url, headers=headers)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        return response.json()


async def fetch_dataverse(req, target_creds, url):
    headers = dmz_dataverse_headers("API_KEY", target_creds[0]["credentials"]["password"])
    dataset_url = f'{url}?{req.url.query}'
    target_url = dataset_url.replace("dataset.xhtml", "api/datasets/:persistentId/")
    response = requests.get(target_url, headers=headers)
    return response
