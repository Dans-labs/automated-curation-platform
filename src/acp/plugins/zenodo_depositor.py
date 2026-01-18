from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict
from urllib.parse import urlparse

import requests
from pydantic import BaseModel, Field
from requests.exceptions import RequestException
from starlette import status

from src.acp.bridge import Bridge
from src.acp.commons import transform, handle_deposit_exceptions
from src.acp.db.dbz import DepositStatus
from src.acp.models.bridge_output_model import TargetDataModel, TargetResponse, ResponseContentType, IdentifierItem


class ZenodoApiDepositor(Bridge):
    """
    A class to handle the deposit of metadata to the Zenodo API.

    Inherits from:
        Bridge: The base class for all bridge implementations.
    """

    @property
    def headers(self) -> Dict[str, str]:
        """
        Return request headers computed from the instance's target.
        Safe if `self.target` or `self.target.password` is not yet set.
        Use `self.headers` throughout the class.
        """
        token = getattr(self.target, "password", None)
        hdr = {"Content-Type": "application/json"}
        if token:
            hdr["Authorization"] = f"Bearer {token}"
        return hdr



    @handle_deposit_exceptions
    def job(self) -> TargetDataModel:
        """
        Executes the deposit process to the Zenodo API.

        This method creates an initial dataset on Zenodo, transforms the metadata, and sends a PUT request to update the dataset.
        It then ingests files into the Zenodo bucket and updates the bridge output model accordingly.

        Returns:
        BridgeOutputDataModel: The output model containing the response from the Zenodo API and the status of the deposit.
        """
        zenodo_resp = self.__create_initial_dataset()
        tdm = TargetDataModel(response=zenodo_resp)
        if zenodo_resp is None:
            logging.error('Error occurs: status code: 500')
            tdm.deposit_status = DepositStatus.ERROR
            return tdm
        zenodo_id = zenodo_resp.get("id")
        str_zenodo_dataset_metadata: str = transform(self.target.metadata.transformed_metadata[0].transformer_url,
                                                self.dataset_rec.metadata_content)

        url = f'{self.target.target_url}/{zenodo_id}?{self.target.username}={self.target.password}'
        logging.info(f"Send to {url}")
        zen_resp = requests.put(url, data=str_zenodo_dataset_metadata, headers={"Content-Type": "application/json"})
        logging.info(f'Zenodo response status code: {zen_resp.status_code}. Zenodo response: {zen_resp.text}')
        if zen_resp.status_code != status.HTTP_200_OK:
            logging.error(f'Error occurs: status code: {zen_resp.status_code}')
            tdm.deposited_metadata = "Error occurs: status code: " + str(zen_resp.status_code)
            tdm.response = zen_resp.text
            tdm.deposit_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
            tdm.deposit_status = DepositStatus.ERROR
            return tdm
        zm = ZenodoModel(**zen_resp.json())
        self.__ingest_files(zm.links.bucket if zm.links else '')

        comm_identifiers = self.__get_community_identifiers(str_zenodo_dataset_metadata)
        if comm_identifiers and len(comm_identifiers) == 1:  # Support only one community for now
            community_id = self.__get_community_id(comm_identifiers[0])
        else:
            tdm.deposit_status = DepositStatus.ERROR
            tdm.deposit_status_message = "No or multiple community identifiers found in metadata."
            logging.error("Error occurs: No or multiple community identifiers found in metadata.")
            return tdm

        if community_id:
            added = self.__add_deposition_to_community(zenodo_id, community_id)
            if not added:
                logging.error(f"Error occurs: Could not add deposition {zenodo_id} to community {community_id}.")
                tdm.deposit_status = DepositStatus.ERROR
                tdm.deposit_status_message="Could not add deposition to community."
                return tdm

            review_submitted = self.__submit_review(zenodo_id)
            if not review_submitted:
                logging.error(f"Error occurs: Could not submit review for deposition {zenodo_id}.")
                tdm.deposit_status = DepositStatus.ERROR
                tdm.deposit_status_message="Could not submit review for deposition."
                return tdm

        tdm.deposit_status =  DepositStatus.FINISH
        tdm.deposited_metadata = "Successfully deposited to Zenodo."
        tdm.deposit_time = datetime.now(timezone.utc).isoformat()
        target_resp = TargetResponse()
        target_resp.url = f'{self.target.target_url}/{zenodo_id}'
        target_resp.content = json.dumps(zen_resp.json())
        target_resp.status_code = zen_resp.status_code
        target_resp.identifiers = [IdentifierItem(value=zm.metadata.prereserve_doi.doi if zm.metadata and zm.metadata.prereserve_doi else None, url=zm.links.html if zm.links else None)]
        target_resp.content_type = ResponseContentType.JSON
        tdm.response = target_resp
        logging.info(f"Successfully deposited to Zenodo. Zenodo response: {zen_resp.text}")
        return tdm


    @staticmethod
    def __get_community_identifiers(submitted_zenodo_metadata: str) -> List[str]:
        try:
            data = json.loads(submitted_zenodo_metadata)
        except json.JSONDecodeError as exc:
            logging.warning("Failed parsing JSON for community identifiers: %s", exc)
            return []

        communities = data.get("metadata", {}).get("communities", [])
        identifiers: List[str] = []
        for c in communities:
            if isinstance(c, dict) and "identifier" in c:
                val = c.get("identifier")
                if val is not None:
                    identifiers.append(str(val))
        return identifiers

    def __get_community_id(self, slug) -> Optional[str]:
        url = f"{self.target.base_url}/api/communities/{slug}"
        try:
            resp = requests.get(url, headers=self.headers, timeout=30)
            resp.raise_for_status()
        except RequestException as exc:
            logging.warning("HTTP request failed fetching community %s: %s", slug, exc)
            return None

        try:
            data = resp.json()
        except json.JSONDecodeError as exc:
            logging.warning("Failed parsing community response JSON: %s", exc)
            return None

        cid = data.get('id')
        if cid:
            logging.info("Found community id: %s", cid)
            return cid
        logging.warning("Community response did not contain 'id': %s", data)
        return None

    def __add_deposition_to_community(self, zenodo_id: int, community_id: str) -> bool:
        url = f"{self.target.base_url}/api/records/{zenodo_id}/draft/review"
        payload = {"receiver": {"community": community_id}, "type": "community-submission"}

        try:
            resp = requests.put(url, headers=self.headers, data=json.dumps(payload), timeout=30)
        except RequestException as exc:
            logging.warning("Network error when adding deposition %s to community %s: %s", zenodo_id, community_id, exc)
            return False

        if resp.status_code in (200, 201, 202, 204):
            logging.info("Successfully added deposition %s to community %s (HTTP %s)", zenodo_id, community_id,
                         resp.status_code)
            try:
                if resp.text:
                    logging.debug("Response JSON: %s", resp.json())
            except json.JSONDecodeError:
                logging.debug("Response text (non-JSON): %s", resp.text)
            return True

        logging.warning("Failed to add deposition to community. HTTP %s: %s", resp.status_code, resp.text)
        return False


    @handle_deposit_exceptions
    def __create_initial_dataset(self) -> dict | None:
        """
        Creates an initial dataset on Zenodo.

        This method sends a POST request to the Zenodo API to create an initial dataset.

        Returns:
        dict | None: The response from the Zenodo API if the dataset is created successfully, otherwise None.
        """
        logging.info('Create an initial zenodo dataset')
        response = requests.post(f"{self.target.base_url}/api/deposit/depositions",
                                 data="{}", headers=self.headers)
        logging.info(f"Response status code: {response.status_code}")
        return response.json() if response.status_code == 201 else None

    def __ingest_files(self, bucket_url: str) -> dict:
        """
        Ingests files into the Zenodo bucket.

        This method uploads files to the specified Zenodo bucket URL.

        Parameters:
        bucket_url (str): The URL of the Zenodo bucket where files will be uploaded.

        Returns:
        dict: A dictionary containing the status of the file ingestion process.
        """
        logging.info(f'Ingesting files to {bucket_url}')
        params = {'access_token': self.target.password, 'access_right': 'restricted'}
        for file in self.db_manager.find_non_registered_files(dataset_id=self.dataset_id):
            file_path = f"{file.path}"
            logging.info(f'Ingesting file {file_path}')
            with open(file_path, "rb") as fp:
                response = requests.put(f"{bucket_url}/{file.name}", data=fp, params=params)
            logging.info(f"Response status code: {response.status_code} and message: {response.text}")
        return {"status": status.HTTP_200_OK}

    @handle_deposit_exceptions
    def __submit_review(self, zenodo_id: int) -> bool:
        """POST to /api/records/{zenodo_id}/draft/actions/submit-review to submit the community review.

        Returns True on success.
        """
        url = f"{self.target.base_url}/api/records/{zenodo_id}/draft/actions/submit-review"

        resp = requests.post(url, headers=self.headers, timeout=30)
        if resp.status_code in (200, 201, 202):
            print(f"Successfully submitted review for {zenodo_id} (HTTP {resp.status_code})")
            try:
                print('Response:', resp.json())
            except Exception:
                print('Response text:', resp.text)
            return True

        print(f"Failed to submit review. HTTP {resp.status_code}: {resp.text}")
        return False

    @handle_deposit_exceptions
    def __publish_dataset(self, zenodo_id: int) -> dict | None:
        """
        Publishes the dataset on Zenodo.

        This method sends a POST request to the Zenodo API to publish the dataset.

        Parameters:
        zenodo_id (int): The ID of the Zenodo dataset to be published.

        Returns:
        dict | None: The response from the Zenodo API if the dataset is published successfully, otherwise None.
        """
        logging.info(f'Publishing zenodo dataset with id {zenodo_id}')
        response = requests.post(f"{self.target.target_url}/{zenodo_id}/actions/publish?{self.target.username}={self.target.password}",
                                 headers={"Content-Type": "application/json"})
        logging.info(f"Response status code: {response.status_code}")
        return response.json() if response.status_code == 202 else None

class PrereserveDoi(BaseModel):
    doi: Optional[str] = None
    recid: Optional[int] = None


class Metadata(BaseModel):
    access_right: Optional[str] = None
    prereserve_doi: Optional[PrereserveDoi] = None


class Links(BaseModel):
    self: Optional[str] = None
    html: Optional[str] = None
    badge: Optional[str] = None
    files: Optional[str] = None
    bucket: Optional[str] = None
    latest_draft: Optional[str] = None
    latest_draft_html: Optional[str] = None
    publish: Optional[str] = None
    edit: Optional[str] = None
    discard: Optional[str] = None
    newversion: Optional[str] = None
    registerconceptdoi: Optional[str] = None


class ZenodoModel(BaseModel):
    created: Optional[str] = None
    modified: Optional[str] = None
    id: Optional[int] = None
    conceptrecid: Optional[str] = None
    metadata: Optional[Metadata] = None
    title: Optional[str] = None
    links: Optional[Links] = None
    record_id: Optional[int] = None
    owner: Optional[int] = None
    files: List[Any] = Field(default_factory=list)
    state: Optional[str] = None
    submitted: Optional[bool] = None


json_data_zenodo_model = '''{
    "created": "2023-12-11T17:50:54.342124+00:00",
    "modified": "2023-12-11T17:50:54.380509+00:00",
    "id": 10358181,
    "conceptrecid": "10358180",
    "metadata": {
        "access_right": "open",
        "prereserve_doi": {
            "doi": "10.5281/zenodo.10358181",
            "recid": 10358181
        }
    },
    "title": "",
    "links": {
        "self": "https://zenodo.org/api/deposit/depositions/10358181",
        "html": "https://zenodo.org/deposit/10358181",
        "badge": "https://zenodo.org/badge/doi/.svg",
        "files": "https://zenodo.org/api/deposit/depositions/10358181/files",
        "bucket": "https://zenodo.org/api/files/b40b73d8-7550-415d-b91e-b981b13e61be",
        "latest_draft": "https://zenodo.org/api/deposit/depositions/10358181",
        "latest_draft_html": "https://zenodo.org/deposit/10358181",
        "publish": "https://zenodo.org/api/deposit/depositions/10358181/actions/publish",
        "edit": "https://zenodo.org/api/deposit/depositions/10358181/actions/edit",
        "discard": "https://zenodo.org/api/deposit/depositions/10358181/actions/discard",
        "newversion": "https://zenodo.org/api/deposit/depositions/10358181/actions/newversion",
        "registerconceptdoi": "https://zenodo.org/api/deposit/depositions/10358181/actions/registerconceptdoi"
    },
    "record_id": 10358181,
    "owner": 548524,
    "files": [],
    "state": "unsubmitted",
    "submitted": false
}
'''
# x = json.loads(json_data_zenodo_model)
# zm = ZenodoModel(**x)
# print(zm.links.self)
