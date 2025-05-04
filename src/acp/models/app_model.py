from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Union

from pydantic import BaseModel, Field


class ResponseDataModel(BaseModel):
    """
    Data model for response data.
    The ResponseDataModel class is a data model defined using the BaseModel class from the pydantic library.
    This class is designed to represent the response data structure with specific attributes.
    The class includes three attributes: status, dataset_id, and start_process.
    The status attribute is a string that represents the status of the response and is initialized with an empty string

    Attributes:
        status (str): The status of the response.
        dataset_id (str): The ID of the dataset, aliased as 'dataset-id'.
        start_process (Optional[bool]): Indicates whether to start the process, aliased as 'start-process'.
    """
    status: str = ''
    dataset_id: str = Field('', alias='dataset-id')
    start_process: Optional[bool] = Field(False, alias='start-process')


@dataclass(frozen=True, kw_only=True)
class InboxDatasetDataModel:
    """
    Data model for inbox dataset.
    The InboxDatasetDataModel class is a data model defined using Python's dataclasses plugin.
    This class is designed to represent the metadata of an inbox dataset.

    Attributes:
        assistant_name (str): The name of the assistant.
        target_creds (str): The credentials for the target.
        owner_id (str): The ID of the owner.
        title (str): The title of the dataset. Defaults to an empty string.
        metadata (dict): The metadata associated with the dataset.
        release_version (str): The release version of the dataset.
    """
    id : str = ""
    assistant_name: str
    target_creds: str
    owner_id: str
    title: str = ''
    metadata_content: dict
    metadata_type: str = ''
    status: str


class TargetApp(BaseModel):
    """
    Data model for target application.

    Attributes:
        repo_name (str): Repository name, aliased as 'repo-name'.
        display_name (str): Display name of the target application, aliased as 'display-name'.
        deposit_status (str): Deposit status, aliased as 'deposit-status'.
        deposited_at (datetime | str): Deposit time, aliased as 'deposit-time'.
        deposit_duration (float | str): Duration of the deposit process.
        output_response (dict): Output response from the target application, aliased as 'output-response'.
        external_identifiers (Union[str, List[dict]]): External identifiers, aliased as 'deposited-identifiers'.
        diff (dict): Differences or changes in the target application.
    """
    repo_name: str = Field(None, alias='repo-name')
    display_name: str = Field(None, alias='display-name')
    deposit_status: str = Field(None, alias='deposit-status')
    deposited_at: datetime | str = Field(None, alias='deposit-time')
    deposit_duration: float | str = ''
    output_response: dict = Field(None, alias='output-response')
    external_identifiers: Union[str, List[dict]] = Field(None, alias='deposited-identifiers')
    diff: dict = {}

class Asset(BaseModel):
    """
    Data model for an asset.

    The Asset class is a data model defined using the BaseModel class from the Pydantic library.
    It represents an asset with various attributes related to its metadata and associated target applications.

    Attributes:
        dataset_id (str): The ID of the dataset, aliased as 'dataset-id'.
        title (str): The title of the asset.
        md (dict | str): The metadata content of the asset, which can be a dictionary or a string.
        created_at (datetime | str): The creation date of the asset, aliased as 'created-at'.
        saved_at (datetime | str): The saved date of the asset, aliased as 'saved-at'.
        submitted_at (datetime | str): The submitted date of the asset, aliased as 'submitted-at'.
        deposited_version (str): The deposited version of the asset, aliased as 'deposited-version'.
        status (str): The status of the asset, aliased as 'status'.
        acp_version (str): The ACP version of the asset, aliased as 'acp-version'.
        targets (List[TargetApp]): A list of TargetApp objects representing the target applications associated with the asset.
    """
    dataset_id: str = Field(None, alias='dataset-id')
    title: str = ''
    md: dict | str = ''
    created_at: datetime | str = Field(None, alias='created-at')
    saved_at: datetime | str = Field(None, alias='saved-at')
    submitted_at: datetime | str = Field(None, alias='submitted-at')
    deposited_version: str = Field(None, alias='deposited-version')
    status: str = Field(None, alias='status')
    acp_version: str = Field(None, alias='acp-version')
    targets: List[TargetApp] = []


class OwnerAssetsModel(BaseModel):
    """
    Data model for owner assets.

    This class represents the assets owned by a specific owner, encapsulating the owner's ID and a list of assets.
    It is defined using the Pydantic `BaseModel` class, which provides data validation and serialization.

    Attributes:
        owner_id (str): The ID of the owner. This field is aliased as 'owner-id' for compatibility with external data sources.
        assets (List[Asset]): A list of `Asset` objects representing the assets owned by the owner.
    """
    owner_id: str = Field(None, alias='owner-id')  # The owner's ID, aliased as 'owner-id'.
    assets: List[Asset] = []  # A list of assets owned by the owner.
