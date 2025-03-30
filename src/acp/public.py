import json
import logging

from fastapi import APIRouter, Request, HTTPException
from starlette.responses import Response

# from src import db
from src.acp.commons import data, app_settings, retrieve_targets_configuration, create_asset
from src.acp.models.app_model import OwnerAssetsModel
from src.acp.models.assistant_datamodel import RepoAssistantDataModel

router = APIRouter()


@router.get("/available-plugins")
async def get_plugins_list():
    """
    Endpoint to retrieve a list of available plugins.

    This endpoint returns a sorted list of keys from the `data` dictionary,
    which represents the available plugins in the system.

    Returns:
        list: A sorted list of available plugin names.
    """
    return sorted(list(data.keys()))


@router.get("/progress-state/{owner_id}")
async def progress_state(owner_id: str, req: Request, page: int = 1, page_size: int = 10):
    """
    Endpoint to retrieve the progress state of assets owned by a specific owner.

    Args:
        owner_id (str): The ID of the owner whose assets' progress state is to be retrieved.
        req (Request): The incoming request object.
        page (int, optional): The page number for pagination. Defaults to 1.
        page_size (int, optional): The number of items per page for pagination. Defaults to 10.

    Returns:
        list: A list of rows representing the progress state of the owner's assets.
              If no assets are found, an empty list is returned.
    """
    # Retrieve the 'targets-credentials' header from the request
    tc_header = req.headers.get('targets-credentials')
    assistant_name = req.headers.get('assistant-config-name')
    if not tc_header or not assistant_name:
            raise HTTPException(status_code=400, detail="Targets credentials are missing")

    # Attempt to parse the 'targets-credentials' header as JSON
    try:
        target_creds = json.loads(tc_header)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid json format of targets-credentials")
    repo_config = retrieve_targets_configuration(assistant_name)
    repo_assistant = RepoAssistantDataModel.model_validate_json(repo_config)
    app_name = repo_assistant.app_name
    db_manager = data[app_name]
    # Find datasets by owner ID
    datasets = db_manager.find_datasets_by_owner(owner_id, page, page_size)
    if datasets:
        oam = OwnerAssetsModel()
        oam.owner_id = owner_id
        for dataset in datasets:
            asset = await create_asset(dataset, db_manager, target_creds)
            oam.assets.append(asset)
        return oam
    return []


@router.get("/dataset/{dataset_id}")
async def find_dataset(dataset_id: str, req: Request):
    """
    Endpoint to retrieve a dataset and its associated targets by dataset ID.

    Args:
        datasetId (str): The ID of the dataset to be retrieved.

    Returns:
        Response: A JSON response containing the dataset and its associated targets if found,
                  otherwise an empty dictionary.
    """
    # logging.debug(f'find_metadata_by_metadata_id - metadata_id: {metadata_id}')
    logging.info(f'find_metadata_by_metadata_id - metadata_id: {dataset_id}')
    assistant_name = req.headers.get('assistant-config-name')
    if assistant_name is None:
        raise HTTPException(status_code=400, detail="assistant-config-name")

    # Attempt to parse the 'targets-credentials' header as JSON

    repo_config = retrieve_targets_configuration(assistant_name)
    repo_assistant = RepoAssistantDataModel.model_validate_json(repo_config)
    app_name = repo_assistant.app_name
    db_manager = data[app_name]
    asset = db_manager.find_dataset_and_targets(dataset_id, exclude_target=True)
    if asset.dataset_id:
        try:
            pretty_json = json.dumps(json.loads(asset.md), indent=4)
            return Response(content=pretty_json, media_type="application/json")
        except json.JSONDecodeError:
            return Response(content=asset.md, media_type="application/xml")

    return {}

@router.get("/utils/languages")
async def get_languages():
    """
    Endpoint to retrieve the list of supported languages.

    This endpoint reads a JSON file specified by the `LANGUAGES_PATH` setting
    and returns its contents, which represent the supported languages.

    Returns:
        dict: A dictionary containing the supported languages.

    Example:
        Request:
            GET /utils/languages

        Response:
            {
                "en": "English",
                "fr": "French",
                "es": "Spanish"
            }
    """
    with open(app_settings.LANGUAGES_PATH, "r") as f:
        languages = json.load(f)
    return languages