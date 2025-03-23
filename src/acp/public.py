import json
import logging
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from starlette.responses import Response

# from src import db
from src.acp.commons import data, db_manager, app_settings, fetch_dv_json
from src.acp.dbz import StateVersion
from src.acp.models.app_model import OwnerAssetsModel, Asset, TargetApp

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
    if tc_header is None:
        raise HTTPException(status_code=400, detail="Targets credentials are missing")

    # Attempt to parse the 'targets-credentials' header as JSON
    try:
        target_creds = json.loads(tc_header)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid json format of targets-credentials")

    # Find datasets by owner ID
    datasets = db_manager.find_datasets_by_owner(owner_id, page, page_size)
    if datasets:
        oam = OwnerAssetsModel()
        oam.owner_id = owner_id
        for dataset in datasets:
            asset = Asset()
            asset.md_id = str(dataset.md_id)
            asset.md_version = dataset.version
            asset.title = dataset.title
            asset.created_date = dataset.created_date.strftime('%Y-%m-%d %H:%M:%S')
            asset.saved_date = dataset.saved_date.strftime('%Y-%m-%d %H:%M:%S')
            asset.submitted_date = dataset.submitted_date.strftime(
                '%Y-%m-%d %H:%M:%S') if dataset.submitted_date else ''
            asset.md_state_version = dataset.md_state_version
            asset.version = dataset.version if dataset.version else ''

            # Find target repositories by dataset ID
            targets_repo = db_manager.find_target_repos_by_dataset_id(dataset.id)
            # logging.info(f'dataset state version: {dataset.md_state_version}')

            # Process target repositories if the dataset is not in DRAFT release version
            if dataset.md_state_version is not StateVersion.DRAFT:
                for target_repo in targets_repo:
                    target = TargetApp()
                    target.repo_name = target_repo.name
                    target.display_name = target_repo.display_name
                    target.deposit_status = target_repo.deposit_status
                    target.deposit_time = target_repo.deposit_time.strftime(
                        '%Y-%m-%d %H:%M:%S') if target_repo.deposit_time else ''
                    target.duration = str(target_repo.duration)

                    # Parse the target repository output as JSON if available
                    rsp = json.loads(target_repo.target_output) if target_repo.target_output else {}
                    if rsp:
                        idents = rsp['response']['identifiers']
                        target.output_response = {"response": {"identifiers": idents}}

                        # Fetch diff for Dataverse if URL contains "dataset.xhtml"
                        if idents:
                            url = rsp['response']['identifiers'][0]['url']
                            if url.find("dataset.xhtml") > 0:
                                logging.info(f'fetching diff for {target.repo_name}')
                                target.diff = await fetch_dv_json(rsp, target, target_creds, url)
                        else:
                            target.output_response = {}
                    else:
                        target.output_response = {}

                    asset.targets.append(target)
            oam.assets.append(asset)
        return oam
    return []


@router.get("/dataset/{metadata_id}")
async def find_dataset(metadata_id: str, md_state_version: Optional[str] = 'DRAFT'):
    """
    Endpoint to retrieve a dataset and its associated targets by dataset ID.

    Args:
        datasetId (str): The ID of the dataset to be retrieved.

    Returns:
        Response: A JSON response containing the dataset and its associated targets if found,
                  otherwise an empty dictionary.
    """
    # logging.debug(f'find_metadata_by_metadata_id - metadata_id: {metadata_id}')
    logging.info(f'find_metadata_by_metadata_id - metadata_id: {metadata_id}')
    state_version = StateVersion(md_state_version)
    dataset = db_manager.find_dataset_and_targets_by_md_id_and_state_version(metadata_id, state_version)
    if dataset.md_id:
        try:
            dataset.md = json.loads(dataset.md)
            return Response(content=dataset.model_dump_json(by_alias=True), media_type="application/json")
        except json.JSONDecodeError:
            return Response(content=dataset.md, media_type="application/xml")

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