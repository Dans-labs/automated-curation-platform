# Import necessary plugins and packages
# Import necessary libraries and plugins
import logging
import mimetypes
import os
import shutil

from fastapi import APIRouter, Request, HTTPException
from starlette.responses import FileResponse

from src.acp.commons import app_settings, data, get_repo_assistant

# Import custom plugins and classes

# Create an API router instance
router = APIRouter()


# Endpoint to register a bridge plugin
@router.post("/register-bridge-plugin/{name}/{overwrite}")
async def register_plugin(name: str, bridge_file: Request, overwrite: bool | None = False) -> {}:
    """
    Endpoint to register a bridge plugin.

    This endpoint registers a new bridge plugin by saving the provided Python file
    to the specified plugins directory. If the plugin already exists and overwrite
    is not specified, an error is raised.

    Args:
        name (str): The name of the bridge plugin to be registered.
        bridge_file (Request): The request containing the bridge plugin file.
        overwrite (bool, optional): Flag to indicate if the existing plugin should be overwritten. Defaults to False.

    Returns:
        dict: A dictionary containing the status and the name of the registered bridge plugin.

    Raises:
        HTTPException: If the plugin already exists and overwrite is not specified.
        HTTPException: If the content type of the provided file is not 'text/x-python'.
        HTTPException: If the file type of the provided file is not 'text/x-python'.
    """
    logging.error(f'Registering {name}')
    if not overwrite and name in data["bridge-plugins"]:
        raise HTTPException(status_code=400,
                            detail=f'The {name} is already exist. Consider /register-bridge-plugin/{name}/true')

    if bridge_file.headers['Content-Type'] != 'text/x-python':
        raise HTTPException(status_code=400, detail="Unsupported content type")

    m_file = await bridge_file.body()
    bridge_path = os.path.join(app_settings.PLUGINS_DIR, name)
    with open(bridge_path, "w+") as file:
        file.write(m_file.decode())

    if mimetypes.guess_type(bridge_path)[0] != 'text/x-python':
        os.remove(bridge_path)
        raise HTTPException(status_code=400, detail='Unsupported file type')

    return {"status": "OK", "bridge-plugin-name": name}

#
@router.delete("/inbox/{dataset_id}", include_in_schema=False)
async def delete_inbox(dataset_id: str, req: Request):
    """
    Endpoint to delete an inbox dataset.

    This endpoint deletes the dataset identified by the given dataset ID from the database.

    Args:
        datasetId (str): The ID of the dataset to be deleted.

    Returns:
        dict: A dictionary containing the status of the deletion and the number of rows deleted.
    """
    repo_assistant = await get_repo_assistant(req)
    db_manager = data[repo_assistant.app_name]

    dataset_id = db_manager.find_draft_dataset_id_by_md_id(dataset_id)
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



# Endpoint to retrieve application app_settings
@router.get("/app_settings-reload", include_in_schema=False)
async def get_app_settings():
    """
    Endpoint to retrieve and reload application app_settings.

    This endpoint retrieves the current application app_settings, reloads them, and returns the updated app_settings.

    Returns:
        dict: A dictionary containing the updated application app_settings.
    """
    logging.info(f"Getting app_settings Before Load: {app_settings.as_dict()}")
    logging.info("Reload app_settings")
    app_settings.reload()
    logging.info(f"Getting app_settings After Load: {app_settings.as_dict()}")
    return app_settings.as_dict()


@router.get('/logs/{app_name}', include_in_schema=False)
def get_log(app_name: str):
    """
    Endpoint to retrieve a specific log file.

    This endpoint returns the log file for the specified application name.

    Args:
        app_name (str): The name of the application whose log file is to be retrieved.

    Returns:
        FileResponse: A response object that allows the client to download the log file.

    Logs:
        Logs the action of retrieving the log file.
    """
    logging.info('logs')
    return FileResponse(path=f"{os.environ['BASE_DIR']}/logs/{app_name}.log", filename=f"{app_name}.log",
                        media_type='text/plain')


@router.get("/logs-list", include_in_schema=False)
def get_log_list():
    """
    Endpoint to retrieve the list of log files.

    This endpoint returns a list of log files present in the logs directory.

    Returns:
        list: A list of log file names.

    Raises:
        FileNotFoundError: If the logs directory does not exist.
    """
    logging.info('logs-list')
    return os.listdir(path=f"{os.environ['BASE_DIR']}/logs")


@router.get("/db-download", include_in_schema=False)
def get_db():
    """
    Endpoint to download the database file.

    This endpoint returns the database file as a downloadable response.

    Returns:
        FileResponse: A response object that allows the client to download the database file.

    Logs:
        Logs the action of downloading the database file.
    """
    logging.info('db-download')
    return FileResponse(path=app_settings.DB_URL, filename="acp.db",
                        media_type='application/octet-stream')


@router.delete("/db-delete-all", include_in_schema=False)
async def delete_all_recs(req: Request):
    """
    Endpoint to delete all records from the database.

    This endpoint deletes all records from the database by calling the `delete_all` method
    of the `db_manager` object.

    Returns:
        dict: A dictionary containing the status of the deletion.

    Logs:
        Logs the action of deleting all records.
    """
    logging.info('Deleting all')
    repo_assistant = await get_repo_assistant(req)
    db_manager = data[repo_assistant.app_name]
    return db_manager.delete_all()



# Endpoint to retrieve application app_settings
@router.get("/app_settings-reload", include_in_schema=False)
async def get_settings():
    """
    Endpoint to retrieve and reload application app_settings.

    This endpoint retrieves the current application app_settings, reloads them, and returns the updated app_settings.

    Returns:
        dict: A dictionary containing the updated application app_settings.
    """
    logging.info(f"Getting app_settings Before Load: {app_settings.as_dict()}")
    logging.info("Reload app_settings")
    app_settings.reload()
    logging.info(f"Getting app_settings After Load: {app_settings.as_dict()}")
    return app_settings.as_dict()
