"""
Automated Curation Platform FastAPI Application

This FastAPI application provides endpoints for file uploads, public access, and protected access.
It integrates Keycloak for OAuth2-based authentication and supports token-based authentication with API keys.

Modules:
- `public`: Contains public access routes.
- `protected`: Contains protected access routes.
- `tus_files`: Contains routes for handling file uploads using the Tus protocol.
- `commons`: Contains common app_settings, logger setup, and utility functions.
- `InspectBridgeModule`: Provides a utility for inspecting bridge plugin classes.
- `db_manager`: Manages the creation of the database and tables.

Dependencies:
- `fastapi`: Web framework for building APIs with Python.
- `starlette`: Asynchronous framework for building APIs.
- `uvicorn`: ASGI server for running the FastAPI application.
- `keycloak`: Provides integration with Keycloak for authentication.
- `emoji`: Library for adding emoji support to Python applications.

"""
import logging
# import importlib.metadata
import os
import sys

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from contextlib import asynccontextmanager
from typing import Annotated

import emoji
import uvicorn
from akmi_utils import commons as a_commons
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from keycloak import KeycloakOpenID, KeycloakAuthenticationError
from starlette import status
from starlette.middleware.cors import CORSMiddleware

from src.acp.api import protected, protected_admin, public
from src.acp.commons import app_settings, data, inspect_bridge_plugin, \
    get_version, get_name, project_details, get_db_manager, retrieve_apps_list
from src.acp.jobs.rq_queue import initialize_queues
from src.acp.tus_files import upload_files


@asynccontextmanager
async def lifespan(application: FastAPI):
    """
    Lifespan event handler for the FastAPI application.

    This function is executed during the startup of the FastAPI application.
    It initializes the database, RQ queues, iterates through saved bridge plugin directories,
    and prints available bridge classes.

    Args:
        application (FastAPI): The FastAPI application.

    Yields:
        None: The context manager does not yield any value.

    """
    print('start up')

    # Initialize RQ queues for background jobs
    initialize_queues()

    apps = retrieve_apps_list()

    if not apps:
        raise RuntimeError("No apps found. Cancelling startup.")

    for app in apps:
        db_manager = get_db_manager(app)
        db_manager.create_db_and_tables()
        data.update({app: db_manager})
    iterate_saved_bridge_plugin_dir()
    print(f'Available bridge classes: {sorted(list(data.keys()))}')
    print(emoji.emojize(':thumbs_up:'))

    yield


api_keys = [app_settings.ACP_SERVICE_API_KEY]

security = HTTPBearer()

APP_NAME = os.environ.get("APP_NAME", project_details['title'])
EXPOSE_PORT = int(os.environ.get("EXPOSE_PORT", 10124))
OTLP_GRPC_ENDPOINT = os.environ.get("OTLP_GRPC_ENDPOINT", "http://localhost:4317")

def auth_header(request: Request, auth_cred: Annotated[HTTPAuthorizationCredentials, Depends(security)]):
    """
    Simplified authentication header dependency function.

    This function checks the provided API key against a list of valid keys or attempts to authenticate using Keycloak.

    Args:
        request (Request): The FastAPI request object.
        auth_cred: The authorization credentials from the request.

    Raises:
        HTTPException: Raised if authentication fails.
    """
    api_key = auth_cred.credentials
    if api_key in api_keys:
        return

    keycloak_env = app_settings.get(f"keycloak_{request.headers.get('auth-env-name')}")
    if not keycloak_env:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Forbidden")

    try:
        KeycloakOpenID(
            server_url=keycloak_env.URL,
            client_id=keycloak_env.CLIENT_ID,
            realm_name=keycloak_env.REALMS
        ).userinfo(api_key)
    except KeycloakAuthenticationError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Forbidden")

def pre_startup_routine(app: FastAPI) -> None:

    # Enable CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Upload-Offset", "Location", "Upload-Length", "Tus-Version", "Tus-Resumable", "Tus-Max-Size",
                        "Tus-Extension", "Upload-Metadata", "Upload-Defer-Length", "Upload-Concat", "Upload-Incomplete",
                        "Upload-Complete", "Upload-Draft-Interop-Version"],

    )


build_date = os.environ.get("BUILD_DATE", "unknown")

os.environ["acp_version"] = f"{project_details['version']} (Build Date: {build_date})"
app = FastAPI(
    title=project_details['title'],
    description=project_details['description'],
    version=os.environ.get("acp_version", "unknown"),
    lifespan=lifespan
)

LOG_FILE = app_settings.LOG_FILE
log_config = uvicorn.config.LOGGING_CONFIG
logging.basicConfig(filename=app_settings.LOG_FILE, level=app_settings.LOG_LEVEL,
                        format=app_settings.LOG_FORMAT)

if app_settings.otlp_enable is False:
    logging.info("Logging configured without OTLP")
else:
    logging.info("OTLP enabled")
    a_commons.set_otlp(app, APP_NAME, OTLP_GRPC_ENDPOINT, LOG_FILE, log_config)

pre_startup_routine(app)


# register routers
app.include_router(public.router, tags=["Public"], prefix="")
app.include_router(protected.router, tags=["Protected"], prefix="", dependencies=[Depends(auth_header)])
app.include_router(protected_admin.router, tags=["Admin"], prefix="", dependencies=[Depends(auth_header)])

app.include_router(upload_files, prefix="/files", include_in_schema=True, dependencies=[Depends(auth_header)])
# app.include_router(tus_files.router, prefix="", include_in_schema=False)


@app.get('/')
def info():
    """
    Root endpoint to retrieve information about the automated curation platform.

    Returns:
        dict: A dictionary containing the name and version of the automated curation platform.

    """
    return {"name": get_name(), "version": get_version()}


def iterate_saved_bridge_plugin_dir():
    """
    Iterates through saved bridge plugin directories.

    For each Python file in the plugins directory, it inspects the file for bridge classes
    and updates the data dictionary with the class name.

    """
    for filename in os.listdir(app_settings.PLUGINS_DIR):
        if filename.endswith(".py") and not filename.startswith('__'):
            plugins_path = os.path.join(app_settings.PLUGINS_DIR, filename)
            for cls_name in inspect_bridge_plugin(plugins_path):
                data.update(cls_name)



if __name__ == "__main__":
    print("Starting the application...")
    print("Database dialect:", app_settings.DB_DIALECT)
    print("Database URL:", app_settings.DB_URL)
    logging.info('START Automated Curation Platform')
    logging.info(f'APP_NAME: {APP_NAME}')
    logging.info(f'Database dialect: {app_settings.DB_DIALECT}')
    logging.info("Database URL: %s", app_settings.DB_URL)
    logging.info(f'app_settings: {app_settings.to_dict()}')
    uvicorn.run(app, host="0.0.0.0", port=EXPOSE_PORT, log_config=log_config)