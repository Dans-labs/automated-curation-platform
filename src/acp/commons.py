import ast
import importlib
import inspect
import json
import logging
import os
import re
import shutil
import smtplib
import zipfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from tempfile import NamedTemporaryFile
from typing import Any, Callable

import boto3
import psutil
import requests
from akmi_utils import commons as a_commons
from dynaconf import Dynaconf
from fastapi import HTTPException
# from hypothesis import app_settings
from jsoncomparison import Compare
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor
from starlette import status

from src.acp.db.dbz import DatabaseManager, DepositStatus, StateVersion, DatasetStatus
from src.acp.models.app_model import Asset, TargetApp
from src.acp.models.assistant_datamodel import ProcessedMetadata, RepoAssistantDataModel
from src.acp.models.bridge_output_model import TargetDataModel, TargetResponse

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["BASE_DIR"] = os.getenv("BASE_DIR", base_dir)

app_settings = Dynaconf(root_path=f'{os.environ["BASE_DIR"]}/conf', settings_files=["*.toml"],
                    environments=True)
data = {}

project_details = a_commons.get_project_details(os.getenv("BASE_DIR"), ['name', 'version', 'description', 'title'])

db_dialect = os.getenv("DB_DIALECT", app_settings.DB_DIALECT)
db_url = os.getenv("DB_URL", app_settings.DB_URL)
encryption_key = os.getenv("DB_ENCRYPTION_KEY", app_settings.DB_ENCRYPTION_KEY)

def get_db_manager(app_name: str):
    return DatabaseManager(db_dialect=db_dialect,
                           db_url=db_url,
                           encryption_key=encryption_key,
                           app_name= app_name)

transformer_headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {app_settings.METADATA_TRANSFORMER_SERVICE_API_KEY}'
}

transformer_headers_xml = {
    'Content-Type': 'application/xml',
    'Authorization': f'Bearer {app_settings.METADATA_TRANSFORMER_SERVICE_API_KEY}'
}

assistant_repo_headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {app_settings.ACP_CONFIG_ASSISTANT_SERVICE_API_KEY}'
}

def get_version() -> str:
    """Retrieve the version of the package."""
    return project_details['version']


def get_name() -> str:
    """Retrieve the name of the package."""
    return project_details['name']



def get_class(kls) -> Any:

    """
    This function dynamically imports a class from a plugin.

    It takes a string `kls` as input, which should be the fully qualified name of a class (i.e., including its plugin path).
    The string is split into parts, and the plugin path is reconstructed by joining all parts except the last one.
    The plugin is then imported using the `__import__` function, and the class is retrieved using `getattr`.

    If the plugin cannot be found, a `ModuleNotFoundError` is caught and logged, and the function returns `None`.

    Parameters:
    kls (str): The fully qualified name of a class to import.

    Returns:
    Any: The class if it can be imported, or `None` otherwise.
    """
    parts = kls.split('.')
    plugin = ".".join(parts[:-1])
    try:
        m = __import__(plugin)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m
    except ModuleNotFoundError as e:
        print(f'error: {kls}')
        logging.error(f'ModuleNotFoundError: {e}')
    return None

def transform(transformer_url: str, str_tobe_transformed: str, headers: {} = None) -> str:
    """
    Transforms a given string using a specified transformer service.

    This function sends a POST request to the transformer service with the string to be transformed.
    If the transformation is successful (HTTP status code 200), it returns the transformed result.
    Otherwise, it raises a ValueError with the response status code.

    Parameters:
    transformer_url (str): The URL of the transformer service.
    str_tobe_transformed (str): The string to be transformed.
    headers (dict, optional): The headers to include in the request. Defaults to `transformer_headers`.

    Returns:
    str: The transformed string if the request is successful.

    Raises:
    ValueError: If `str_tobe_transformed` is not a string or if the response status code is not 200.
    """
    logging.info(f'transformer_url: {transformer_url}')
    if not isinstance(str_tobe_transformed, str):
        raise ValueError(f"Error - str_tobe_transformed is not a string. It is : {type(str_tobe_transformed)}")
    if headers is None:
        headers = transformer_headers

    response = requests.post(transformer_url, headers=headers, data=str_tobe_transformed)
    if response.status_code == 200:
        return response.json().get('result')

    logging.error(f'transformer_response.status_code: {response.status_code}')
    logging.error(f'transformer_response.text: {response.text}')
    logging.error(f'transformer_response.headers: {response.headers}')
    logging.error(f'send header: {headers}')
    logging.error(f'send body: {str_tobe_transformed}')
    raise ValueError(f"Error - Transformer response status code: {response.status_code}")

def transform_json(transformer_url: str, str_tobe_transformed: str) -> str:
    """
    Transforms a given string using a specified transformer service with JSON headers.

    This function calls the `transform` function with the provided transformer URL and string to be transformed,
    using the `transformer_headers` for the request headers.

    Parameters:
    transformer_url (str): The URL of the transformer service.
    str_tobe_transformed (str): The string to be transformed.

    Returns:
    str: The transformed string if the request is successful.
    """
    return transform(transformer_url, str_tobe_transformed, transformer_headers)



def transform_xml(transformer_url: str, str_tobe_transformed: str) -> str:
    """
    Transforms a given string using a specified transformer service with XML headers.

    This function calls the `transform` function with the provided transformer URL and string to be transformed,
    using the `transformer_headers_xml` for the request headers.

    Parameters:
    transformer_url (str): The URL of the transformer service.
    str_tobe_transformed (str): The string to be transformed.

    Returns:
    str: The transformed string if the request is successful.
    """
    return transform(transformer_url, str_tobe_transformed, transformer_headers_xml)



def handle_deposit_exceptions(
        func) -> Callable[[tuple[Any, ...], dict[str, Any]], TargetDataModel | Any]:
    """
    This function is a decorator that wraps around a function to handle exceptions during the deposit process.

    It logs the entry into the function it is decorating, then attempts to execute the function.
    If an exception is raised during the execution of the function, it logs the error and creates a BridgeOutputDataModel
    instance with an error status and a TargetResponse instance containing the error details.

    The decorated function should take a BridgeOutputDataModel instance as its first argument.

    Parameters:
    func (Callable): The function to be decorated.

    Returns:
    Callable: The decorated function.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        #logger(f'Enter to handle_deposit_exceptions for {func.__name__}. args: {args}', app_settings.LOG_LEVEL, LOG_NAME_PS)
        try:
            rv = func(*args, **kwargs)
            return rv
        except Exception as ex:
            logging.error(f'Errors in {func.__name__}: {ex} - {ex.with_traceback(ex.__traceback__)}')
            target = args[0].target
            bom = TargetDataModel()
            bom.deposit_status = DepositStatus.ERROR
            tr = TargetResponse()
            tr.url = target.target_url
            # tr.status = DepositStatus.ERROR
            tr.error = f'error: {ex.with_traceback(ex.__traceback__)}'
            bom.response = tr
            return bom

    return wrapper


def handle_ps_exceptions(func) -> Any:
    """
    This function is a decorator that wraps around a function to handle exceptions during the execution of the function.

    It logs the entry into the function it is decorating, then attempts to execute the function.
    If an HTTPException is raised during the execution of the function, it logs the error and re-raises the exception.
    If any other exception is raised, it sends an email with the error details, logs the error, and re-raises the exception.

    The decorated function can take any number of positional and keyword arguments.

    Parameters:
    func (Callable): The function to be decorated.

    Returns:
    Callable: The decorated function.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logging.debug(f'Enter to handle_ps_exceptions:: {func.__name__}')
            rv = func(*args, **kwargs)
            return rv
        except HTTPException as ex:
            # send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'status code: {ex.status_code}.'
            #                                                               f'\nDetails: {ex.detail}.')
            logging.error(
                f'handle_ps_exceptions: Errors in {func.__name__}. status code: {ex.status_code}. Details: {ex.detail}. '
                f'args: {args}')
            raise ex
        except Exception as ex:
            send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'{ex} - '
                                                                          f'{ex.with_traceback(ex.__traceback__)}.')
            logging.error(f'handle_ps_exceptions: Errors in {func.__name__}: {ex} - {ex.with_traceback(ex.__traceback__)}')
            raise ex
        except BaseException as ex:
            send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'{ex} - '
                                                                          f'{ex.with_traceback(ex.__traceback__)}.')
            logging.error(f'handle_ps_exceptions: Errors in {func.__name__}:  {ex} - {ex.with_traceback(ex.__traceback__)}')
            raise ex

    return wrapper


def inspect_bridge_plugin(py_file_path: str):
    """
    This function inspects a Python plugin and returns a list of classes that inherit from the 'Bridge' class.

    It opens the Python file at the given path and parses it into an AST (Abstract Syntax Tree) using the `ast.parse` function.
    It then iterates over the nodes in the AST, and for each class definition, it checks if it inherits from the 'Bridge' class.
    If it does, it constructs the fully qualified name of the class and adds it to the results list.

    The fully qualified name of a class is constructed by replacing the base directory path in the file path with an empty string,
    replacing all slashes with dots, and appending the class name.

    Parameters:
    py_file_path (str): The path to the Python file to inspect.

    Returns:
    list[dict[str, str]]: A list of dictionaries, where each dictionary has one key-value pair.
                           The key is the name of a class that inherits from the 'Bridge' class,
                           and the value is the fully qualified name of the class.
    """
    with open(py_file_path, 'r') as f:
        bridge_mdl = ast.parse(f.read())
    results = []
    for node in bridge_mdl.body:
        if isinstance(node, ast.ClassDef) and any(
                isinstance(base, ast.Name) and base.id == 'Bridge' for base in node.bases):
            plugin_name = py_file_path.replace(f'{os.getenv("BASE_DIR", os.getcwd())}/', '').replace('/', '.')
            name_of_bridge_subclass = f"{plugin_name[:-3]}.{node.name}"
            results.append({node.name: name_of_bridge_subclass})
    return results



def send_mail(subject: str, text: str, recipients: list[str] = None):
    """
    Send an email with the specified subject and text using SMTP.

    Args:
        subject (str): The subject of the email.
        text (str): The text content of the email.
        recipients (list[str], optional): List of recipient email addresses. Defaults to app_settings.MAIL_TO.

    Raises:
        ValueError: If there is an error sending the email.
    """
    sender_email = app_settings.MAIL_USR
    app_password = app_settings.MAIL_PASS
    recipients = recipients or list(app_settings.MAIL_TO)

    message = MIMEMultipart()
    message['From'] = sender_email
    message['Subject'] = f'{app_settings.get("MAIL_SUBJECT_PREFIX", "mail_subject_prefix not set")}: {subject}'
    message.attach(MIMEText(text, 'plain'))

    if not app_settings.get('send_mail', True):
        logging.info("Sending email is disabled.")
        return

    try:
        with smtplib.SMTP(app_settings.SMTP_SERVER, app_settings.SMTP_PORT) as server:
            if app_settings.get('use_tls', True):
                server.starttls()
                server.login(sender_email, app_password)
            for recipient in recipients:
                message['To'] = recipient
                server.sendmail(sender_email, recipient, message.as_string())
                logging.debug(f"Email sent successfully to {recipient}")
        logging.debug(f"Email sent successfully to all recipients: {recipients}")
    except Exception as e:
        logging.error(f"Failed to send email to {recipients}: {e}")
        raise ValueError(f"Error: {e}")

def dmz_dataverse_headers(username, password) -> dict:
    headers = {}
    if app_settings.exists("dmz_x_authorization_value", fresh=False):
        headers['X-Authorization'] = app_settings.dmz_x_authorization_value
    if username == 'API_KEY':
        headers["X-Dataverse-key"] = password
    return headers


def upload_large_file(url, file_path, json_data, api_key, file_name=None):
    """
    Uploads a large file to a specified URL with progress logging.

    This function uploads a file to the given URL using the `requests` library and the `MultipartEncoder` for handling
    large file uploads. It logs the upload progress at intervals of 5% or when the progress exceeds 95%.

    Parameters:
    url (str): The URL to which the file will be uploaded.
    file_path (str): The path to the file to be uploaded.
    json_data (dict): A dictionary containing JSON data to be included in the upload.
    api_key (str): The API key for authentication.
    file_name (str, optional): The name of the file to be uploaded. If not provided, the basename of `file_path` is used.

    Returns:
    requests.Response: The response from the server after the file upload.
    """
    def create_callback(encoder):
        """
        Creates a callback function to log the upload progress.

        Parameters:
        encoder (MultipartEncoder): The encoder handling the file upload.

        Returns:
        function: A callback function that logs the upload progress.
        """
        encoder_len = encoder.len
        last_reported_progress = -5  # Initialize to -5 so it prints at 0%

        def callback(monitor):
            nonlocal last_reported_progress  # To modify the outer variable
            progress = (monitor.bytes_read / encoder_len) * 100
            if progress >= last_reported_progress + 5 or progress > 95:
                memory_usage_msg = f", Memory usage: {psutil.Process().memory_info().rss / (1024 * 1024):.2f} MB" \
                    if progress >= last_reported_progress + 5 else ""
                logging.info(f"Upload Progress: {progress:.2f}%{memory_usage_msg}")
                last_reported_progress = progress if progress >= last_reported_progress + 5 else last_reported_progress

        return callback

    with open(file_path, 'rb') as f:
        encoder = MultipartEncoder(
            fields={'file': (file_name if file_name else os.path.basename(file_path), f, 'application/octet-stream'),
                    'jsonData': (None, json_data['jsonData'])}
        )
        callback = create_callback(encoder)
        monitor = MultipartEncoderMonitor(encoder, callback)
        logging.info(f'upload_large_file  api_key: {api_key}')
        response = requests.post(url, data=monitor, headers={"X-Dataverse-key": api_key,
                                                             'X-Authorization': app_settings.dmz_x_authorization_value,
                                                             'Content-Type': monitor.content_type})

        logging.info(f'upload_large_file response: {response.status_code}')
        if response.status_code == status.HTTP_502_BAD_GATEWAY:
            logging.error(f'ERROR 502 upload_large_file response: {response.text}')

    return response


def zip_with_progress(file_path, zip_path):
    """
    Compresses a file into a zip archive with progress logging.

    This function compresses the specified file into a zip archive, logging the progress at intervals of 10%.

    Parameters:
    file_path (str): The path to the file to be compressed.
    zip_path (str): The path where the zip archive will be created.

    Returns:
    None
    """
    # Resolve the file_path if it's a symlink
    if os.path.islink(file_path):
        real_file_path = os.readlink(file_path)
        print(f"'{file_path}' is a symlink, including the real file '{real_file_path}'.")
    else:
        real_file_path = file_path

    file_size = os.path.getsize(real_file_path)
    chunk_size = 10 * 1024 * 1024  # 10MB chunks
    processed_size = 0
    last_printed_progress = 0
    arcname = os.path.basename(file_path)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        with open(real_file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break

                # Create a temporary file to write the chunk
                temp_chunk_path = 'temp_chunk'
                with open(temp_chunk_path, 'wb') as tempf:
                    tempf.write(chunk)

                zipf.write(temp_chunk_path, arcname=arcname)

                processed_size += len(chunk)
                progress = processed_size / file_size * 100
                if progress - last_printed_progress >= 10:
                    logging.info(f"Zipping Progress of {arcname}: {progress:.0f}%")
                    last_printed_progress += 10

                # Remove the temporary file
                os.remove(temp_chunk_path)

    logging.info(f"Zipping of '{file_path}' completed.")


def delete_symlink_and_target(link_name):
    """
    Deletes a symbolic link and its target.

    This function checks if the given `link_name` is a symbolic link. If it is, it reads the target of the symbolic link.
    If the target is a directory, it removes the directory and its contents. If the target is a file, it removes the file.
    Finally, it removes the symbolic link itself and logs the deletion.

    Parameters:
    link_name (str): The name of the symbolic link to be deleted.

    Returns:
    None
    """
    if os.path.islink(link_name):
        target = os.readlink(link_name)
        if os.path.isdir(target):
            shutil.rmtree(target)
        else:
            os.remove(target)
        os.remove(link_name)
        logging.info(f'{link_name} and its target {target} DELETED successfully.')

def compress_zip_file(original_zip_path):
    """
    Compresses an existing zip file to reduce its size.

    This function reads the contents of the specified zip file, compresses them with the highest compression level,
    and writes the compressed contents to a temporary file. The temporary file is then moved to replace the original zip file.
    Progress is printed to the console.

    Parameters:
    original_zip_path (str): The path to the original zip file to be compressed.

    Returns:
    None
    """
    if not os.path.exists(original_zip_path):
        print(f"File {original_zip_path} does not exist.")
        return

    with NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        with zipfile.ZipFile(original_zip_path, 'r') as original_zip:
            total_size = sum([zinfo.file_size for zinfo in original_zip.infolist()])
            processed_size = 0

            with zipfile.ZipFile(temp_file_path, 'w', compression=zipfile.ZIP_DEFLATED,
                                 compresslevel=9) as compressed_zip:
                for file_info in original_zip.infolist():
                    with original_zip.open(file_info.filename) as file:
                        file_content = file.read()
                        compressed_zip.writestr(file_info, file_content)
                        processed_size += file_info.file_size
                        progress = (processed_size / total_size) * 100
                        print(f"Progress: {progress:.2f}%")

        shutil.move(temp_file_path, original_zip_path)
        print(f"Compression of {original_zip_path} completed successfully.")
    except Exception as e:
        os.remove(temp_file_path)
        print(f"An error occurred: {e}")


def zip_a_zipfile_with_progress(original_zip_path, new_zip_path):
    """
    Compresses an existing zip file into a new zip file with progress logging.

    This function creates a new zip file and adds the original zip file to it. The progress is logged as 100%
    since the file is added in one go.

    Parameters:
    original_zip_path (str): The path to the original zip file to be compressed.
    new_zip_path (str): The path where the new zip file will be created.

    Returns:
    None
    """
    # Get the size of the original zip file
    # original_zip_size = os.path.getsize(original_zip_path)
    arcname = original_zip_path.split('/')[-1]

    # Create a new zip file (outer zip)
    with zipfile.ZipFile(new_zip_path, 'w', zipfile.ZIP_DEFLATED) as new_zip:
        # Add the original zip file to the new zip file
        new_zip.write(original_zip_path, arcname=arcname)

        # Calculate the progress (since we're adding the file in one go, it'll jump to 100%)
        progress = 100  # In a real-world scenario, you'd calculate this based on bytes written vs total size

        # Print the progress
        logging.info(f"Zipping Progress of {arcname} : {progress}%")

def escape_invalid_json_characters(json_string: str) -> str:
    # Replace invalid control characters with their escaped equivalents
    escaped_string = re.sub(r'[\x00-\x1F\x7F]', lambda match: '\\u{:04x}'.format(ord(match.group())), json_string)
    return escaped_string

def create_s3_client():
    """
    Initializes and returns an S3 client using the provided app_settings.

    The app_settings for the S3 client are retrieved from the global `app_settings` object, which includes:
    - `S3_STORAGE_ENDPOINT`: The endpoint URL for the S3 storage.
    - `S3_ACCESS_KEY_ID`: The AWS access key ID.
    - `S3_ACCESS_KEY_SECRET`: The AWS secret access key.

    Returns:
        boto3.client: An S3 client instance configured with the specified app_settings.
    """
    return boto3.client(
        's3',
        endpoint_url=app_settings.S3_STORAGE_ENDPOINT,
        aws_access_key_id=app_settings.S3_ACCESS_KEY_ID,
        aws_secret_access_key=app_settings.S3_ACCESS_KEY_SECRET
    )


async def compare_dv_json(deposited_metadata, target_repo_name, target_creds, api_url):
    """
    Fetch JSON data from a Dataverse API and compare it with the original deposited metadata
    so that we can see whether any changes have been made to the dataset after deposit from the ACP.

    Args:
        deposited_metadata (dict): The response dictionary containing deposited metadata.
        target_repo_name (TargetApp): The target application instance.
        target_creds (list): A list of credentials for target repositories.
        api_url (str): The URL to fetch the JSON data from.

    Returns:
        dict: The differences between the deposited metadata and the fetched JSON data.
              If no differences are found, returns an empty dictionary.
    """
    # Modify the URL to point to the correct API endpoint

    # Iterate over the target credentials to find the matching repository
    for tc in target_creds:
        if tc["target-repo-name"] == target_repo_name:
            api_token = tc["credentials"]["password"]
            headers = dmz_dataverse_headers("API_KEY", api_token)
            dv_response = requests.get(api_url, headers=headers)

            if dv_response.status_code == 200:
                return Compare().check(deposited_metadata, dv_response.json())
            else:
                logging.error(f'Error occurs: status code: {dv_response.status_code} from {api_url}')

            break

# DEZE FUNCTIES STAAN ERGENS IN JE FRAMEWORK BIJ HET INTERPRETEREN VAN BIJVOORBEELD aircore4eosc-swh_dev-dataverse_demo.json

def process_metadata(pm: ProcessedMetadata, rec: str, pms: [ProcessedMetadata]):
    print(rec)
    mod = importlib.import_module(f"src.hooks.{pm.hook_name}")
    func = getattr(mod, pm.process_function)
    # Get the signature of the function
    signature = inspect.signature(func)
    num_params = len(signature.parameters)
    if num_params == 3:
        rec = func(rec, pm, pms)
    elif num_params == 2:
        rec = func(rec, pm)
    print(rec)
    return rec
    # file_path = f'{pm.dir}/{pm.name}'
    # if not os.path.exists(file_path):
    #     os.makedirs(pm.dir)
    # with open(file_path, 'w') as f:
    #     json.dump(rec, f, indent=4)

def processed_metadata_handler(steps, rec):
    for step in steps:
        rec = process_metadata(step, rec, steps)

    return rec


def fetch_from_assistant_config(endpoint: str) -> str:
    """
    Fetch data from the assistant configuration URL.

    Args:
        endpoint (str): The endpoint to append to the base assistant configuration URL.

    Returns:
        str: The JSON response from the specified endpoint.

    Raises:
        HTTPException: If the request fails with a non-200 status code.
    """
    repo_url = f'{app_settings.ASSISTANT_CONFIG_URL}/{endpoint}'
    logging.info(f'Fetching data from {repo_url}')
    response = requests.get(repo_url, headers=assistant_repo_headers)
    if response.status_code != 200:
        logging.error(f'Failed to fetch data: {repo_url}, status code: {response.status_code}')
        raise HTTPException(status_code=404, detail=f"{repo_url} not found")
    return response.json()

@handle_ps_exceptions
def retrieve_targets_configuration(assistant_config_name: str) -> str:
    """
    Retrieve the configuration for the specified assistant.

    Args:
        assistant_config_name (str): The name of the assistant configuration to retrieve.

    Returns:
        str: The JSON response containing the assistant configuration.
    """
    return fetch_from_assistant_config(f'name/{assistant_config_name}')

@handle_ps_exceptions
def retrieve_apps_list() -> str:
    """
    Retrieve the list of apps from the assistant configuration URL.

    Returns:
        str: The JSON response containing the list of apps.
    """
    return fetch_from_assistant_config('list-apps')

#TODO: Refactor this function, retrieve repo_config_name instead of only app_mae
async def get_repo_assistant(req):
    assistant_name = req.headers.get('assistant-config-name')
    if not assistant_name:
        raise HTTPException(status_code=400, detail="assistant-config-name")

    repo_config = retrieve_targets_configuration(assistant_name)
    return RepoAssistantDataModel.model_validate_json(repo_config)


async def create_asset(dataset, db_manager, target_creds):
    asset = Asset()
    asset.dataset_id = dataset.id
    asset.title = dataset.title
    asset.created_at = dataset.created_at.strftime('%Y-%m-%d %H:%M:%S')
    asset.saved_at = dataset.saved_at.strftime('%Y-%m-%d %H:%M:%S')
    asset.submitted_at = dataset.submitted_at.strftime('%Y-%m-%d %H:%M:%S') if dataset.submitted_at else ''
    asset.status = DatasetStatus.RESUBMIT if dataset.status == DatasetStatus.DRAFT_RESUBMIT else dataset.status

    # Find target repositories by dataset ID
    target_repo_recs = db_manager.find_target_repos_by_dataset_id(dataset_id=dataset.id, status_not_in=[StateVersion.DRAFT])
    # Process target repositories if the dataset is not in DRAFT release version
    for target_repo_rec in target_repo_recs:
        target_app = TargetApp()
        target_app.repo_name = target_repo_rec.name
        target_app.display_name = target_repo_rec.display_name
        target_app.deposit_status = target_repo_rec.deposit_status.name.lower()
        target_app.deposited_at = target_repo_rec.deposited_at.strftime(
            '%Y-%m-%d %H:%M:%S') if target_repo_rec.deposited_at else ''
        target_app.deposit_duration = str(target_repo_rec.deposit_duration)

        # Parse the target repository output as JSON if available
        target_service_response_json = json.loads(target_repo_rec.target_service_response) if target_repo_rec.target_service_response else {}
        target_service_response_deposited_metadata = target_service_response_json.get('deposited_metadata')
        target_repo_identifiers = target_repo_rec.deposited_identifiers
        if target_repo_identifiers:
            target_app.deposited_identifiers = json.loads(target_repo_identifiers)
            api_url = target_app.deposited_identifiers[0]['api-url']
            target_app.diff = await compare_dv_json(
                target_service_response_deposited_metadata,
                target_repo_rec.name,
                target_creds,
                api_url
            )
        else:
            target_app.output_response = {}

        asset.targets.append(target_app)
    return asset

def validate_json(str_dv_metadata):
    try:
        json.loads(str_dv_metadata)
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        # Remove invalid control characters and retry
        str_dv_metadata = re.sub(r'[\x00-\x1F\x7F]', '', str_dv_metadata)
        try:
            json.loads(str_dv_metadata)
        except json.JSONDecodeError as e:
            logging.error(f"Retry failed: {e}")
            return None
    return str_dv_metadata