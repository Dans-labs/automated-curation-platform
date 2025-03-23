import base64
import json
import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from enum import StrEnum, auto
from typing import List, Optional, Sequence, Any

from cryptography.fernet import Fernet
from sqlalchemy import text, delete, inspect, UniqueConstraint, func, and_
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel, Field, create_engine, Session, select

from src.acp.models.app_model import Asset, TargetApp

'''
import logging
logging.basicConfig()
logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.DEBUG)
# run sqlmodel code after this
'''

class StateVersion(StrEnum):
    DRAFT = 'DRAFT'
    PUBLISH = 'PUBLISH'
    PUBLISHED = 'PUBLISHED'
    PUBLISHING = 'PUBLISHING'
    SUBMIT = 'SUBMIT'
    SUBMITTED = 'SUBMITTED'

class MetadataType(StrEnum):
    JSON = 'application/json'
    XML = 'application/xml'
    TEXT = 'text/plain'

class DepositStatus(StrEnum):
    INITIAL = auto()
    PROGRESS = auto()
    FINISH = auto()
    REJECTED = auto()
    FAILED = auto()
    ERROR = auto()
    SUCCESS = auto()
    ACCEPTED = auto()
    FINALIZING = auto()
    SUBMITTED = auto()
    PUBLISHED = auto()
    UNDEFINED = auto()
    DEPOSITED = auto()


class DataFileWorkState(StrEnum):
    GENERATED = "GENERATED"
    UPLOADED = "UPLOADED"
    REGISTERED = "REGISTERED"


class DatasetWorkState(StrEnum):
    NOT_READY = 'not-ready'
    READY = auto()
    RELEASED = auto()


class FilePermissions(StrEnum):
    PUBLIC = auto()
    PRIVATE = auto()


# Define the Metadata model
class Dataset(SQLModel, table=True):
    __tablename__ = "dataset"

    id: int = Field(default=None, primary_key=True)
    md_id: str = Field(primary_key=True, index=True)
    title: Optional[str] = Field(nullable=True)
    owner_id: str = Field(index=True)
    created_date: datetime =  datetime.now(timezone.utc)
    saved_date: datetime =  datetime.now(timezone.utc)
    submitted_date: Optional[datetime]
    app_name: str = Field(index=True)
    md: str  # https://www.sqlite.org/fasterthanfs.html
    md_type: MetadataType = MetadataType.JSON
    md_state_version: StateVersion = StateVersion.DRAFT
    version: str = StateVersion.DRAFT.DRAFT
    state: DatasetWorkState = DatasetWorkState.NOT_READY

    def encrypt_md(self, cipher_suite):
        if self.md is not None:
            self.md = cipher_suite.encrypt(self.md.encode()).decode()
        else:
            raise ValueError("The 'md' attribute is None and cannot be encrypted.")

    def decrypt_md(self, cipher_suite):
        self.md = cipher_suite.decrypt(self.md.encode()).decode()


# Define the TargetRepo model
class TargetRepo(SQLModel, table=True):
    __tablename__ = "target_repo"
    __table_args__ = (
        UniqueConstraint("ds_id", "name", name="unique_dataset_id_target_repo_name"),
    )
    id: int = Field(default=None, primary_key=True)
    ds_id: int = Field(foreign_key="dataset.id")
    name: str = Field(index=True)
    display_name: str = Field(index=True)
    config: str
    url: str
    deposit_status: Optional[DepositStatus]
    deposit_time: Optional[datetime]
    duration: float = 0.0
    target_output: Optional[str]

    def encrypt_config(self, cipher_suite):
        self.config = cipher_suite.encrypt(self.config.encode()).decode()

    def decrypt_config(self, cipher_suite):
        self.config = cipher_suite.decrypt(self.config.encode()).decode()
    # Optional since some repo uses the same uername/password
    # e.g. dataverse username is always API_KEY, SWH API uses the same username/password for every user.
    # username: Optional[str]
    # password: Optional[str]


# Define the Files model
class DataFile(SQLModel, table=True):
    """
    Represents a data file associated with a dataset.

    Attributes:
        id (int): The primary key of the data file.
        ds_id (str): The foreign key referencing the dataset.
        name (str): The name of the data file.
        path (Optional[str]): The path to the data file.
        size (Optional[int]): The size of the data file.
        mime_type (Optional[str]): The MIME type of the data file.
        checksum_value (Optional[str]): The checksum value of the data file.
        date_added (Optional[datetime]): The date the data file was added.
        permissions (FilePermissions): The permissions of the data file. Defaults to PRIVATE.
        state (DataFileWorkState): The state of the data file. Defaults to REGISTERED.
    """
    __tablename__ = "data_file"
    id: int = Field(primary_key=True)
    ds_id: int = Field(foreign_key="dataset.id")
    name: str = Field(index=True)
    path: Optional[str]
    size: Optional[int]
    mime_type: Optional[str]
    checksum_value: Optional[str]
    date_added: Optional[datetime]
    permissions: FilePermissions = FilePermissions.PRIVATE
    state: DataFileWorkState = DataFileWorkState.REGISTERED


class DatabaseManager:
    """
    Manages database operations including connection setup, encryption, and various CRUD operations.

    Attributes:
        cipher_suite (Fernet): The encryption suite used for encrypting and decrypting data.
    """
    cipher_suite = None

    def __init__(self, db_dialect: str, db_url: str, encryption_key: str):
        """
        Initializes the DatabaseManager with the specified database dialect, URL, and encryption key.

        Args:
            db_dialect (str): The database dialect (e.g., 'sqlite').
            db_url (str): The database URL.
            encryption_key (str): The encryption key used for data encryption.
        """
        self.conn_url = f'{db_dialect}:{db_url}'
        self.engine = create_engine(self.conn_url, pool_size=10)
        # TODO: Remove db_file = self.conn_url.split("///")[1]
        # TODO use self.engine
        self.db_file = self.conn_url.split("///")[1]  # sqlite:////
        self.cipher_suite = Fernet(base64.urlsafe_b64encode(encryption_key.encode()))
    # def get_db(self):
    #     database = self.session_local()
    #     try:
    #         yield database
    #     finally:
    #         database.close()

    # def encrypt_data(self, data):
    #     return self.cipher_suite.encrypt(data.encode()).decode()
    #
    # # Function to decrypt data
    # def decrypt_data(self, data):
    #     return  self.cipher_suite.decrypt(data.encode()).decode()

    def create_db_and_tables(self):
        # checkfirst=True means if not exist create one, otherwise skip it.
        # But it doesn't work in multiple uvicorn workers
        if not inspect(self.engine).has_table("Dataset"):
            SQLModel.metadata.create_all(self.engine, checkfirst=True)
        else:
            logging.info('TABLES ALREADY CREATED')

    def insert_dataset_and_target_repo(self, ds_record: Dataset, repo_records: List[TargetRepo]) -> Dataset:
        # Encrypt the md field of the Dataset
        ds_record.encrypt_md(self.cipher_suite)

        # Encrypt the config field of each TargetRepo
        for tr in repo_records:
            tr.encrypt_config(self.cipher_suite)

        with Session(self.engine) as session:
            # Query the maximum ID from the Dataset table
            max_id = session.exec(select(func.max(Dataset.id))).one_or_none()
            new_id = (max_id or 0) + 1  # Increment max_id by 1, default to 1 if max_id is None

            # Assign the new ID to ds_record
            ds_record.id = new_id

            session.add(ds_record)
            session.commit()
            for tr in repo_records:
                tr.ds_id = ds_record.id
                session.add(tr)
            session.commit()
            session.refresh(ds_record)
        return ds_record

    def insert_datafiles(self, ds_id, file_records: [DataFile]) -> None:
        try:
            with Session(self.engine) as session:
                for file_record in file_records:
                    file_record.ds_id = ds_id
                    session.add(file_record)
                    session.commit()
                    session.refresh(file_record)
        except IntegrityError as e:
            # Handle the unique constraint violation
            print(f"IntegrityError: {e.orig}")
            # Optionally, you can re-raise the exception or handle it as needed
            raise ValueError(f"------- IntegrityError: {e.orig}")


    def delete_datafile(self, dataset_id: str, filename: str) -> None:
        with Session(self.engine) as session:
            file_record = session.exec(select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.name == filename)).one_or_none()
            if file_record:
                session.delete(file_record)
                session.commit()

    def delete_all(self) -> dict:
        with Session(self.engine) as session:
            tabs = {cls.__qualname__: session.exec(delete(cls)).rowcount for cls in [DataFile, TargetRepo, Dataset]}
            session.commit()
        return tabs

    def delete_by_dataset_id(self, dataset_id) -> type(int):
        with Session(self.engine) as session:
            ds = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if ds:
                # Delete DataFiles and TargetRepos in a single transaction
                for model in [DataFile, TargetRepo]:
                    session.exec(delete(model).where(model.ds_id == dataset_id))

                # Delete Dataset
                session.delete(session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none())
                session.commit()
                return 1
            return 0
    #Unique: md_id, version, md_state_version
    def find_dataset(self, dataset: Dataset) -> Dataset:
        with Session(self.engine) as session:
            query = select(Dataset).where(
                and_(
                    Dataset.md_id == dataset.md_id,
                    Dataset.version == dataset.version,
                    Dataset.md_state_version == dataset.md_state_version
                )
            )
            return session.exec(query).first()


    def find_draft_dataset(self, dataset: Dataset) -> Dataset:
        with Session(self.engine) as session:
            query = select(Dataset).where(
                and_(
                    Dataset.md_id == dataset.md_id,
                    Dataset.version == StateVersion.DRAFT,
                    Dataset.md_state_version == StateVersion.DRAFT
                )
            )
            return session.exec(query).first()

    def find_dataset_by_id(self, dataset_id: int) -> Dataset:
        with Session(self.engine) as session:
            query = select(Dataset).where(Dataset.id == dataset_id)
            dataset = session.exec(query).one_or_none()
            if dataset:
                dataset.decrypt_md(self.cipher_suite)
            return dataset

    def find_dataset_id_by_md_id(self, md_id: str, state_version: StateVersion = StateVersion.DRAFT) -> Dataset:
        with Session(self.engine) as session:
            dataset = session.exec(select(Dataset).where(Dataset.md_id == md_id
                                                         and Dataset.md_state_version == state_version)).one_or_none()

            return dataset
    def find_draft_dataset_id_by_md_id(self, md_id: str) -> Dataset:
        with Session(self.engine) as session:
            dataset_id = session.exec(select(Dataset.id).where(Dataset.md_id == md_id
                                                               and Dataset.md_state_version == StateVersion.DRAFT
                                                               and Dataset.version == StateVersion.DRAFT)).one_or_none()

            return dataset_id

    def find_target_repo(self, dataset_id: str, target_name: str) -> TargetRepo:
        with Session(self.engine) as session:
            target_repo = session.exec(
                select(TargetRepo).where(TargetRepo.ds_id == dataset_id, TargetRepo.name == target_name)).one_or_none()
            if target_repo:
                target_repo.decrypt_config(self.cipher_suite)
            return target_repo

    def find_dataset_and_targets_by_md_id_and_state_version(self, metadata_id: str, md_state_version: StateVersion = StateVersion.DRAFT) -> Asset:
        with Session(self.engine) as session:
            dataset = session.exec(select(Dataset).where(Dataset.md_id == metadata_id and Dataset.md_state_version == md_state_version)).one_or_none()
            if dataset:
                dataset.decrypt_md(self.cipher_suite)
                asset = Asset()
                asset.md_id = dataset.md_id
                asset.md_state_version = dataset.md_state_version
                asset.title = dataset.title
                asset.md = dataset.md
                asset.created_date = dataset.created_date
                asset.saved_date = dataset.saved_date
                asset.submitted_date = dataset.submitted_date
                asset.version = dataset.version
                # Fetch TargetRepo objects associated with the Dataset and order them
                targets_repo = session.exec(
                    select(TargetRepo).where(TargetRepo.ds_id == dataset.id).order_by(TargetRepo.id)).all()
                for target_repo in targets_repo:
                    target_repo.decrypt_config(self.cipher_suite)
                    target = TargetApp()
                    target.repo_name = target_repo.name
                    target.display_name = target_repo.display_name
                    target.deposit_status = target_repo.deposit_status
                    target.deposit_time = target_repo.deposit_time
                    target.duration = target_repo.duration
                    if target_repo.target_output is not None and target_repo.target_output != '':
                        target.output_response = json.loads(target_repo.target_output)
                    asset.targets.append(target)
                return asset
            return Asset()
    # TODO: CHECK THIS
    def find_dataset_ids_by_owner(self, owner_id: str) -> [TargetRepo]:
        with Session(self.engine) as session:
            statement = select(Dataset.id).where(Dataset.owner_id == owner_id)
            results = session.exec(statement)
            result = results.all()
        # or the compact version: session.exec(select(TargetRepo)).all()
        return result

    # TODO: CHECK THIS
    def find_datasets_by_owner(self, owner_id: str, page: int = 1, page_size: int = 10) -> [TargetRepo]:
        """
        Find datasets by owner ID with pagination.

        Args:
            owner_id (str): The ID of the owner whose datasets are to be retrieved.
            page (int, optional): The page number to retrieve. Defaults to 1.
            page_size (int, optional): The number of records per page. Defaults to 10.

        Returns:
            List[TargetRepo]: A list of TargetRepo objects for the specified owner, ordered by dataset ID.
        """
        with Session(self.engine) as session:
            statement = (
                select(Dataset)
                .where(Dataset.owner_id == owner_id)
                .order_by(Dataset.md_id)
                .limit(page_size)
                .offset((page - 1) * page_size)
            )
            results = session.exec(statement)
            result = results.all()
        return result

    def find_target_repos_by_dataset_id(self, dataset_id: int) -> [TargetRepo]:
        with Session(self.engine) as session:
            statement = (
                select(TargetRepo)
                .join(Dataset, TargetRepo.ds_id == Dataset.id)
                .where(Dataset.id == dataset_id)
                .order_by(TargetRepo.id)
            )
            results = session.exec(statement)
            target_repos = results.all()
            for target_repo in target_repos:
                target_repo.decrypt_config(self.cipher_suite)
            return target_repos

    def find_uploaded_files(self, dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.state == DataFileWorkState.UPLOADED)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_file_by_name(self, dataset_id: str, file_name: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.name == file_name)
            results = session.exec(statement)
            result = results.one_or_none()
        return result

    def find_files(self, dataset_id: int) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_registered_files(self, dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.state == DataFileWorkState.REGISTERED)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_non_generated_files(self, dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.state != DataFileWorkState.REGISTERED)
            results = session.exec(statement)
            result = results.all()
        return result

    # TODO: REFACTOR - CHANGE THE NAME
    def execute_l(self, dataset_id: str) -> Any:
        rst = []
        with Session(self.engine) as session:
            statement = select(DataFile.name).where(DataFile.ds_id == dataset_id,
                                                    DataFile.state == DataFileWorkState.UPLOADED)
            results = session.exec(statement).all()
            rst = results
        return rst

    def update_metadata(self, dataset: Dataset) -> Dataset:
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset.id)
            results = session.exec(statement)
            ds_record = results.one_or_none()
            if ds_record:
                ds_record.md = dataset.md
                ds_record.title = dataset.title
                ds_record.md_state_version = dataset.md_state_version
                ds_record.saved_date =  datetime.now(timezone.utc)
                ds_record.state = dataset.state
                ds_record.encrypt_md(self.cipher_suite)
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)
        return ds_record

    def update_dataset_md(self, id: str, md: str) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.md_id == id)
            results = session.exec(statement)
            ds_record = results.one_or_none()
            if ds_record:
                ds_record.md = md
                ds_record.encrypt_md(self.cipher_suite)
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)

    def set_dataset_ready_for_ingest(self, dataset_id: str, status: DatasetWorkState= DatasetWorkState.READY) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id)
            results = session.exec(statement)
            md_record = results.one_or_none()
            if md_record:
                # md_record.release_version = ReleaseVersion.PUBLISH
                md_record.state = status
                session.add(md_record)
                session.commit()
                session.refresh(md_record)


    def update_target_repo_deposit_status(self, target_repo: TargetRepo) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.ds_id == target_repo.ds_id,
                                                 TargetRepo.name == target_repo.name)
            results = session.exec(statement)
            target_repo_record = results.one_or_none()
            if target_repo:
                target_repo_record.deposit_status = target_repo.deposit_status
                target_repo_record.target_output = target_repo.target_output
                target_repo_record.deposit_time = datetime.now(timezone.utc)
                target_repo_record.duration = target_repo.duration
                target_repo_record.encrypt_config(self.cipher_suite)
                session.add(target_repo_record)
                session.commit()
                session.refresh(target_repo_record)


    def submitted_now(self, dataset_id: int) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id)
            results = session.exec(statement)
            md_record = results.one_or_none()
            if md_record:
                md_record.submitted_date =  datetime.now(timezone.utc)
                md_record.saved_date =  datetime.now(timezone.utc)
                session.add(md_record)
                session.commit()
                session.refresh(md_record)

    def update_file(self, df: DataFile) -> type(None):
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == df.ds_id, DataFile.name == df.name)
            results = session.exec(statement)
            f_record = results.one_or_none()
            if f_record:
                f_record.date_added =  datetime.now(timezone.utc)
                f_record.path = df.path
                f_record.mime_type = df.mime_type
                f_record.size = df.size
                f_record.checksum_value = df.checksum_value
                f_record.state = df.state
                session.add(f_record)
                session.commit()
                session.refresh(f_record)

    def update_file_permission(self, dataset_id: str, filename: str, permission: FilePermissions) -> type(None):
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.name == filename)
            results = session.exec(statement)
            f_record = results.one_or_none()
            if f_record:
                f_record.permissions = permission
                session.add(f_record)
                session.commit()
                session.refresh(f_record)

    def replace_targets_record(self, ds_id: str, target_repo_records: [TargetRepo]) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.ds_id == ds_id)
            results = session.exec(statement)
            trs = results.fetchall()
            for tr in trs:
                session.delete(tr)
            session.commit()
            for tr in target_repo_records:
                tr.ds_id = ds_id
                tr.encrypt_config(self.cipher_suite)
                session.add(tr)
            session.commit()


    def is_dataset_ready(self, ds_id: str) -> bool:
        with Session(self.engine) as session:
            dataset_id_rec = session.exec(
                select(Dataset.id).where((Dataset.id == ds_id) & (Dataset.state == DatasetWorkState.READY) &
                                            (Dataset.md_state_version == StateVersion.SUBMIT))).one_or_none()
            return dataset_id_rec is not None

    def are_files_uploaded(self, dataset_id: str) -> bool:
        with Session(self.engine) as session:
            results = session.exec(select(DataFile).where(DataFile.ds_id == dataset_id,
                                                          DataFile.state == DataFileWorkState.REGISTERED)).all()

        return len(results) == 0
