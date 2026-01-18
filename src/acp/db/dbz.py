import base64
import json
import logging
import os
from datetime import datetime, timezone
from enum import StrEnum, auto, Enum
from typing import Any, List, Optional
from contextlib import contextmanager

import psycopg2
import sqlite3

from cryptography.fernet import Fernet
# Why use Fernet instead of pgcrypto?
# Less portable, as the encrypted data is tied to PostgreSQL's implementation.
#Requires the pgcrypto extension to be installed and enabled in the database.
from sqlalchemy import delete, inspect, func, ForeignKey, text, Column, BigInteger, Index
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError
from sqlmodel import (SQLModel, Field, Relationship, create_engine, Session,
                      select)
from sqlalchemy.orm import selectinload
from sqlmodel import JSON

from src.acp.models.app_model import Asset, TargetApp

def get_acp_version() -> str:
    return os.environ.get("acp_version", "unknown")  # Replace with the actual logic to retrieve the version if needed


class StateVersion(StrEnum):
    DRAFT = 'DRAFT'
    PUBLISH = 'PUBLISH'
    PUBLISHED = 'PUBLISHED'
    PUBLISHING = 'PUBLISHING'
    SUBMIT = 'SUBMIT'
    SUBMITTED = 'SUBMITTED'
    RESUBMIT = 'RESUBMIT'
    RESUBMITTED = 'RESUBMITTED'
    FAILED = 'FAILED'
    DRAFT_RESUBMIT =  "DRAFT-RESUBMIT"

class MetadataType(StrEnum):
    JSON = 'application/json'
    XML = 'application/xml'
    TEXT = 'text/plain'

class DatasetWorkState(StrEnum):
    NOT_READY = 'not-ready'
    READY = auto()
    RELEASED = auto()

class DatasetStatus(str, Enum):
    FAILED = "FAILED"
    DRAFT = "DRAFT"
    SUBMIT = "SUBMIT"
    SUBMITTED = "SUBMITTED"
    RESUBMIT = "RESUBMIT"
    RESUBMITTED = "RESUBMITTED"
    DRAFT_RESUBMIT = "DRAFT-RESUBMIT"



class Dataset(SQLModel, table=True):
    __tablename__ = "dataset"

    id: str = Field(primary_key=True, index=True)
    title: Optional[str] = Field(nullable=True)
    owner_id: str = Field(index=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    saved_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: DatasetStatus = Field(default=DatasetStatus.DRAFT)
    submitted_at: Optional[datetime] = None
    metadata_type: MetadataType = Field(default=MetadataType.JSON, nullable=False)
    submission_ready: bool = Field(default=False)
    acp_version: str = Field(default_factory=get_acp_version)  # Dynamically set default
    metadata_content: str = Field(default="{}", nullable=False)

    target_repos: List["TargetRepo"] = Relationship(back_populates="dataset")
    data_files: List["DataFile"] = Relationship(back_populates="dataset")

    def encrypt_metadata_content(self, cipher_suite):
        if not self.metadata_content:
            raise ValueError("The 'metadata_content' attribute is None and cannot be encrypted.")
        self.metadata_content = cipher_suite.encrypt(self.metadata_content.encode()).decode()

    def decrypt_metadata_content(self, cipher_suite):
        self.metadata_content = cipher_suite.decrypt(self.metadata_content.encode()).decode()


class DepositStatus(str, Enum):
    PREPARING = 'PREPARING'
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PROGRESS = "PROGRESS"
    FINISH = "FINISH"
    REJECTED = "REJECTED"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"
    ACCEPTED = "ACCEPTED"
    FINALIZING = "FINALIZING"
    SUBMITTED = "SUBMITTED"
    PUBLISHED = "PUBLISHED"
    DEPOSITED = "DEPOSITED"


class TargetRepo(SQLModel, table=True):
    __tablename__ = "target_repo"

    id: int = Field(default=None, primary_key=True)
    dataset_id: str = Field(sa_column=Column(ForeignKey("dataset.id", ondelete="CASCADE")))
    name: str = Field(index=True)
    display_name: str = Field(index=True)
    url: str
    deposit_status: Optional[DepositStatus] = Field(default=DepositStatus.PREPARING, index=True)
    deposit_status_message: Optional[str] = None
    # deposit_attempts: int = 0
    # last_deposit_attempt: Optional[datetime] = None
    deposited_at: Optional[datetime]
    deposit_duration: float = 0.0
    deposited_version: Optional[str]
    external_identifiers: Optional[dict] = Field(default_factory=dict, sa_column=Column(JSON))
    configuration: str = ""
    target_service_response: Optional[str]

    dataset: Optional["Dataset"] = Relationship(back_populates="target_repos")

    def encrypt_config(self, cipher_suite):
        self.configuration = cipher_suite.encrypt(self.configuration.encode()).decode()

    def decrypt_config(self, cipher_suite):
        self.configuration = cipher_suite.decrypt(self.configuration.encode()).decode()

    def encrypt_target_service_response(self, cipher_suite):
        if self.target_service_response:
            self.target_service_response = cipher_suite.encrypt(self.target_service_response.encode()).decode()

    def decrypt_target_service_response(self, cipher_suite):
        if self.target_service_response:
            self.target_service_response = cipher_suite.decrypt(self.target_service_response.encode()).decode()

class IngestFileStatus(str, Enum):
    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

class DataFileState(str, Enum):
    REGISTERED = "REGISTERED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    GENERATED = "GENERATED"
    GENERATED_INGESTED = "GENERATED-INGESTED"
    UPDATED = "UPDATED"
    UPLOADED = "UPLOADED"
    UPLOADED_INGESTED = "UPLOADED-INGESTED"


class AccessLevel(str, Enum):
    PRIVATE = "PRIVATE"
    PUBLIC = "PUBLIC"


class DataFile(SQLModel, table=True):
    __tablename__ = "data_file"

    id: int = Field(primary_key=True)
    dataset_id: str = Field(sa_column=Column(ForeignKey("dataset.id", ondelete="CASCADE")))
    name: str = Field(index=True)
    path: Optional[str]  = Field(index=True)
    size: Optional[int] = Field(sa_column=Column(BigInteger))
    mime_type: Optional[str]
    checksum: Optional[str]
    added_at: Optional[datetime]
    access_level: AccessLevel = Field(default=AccessLevel.PRIVATE)
    state: DataFileState = Field(default=DataFileState.REGISTERED)
    ingest_status: IngestFileStatus = Field(default=IngestFileStatus.PENDING)
    ingest_status_message: Optional[str] = None
    ingested_at: Optional[datetime]
    ingest_duration: Optional[float]

    dataset: Optional["Dataset"] = Relationship(back_populates="data_files")

class DatasetBackup(SQLModel, table=True):
    __tablename__ = "dataset_backups"

    backup_id: int = Field(default=None, primary_key=True)
    dataset_id: str = Field(sa_column=Column(ForeignKey("dataset.id", ondelete="SET NULL"), nullable=True))
    backup_timestamp: datetime = Field(nullable=False)
    table_name: str = Field(nullable=False)
    record_data: str = Field(nullable=False)


# Define the index separately
Index("ix_dataset_backups_dataset_id", DatasetBackup.__table__.c.dataset_id)

class DatabaseManager:
    def __init__(self, db_dialect: str, db_url: str, encryption_key: str, app_name: str = ""):
        self.db_dialect = db_dialect
        self.app_name = app_name
        db_name = f"acp_{app_name}" if app_name.strip() else "acp"
        if db_dialect == "sqlite":
            if db_url == ":memory:":
                self.conn_url = f'{db_dialect}:///{db_url}'
            else:
                self.conn_url = f'{db_dialect}:{db_url}/{db_name}.db'
                db_path = os.path.dirname(self.conn_url.split("///")[1])
                os.makedirs(db_path, exist_ok=True)
        elif self.db_dialect == "postgresql+psycopg2":
            # Correctly parse the db_url and construct the connection URL
            db_url_parts = db_url.split("@")
            credentials, host_port = db_url_parts[0], db_url_parts[1]
            username, password = credentials.split(":")
            host, port = host_port.split(":")

            self.conn_url = URL.create(
                drivername=self.db_dialect,  # e.g., 'postgresql+psycopg2'
                username=username,
                password=password,
                host=host,
                port=port,
                database=db_name
            )

        self.engine = create_engine(self.conn_url, pool_size=10, echo=False)
        self.cipher_suite = Fernet(base64.urlsafe_b64encode(encryption_key.encode()))
    def __sqlite_create_db_and_tables(self):
        if not inspect(self.engine).has_table("Dataset"):
            msg = f"Creating dataset table of database '{self.app_name}'"
            logging.info(msg)
            print(msg)
            SQLModel.metadata.create_all(self.engine, checkfirst=True)
        else:
            msg = f'TABLES ALREADY CREATED IN DATABASE: {self.app_name} at {self.conn_url}'
            print(msg)
            logging.info(msg)

    @contextmanager
    def __db_session(self):
        session = Session(self.engine)
        try:
            yield session
        finally:
            session.close()
    def create_db_and_tables(self):
        if self.db_dialect == "sqlite":
            self.__sqlite_create_db_and_tables()
        elif self.db_dialect == "postgresql+psycopg2":
            # Extract the database name from the connection URL
            db_name = self.conn_url.database

            # Create a temporary engine with AUTOCOMMIT isolation level
            default_engine = create_engine(self.conn_url.set(database="postgres"), isolation_level="AUTOCOMMIT")

            with default_engine.connect() as connection:
                # Check if the database exists
                result = connection.execute(text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                                            {"db_name": db_name})
                if not result.fetchone():
                    msg = f"Database '{db_name}' does not exist. Creating it..."
                    print(msg)
                    logging.info(msg)
                    connection.execute(text(f"CREATE DATABASE {db_name}"))
                else:
                    msg = f"Database '{db_name}' already exists."
                    print(msg)
                    logging.info(msg)
            # Create tables in the target database
            logging.info(f"Creating tables in database '{self.app_name}' if they do not already exist.")
            SQLModel.metadata.create_all(self.engine, checkfirst=True)
        else:
            msg = f"Unsupported database dialect '{self.db_dialect}'"
            logging.error(msg)
            print(msg)
            raise ValueError(msg)

    def create_initial_dataset_record(self, dataset_id: str, owner_id: str, title: Optional[str] = None) -> Dataset:
        dataset = Dataset(
            id=dataset_id,
            owner_id=owner_id,
            title=title,
            status=StateVersion.DRAFT,
            submission_ready=False
        )
        dataset.encrypt_metadata_content(self.cipher_suite)
        with self.__db_session() as session:
            session.add(dataset)
            session.commit()
            session.refresh(dataset)
        return dataset

    def insert_dataset_and_target_repo(self, ds_record: Dataset, repo_records: List[TargetRepo]) -> Dataset:
        # Encrypt dataset metadata and target repo configurations
        ds_record.encrypt_metadata_content(self.cipher_suite)
        for tr in repo_records:
            tr.encrypt_config(self.cipher_suite)
            tr.encrypt_target_service_response(self.cipher_suite)

        with self.__db_session() as session:
            # Assign a new ID to the dataset
            ds_record.id = (session.exec(select(func.max(Dataset.id))).one_or_none() or 0) + 1

            # Add dataset and target repos to the database
            session.add(ds_record)
            session.commit()
            for tr in repo_records:
                tr.dataset_id = ds_record.id
                session.add(tr)
            session.commit()

            # Refresh and return the dataset record
            session.refresh(ds_record)
        return ds_record

    def insert_datafiles(self, dataset_id, file_records: List[DataFile]) -> None:
        try:
            with self.__db_session() as session:
                for file_record in file_records:
                    file_record.dataset_id = dataset_id
                    session.add(file_record)
                    session.commit()
        except IntegrityError as e:
            raise ValueError(f"IntegrityError: {e.orig}")
        except Exception as e:
            raise ValueError(f"Exception: {e}")

    def delete_datafile(self, dataset_id: str, filename: str) -> None:
        with self.__db_session() as session:
            file_record = session.exec(
                select(DataFile).where(
                    DataFile.dataset_id == dataset_id,
                    DataFile.name == filename
                )
            ).one_or_none()
            if file_record:
                session.delete(file_record)
                session.commit()

    def delete_by_dataset_id(self, dataset_id: str) -> int:
        with self.__db_session() as session:
            dataset = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if not dataset:
                return 0

            # Delete related records and the dataset
            for model in [DataFile, TargetRepo]:
                session.exec(delete(model).where(model.dataset_id == dataset_id))
            session.delete(dataset)
            session.commit()
            return 1

    def find_draft_dataset(self, dataset: Dataset) -> Dataset:
        with self.__db_session() as session:
            return session.exec(
                select(Dataset).where(
                    Dataset.id == dataset.id,
                    Dataset.status == StateVersion.DRAFT
                )
            ).first()

    def _get_dataset_with_relationships(self, dataset_id: str) -> Optional[Dataset]:
        with self.__db_session() as session:
            dataset = session.exec(
                select(Dataset)
                .where(Dataset.id == dataset_id)
                .options(
                    selectinload(Dataset.target_repos),
                    selectinload(Dataset.data_files)
                )
            ).one_or_none()

            if dataset:
                session.refresh(dataset)
                for repo in dataset.target_repos:
                    repo.decrypt_config(self.cipher_suite)
                    repo.decrypt_target_service_response(self.cipher_suite)
                for file in dataset.data_files:
                    session.refresh(file)
                dataset.decrypt_metadata_content(self.cipher_suite)

            return dataset

    def find_dataset_by_id(self, dataset_id: str) -> Optional[Dataset]:
        return self._get_dataset_with_relationships(dataset_id)

    def find_dataset_only_by_id(self, dataset_id: str) -> Optional[Dataset]:
        return self._get_dataset_with_relationships(dataset_id)

    def find_target_repo(self, dataset_id: str, target_name: str) -> TargetRepo:
        with self.__db_session() as session:
            target_repo = session.exec(
                select(TargetRepo).where(
                    TargetRepo.dataset_id == dataset_id,
                    TargetRepo.name == target_name)
            ).one_or_none()
            if target_repo:
                target_repo.decrypt_config(self.cipher_suite)
                target_repo.decrypt_target_service_response(self.cipher_suite)
            return target_repo

    def _create_asset_from_dataset(self, dataset: Dataset, include_targets: bool = True) -> Asset:
        asset = Asset()
        asset.dataset_id = dataset.id
        asset.status = dataset.status
        asset.title = dataset.title
        asset.md = dataset.metadata_content
        asset.created_at = dataset.created_at
        asset.saved_at = dataset.saved_at
        asset.submitted_at = dataset.submitted_at

        if include_targets:
            with self.__db_session() as session:
                targets_repo = session.exec(
                    select(TargetRepo)
                    .where(TargetRepo.dataset_id == dataset.id)
                    .order_by(TargetRepo.id)
                ).all()

                for target_repo in targets_repo:
                    target_repo.decrypt_config(self.cipher_suite)
                    target_repo.decrypt_target_service_response(self.cipher_suite)
                    target = TargetApp()
                    target.repo_name = target_repo.name
                    target.display_name = target_repo.display_name
                    target.deposit_status = target_repo.deposit_status
                    target.deposited_at = target_repo.deposited_at
                    target.deposit_duration = target_repo.deposit_duration
                    if target_repo.target_service_response:
                        target.target_service_response = json.loads(target_repo.decrypt_target_service_response(self.cipher_suite))
                    asset.targets.append(target)
        return asset

    def find_dataset_and_targets(self, dataset_id: str, exclude_target=False) -> Asset:
        with self.__db_session() as session:
            dataset = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if dataset:
                dataset.decrypt_metadata_content(self.cipher_suite)
                return self._create_asset_from_dataset(dataset, not exclude_target)
            return Asset()

    def find_dataset_and_targets_by_dataset_id(self, dataset_id: str) -> Asset:
        return self.find_dataset_and_targets(dataset_id, exclude_target=False)

    def find_dataset_ids_by_owner(self, owner_id: str) -> List[TargetRepo]:
        with self.__db_session() as session:
            return session.exec(select(Dataset.id).where(Dataset.owner_id == owner_id)).all()

    def find_datasets_by_owner(
            self,
            owner_id: str,
            page: int = 1,
            page_size: int = 10,
            sort_by: List[tuple] = [("saved_at", "ASC")]
    ) -> List[Dataset]:
        with self.__db_session() as session:
            # Dynamically get the columns and sort order
            sort_columns = [
                getattr(Dataset, col).asc() if order.upper() == "ASC" else getattr(Dataset, col).desc()
                for col, order in sort_by
            ]
            return session.exec(
                select(Dataset)
                .where(Dataset.owner_id == owner_id)
                .order_by(*sort_columns)  # Unpack the list of sort columns
                .limit(page_size)
                .offset((page - 1) * page_size)
            ).all()

    def find_target_repos_by_dataset_id(self, dataset_id: str, status_not_in: List[StateVersion]) -> List[TargetRepo]:
        with self.__db_session() as session:
            target_repos = session.exec(
                select(TargetRepo)
                .join(Dataset, TargetRepo.dataset_id == Dataset.id)
                .where(Dataset.id == dataset_id, Dataset.status.notin_(status_not_in))
                .order_by(TargetRepo.id)
            ).all()
            for target_repo in target_repos:
                target_repo.decrypt_config(self.cipher_suite)
                target_repo.decrypt_target_service_response(self.cipher_suite)
            return target_repos

    def find_files_by_state(self, dataset_id: str, state: Optional[DataFileState] = None) -> List[DataFile]:
        with self.__db_session() as session:
            query = select(DataFile).where(DataFile.dataset_id == dataset_id)
            if state is not None:
                if isinstance(state, list):
                    query = query.where(DataFile.state.in_(state))
                else:
                    query = query.where(DataFile.state == state)
            return session.exec(query).all()

    def find_uploaded_files(self, dataset_id: str) -> List[DataFile]:
        return self.find_files_by_state(dataset_id, DataFileState.UPLOADED)

    def find_file_by_name(self, dataset_id: str, file_name: str) -> DataFile:
        with self.__db_session() as session:
            return session.exec(
                select(DataFile)
                .where(DataFile.dataset_id == dataset_id, DataFile.name == file_name)
            ).one_or_none()

    def find_files(self, dataset_id: str) -> List[DataFile]:
        return self.find_files_by_state(dataset_id)

    def find_registered_files(self, dataset_id: str) -> List[DataFile]:
        return self.find_files_by_state(dataset_id, DataFileState.REGISTERED)

    def find_non_registered_files(self, dataset_id: str) -> List[DataFile]:
        with self.__db_session() as session:
            return session.exec(
                select(DataFile)
                .where(DataFile.dataset_id == dataset_id, DataFile.state != DataFileState.REGISTERED)
            ).all()

    def execute_l(self, dataset_id: str) -> List[str]:
        with self.__db_session() as session:
            return session.exec(
                select(DataFile.name)
                .where(DataFile.dataset_id == dataset_id, DataFile.state == DataFileState.UPLOADED)
            ).all()

    def update_dataset_metadata_content(self, dataset_id: str, metadata_content: str) -> Dataset:
        with self.__db_session() as session:
            ds_record = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if ds_record:
                ds_record.metadata_content = metadata_content
                ds_record.encrypt_metadata_content(self.cipher_suite)
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)
                return ds_record
        return None
    def update_dataset(self, dataset: Dataset) -> Dataset:
        with self.__db_session() as session:
            ds_record = session.exec(select(Dataset).where(Dataset.id == dataset.id)).one_or_none()
            if ds_record:
                ds_record.metadata_content = dataset.metadata_content
                ds_record.metadata_type = dataset.metadata_type
                ds_record.title = dataset.title
                ds_record.status = dataset.status
                ds_record.saved_at = datetime.now(timezone.utc)
                ds_record.submission_ready = dataset.submission_ready
                ds_record.encrypt_metadata_content(self.cipher_suite)
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)
                return ds_record
        return None

    def set_dataset_ready_for_ingest(self, dataset_id: str, submission_ready: bool = False) -> None:
        with self.__db_session() as session:
            dataset = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if dataset:
                dataset.submission_ready = submission_ready
                session.add(dataset)
                session.commit()
                session.refresh(dataset)

    def update_target_repo_deposit_status(self, target_repo: TargetRepo) -> None:
        with self.__db_session() as session:
            target_repo_rec = session.exec(
                select(TargetRepo).where(
                    TargetRepo.dataset_id == target_repo.dataset_id,
                    TargetRepo.name == target_repo.name
                )
            ).one_or_none()

            if target_repo_rec:
                target_repo_rec.deposit_status = target_repo.deposit_status
                target_repo_rec.deposited_version = target_repo.deposited_version
                if target_repo.external_identifiers:
                    target_repo_rec.external_identifiers = target_repo.external_identifiers
                if target_repo.target_service_response:
                    target_repo_rec.target_service_response = target_repo.target_service_response
                    target_repo_rec.encrypt_target_service_response(self.cipher_suite)
                target_repo_rec.deposited_at = datetime.now(timezone.utc)
                target_repo_rec.deposit_duration = target_repo.deposit_duration

                session.add(target_repo_rec)
                session.commit()

    def submitted_now(self, dataset_id: str) -> None:
        with self.__db_session() as session:
            dataset = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if dataset:
                dataset.submitted_at = datetime.now(timezone.utc)
                dataset.saved_at = datetime.now(timezone.utc)
                session.add(dataset)
                session.commit()
                session.refresh(dataset)

    def update_file(self, df: DataFile) -> None:
        with self.__db_session() as session:
            f_record = session.exec(
                select(DataFile)
                .where(DataFile.dataset_id == df.dataset_id, DataFile.name == df.name)
            ).one_or_none()
            if f_record:
                f_record.added_at = datetime.now(timezone.utc)
                f_record.path = df.path
                f_record.mime_type = df.mime_type
                f_record.size = df.size
                f_record.checksum = df.checksum
                f_record.state = df.state
                if df.ingest_status:
                    f_record.ingest_status = df.ingest_status
                if df.ingested_at:
                    f_record.ingested_at = df.ingested_at
                if df.ingest_duration:
                    f_record.ingest_duration = df.ingest_duration
                if df.ingest_status_message:
                    f_record.ingest_status_message = df.ingest_status_message
                session.add(f_record)
                session.commit()
                session.refresh(f_record)

    def update_file_access_level(self, dataset_id: str, filename: str, access_level: AccessLevel) -> None:
        with self.__db_session() as session:
            f_record = session.exec(
                select(DataFile)
                .where(DataFile.dataset_id == dataset_id, DataFile.name == filename)
            ).one_or_none()
            if f_record:
                f_record.access_level = access_level
                session.add(f_record)
                session.commit()
                session.refresh(f_record)

    def replace_targets_record(self, dataset_id: str, target_repo_records: List[TargetRepo]) -> None:
        with self.__db_session() as session:
            session.exec(delete(TargetRepo).where(TargetRepo.dataset_id == dataset_id))
            session.commit()
            for tr in target_repo_records:
                tr.dataset_id = dataset_id
                tr.encrypt_config(self.cipher_suite)
                tr.encrypt_target_service_response(self.cipher_suite)
                session.add(tr)
            session.commit()

    def is_dataset_ready(self, dataset_id: str) -> bool:
        with self.__db_session() as session:
            return session.exec(
                select(Dataset.id).where(
                    (Dataset.id == dataset_id) &
                    Dataset.submission_ready &
                    (Dataset.status.notin_([StateVersion.DRAFT, StateVersion.DRAFT_RESUBMIT]))
                )).one_or_none() is not None

    def are_files_uploaded(self, dataset_id: str) -> bool:
        return len(self.find_registered_files(dataset_id)) == 0

    def update_dataset_status(self, dataset_id: str, state: StateVersion) -> None:
        with self.__db_session() as session:
            ds_record = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if ds_record:
                ds_record.status = state
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)

    def delete_generated_files(self, dataset_id: str) -> None:
        with self.__db_session() as session:
            session.exec(
                delete(DataFile)
                .where(DataFile.dataset_id == dataset_id, DataFile.state == DataFileState.GENERATED)
            )
            session.commit()

    def find_target_repo_by_indentifier(self, doi: str) -> Optional[TargetRepo]:
        with self.__db_session() as session:
            return session.exec(
                select(TargetRepo).where(TargetRepo.external_identifiers.contains(doi))
            ).one_or_none()

    from sqlalchemy import text

    def backup_dataset_by_id(self, dataset_id):
        """
        Backup all rows related to a specific dataset ID to a backup table.

        Args:
            dataset_id (str): The dataset ID to backup
        """
        backup_time = datetime.now()
        with self.__db_session() as session:
            try:
                # Check if dataset_id already exists in dataset_backups
                existing_backup = session.exec(
                    select(DatasetBackup).where(DatasetBackup.dataset_id == dataset_id)
                ).first()

                if existing_backup:
                    logging.info(f"Backup for dataset {dataset_id} already exists. Skipping backup.")
                    return

                # Backup dataset table
                statement = select(Dataset).where(Dataset.id == dataset_id)
                dataset_row = session.exec(statement).one_or_none()

                if not dataset_row:
                    raise ValueError(f"No dataset found with ID: {dataset_id}")

                # Convert row to JSON string
                record_data = dataset_row.model_dump_json()
                backup_record = DatasetBackup(
                    dataset_id=dataset_id,
                    backup_timestamp=backup_time,
                    table_name='dataset',
                    record_data=record_data
                )
                session.add(backup_record)
                session.commit()

                # Backup target_repo records
                statement = select(TargetRepo).where(TargetRepo.dataset_id == dataset_id)
                target_repo_rows = session.exec(statement).all()

                for row in target_repo_rows:
                    record_data = row.model_dump_json()
                    backup_record = DatasetBackup(
                        dataset_id=dataset_id,
                        backup_timestamp=backup_time,
                        table_name='target_repo',
                        record_data=record_data  # Serialize dictionary to JSON string
                    )
                    session.add(backup_record)

                session.commit()

                # Backup data_file records
                statement = select(DataFile).where(DataFile.dataset_id == dataset_id)
                data_file_rows = session.exec(statement).all()

                for row in data_file_rows:
                    record_data =row.model_dump_json()
                    backup_record = DatasetBackup(
                        dataset_id=dataset_id,
                        backup_timestamp=backup_time,
                        table_name='data_file',
                        record_data=record_data  # Serialize dictionary to JSON string
                    )
                    session.add(backup_record)

                session.commit()
                print(f"Backup successfully created in database for dataset {dataset_id}")

            except Exception as e:
                session.rollback()
                raise e

    def restore_from_backup(self, dataset_id):
        """
        Restore a dataset from the backup table and clean up the backup records

        Args:
            dataset_id (str): The dataset ID to restore
        """

        with self.__db_session() as session:
            try:
                # Get the backup records and identify which ones we're working with
                statement = (
                    select(DatasetBackup)
                    .where(DatasetBackup.dataset_id == dataset_id)
                    .where(
                        DatasetBackup.backup_timestamp == (
                            select(func.max(DatasetBackup.backup_timestamp))
                            .where(DatasetBackup.dataset_id == dataset_id).limit(1).scalar_subquery()
                        )
                    )
                )
                backup_records = session.exec(statement).all()

                # First collect all backup records before modifying anything
                if not backup_records:
                    raise ValueError(f"No backup records found for dataset {dataset_id}")

                # Delete existing records in target tables
                session.exec(delete(DataFile).where(DataFile.dataset_id == dataset_id))
                session.exec(delete(TargetRepo).where(TargetRepo.dataset_id == dataset_id))
                session.exec(delete(Dataset).where(Dataset.id == dataset_id))
                session.commit()

                # Restore records from backup
                restored_counts = {'dataset': 0, 'target_repo': 0, 'data_file': 0}
                for backup_record in backup_records:
                    table_name = backup_record.table_name
                    try:
                        record_data = json.loads(backup_record.record_data)
                        # Convert datetime fields
                        if table_name == 'dataset':
                            record_data["created_at"] = datetime.fromisoformat(record_data["created_at"])
                            record_data["saved_at"] = datetime.fromisoformat(record_data["saved_at"])
                            record_data["submitted_at"] = datetime.fromisoformat(record_data["submitted_at"])
                        elif table_name == 'target_repo':
                            record_data["deposited_at"] = datetime.fromisoformat(record_data["deposited_at"])
                        else:
                            record_data["added_at"] = datetime.fromisoformat(record_data["added_at"])

                    except Exception as e:
                        print(f"Error parsing record data for {table_name}: {e}")
                        continue

                    # Map table names to SQLModel classes
                    table_mapping = {
                        'dataset': Dataset,
                        'target_repo': TargetRepo,
                        'data_file': DataFile
                    }

                    model_class = table_mapping.get(table_name)
                    if not model_class:
                        print(f"Unknown table name: {table_name}")
                        continue

                    # Create an instance of the model class
                    try:
                        record_instance = model_class(**record_data)
                        session.add(record_instance)
                        session.commit()
                        restored_counts[table_name] += 1
                    except IntegrityError as e:
                        print(f"Skipping duplicate record for {table_name}: {e}")
                        session.rollback()
                        continue

                # Verify we restored required records (dataset and target_repo are required)
                if restored_counts['dataset'] == 0:
                    raise ValueError("Failed to restore the main dataset record")
                if restored_counts['target_repo'] == 0:
                    print("Warning: No target_repo records restored")  # Change to raise ValueError if required
                # Only if restoration succeeded do we delete the backups
                self.delete_dataset_backups_by_dataset_id(dataset_id)
                logging.info(f"Successfully restored dataset {dataset_id} and cleaned up backup records")
                logging.info(f"Records restored: Dataset: {restored_counts['dataset']}, "
                      f"Target Repo: {restored_counts['target_repo']}, "
                      f"Data Files: {restored_counts['data_file']}")

            except Exception as e:
                session.rollback()
                logging.info(f"Restoration failed - all changes reverted: {e}")
                raise e

    def delete_dataset_backups_by_dataset_id(self, dataset_id):
        with self.__db_session() as session:
            try:
                statement = (
                    delete(DatasetBackup)
                    .where(DatasetBackup.dataset_id == dataset_id)
                    .where(
                        DatasetBackup.backup_timestamp == (
                            select(func.max(DatasetBackup.backup_timestamp))
                            .where(DatasetBackup.dataset_id == dataset_id)
                        ).limit(1).scalar_subquery()
                    )
                )
                session.exec(statement)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e