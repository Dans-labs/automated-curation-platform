import base64
import json
import logging
import os
from enum import StrEnum, auto
from typing import Any

from cryptography.fernet import Fernet
from sqlalchemy import delete, inspect, func, and_
from sqlalchemy.exc import IntegrityError
from sqlmodel import create_engine, Session, select
from src.acp.models.app_model import Asset, TargetApp


from datetime import datetime, timezone
from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from enum import Enum
from sqlalchemy.orm import selectinload

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


# class FilePermissions(StrEnum):
#     PUBLIC = auto()
#     PRIVATE = auto()


class DatasetStatus(str, Enum):
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
    submitted_at: Optional[datetime] = None

    metadata_content: str = Field(default="{}", nullable=False)  # Renamed to avoid conflict
    metadata_type: MetadataType = Field(default= MetadataType.JSON, nullable=False)

    status: DatasetStatus = Field(default=DatasetStatus.DRAFT)
    submission_ready: bool = Field(default=False)

    # Relationships
    target_repos: List["TargetRepo"] = Relationship(back_populates="dataset")
    data_files: List["DataFile"] = Relationship(back_populates="dataset")

    def encrypt_metadata_content(self, cipher_suite):
        if self.metadata_content:
            self.metadata_content = cipher_suite.encrypt(self.metadata_content.encode()).decode()
        else:
            raise ValueError("The 'metadata_content' attribute is None and cannot be encrypted.")

    def decrypt_metadata_content(self, cipher_suite):
        self.metadata_content = cipher_suite.decrypt(self.metadata_content.encode()).decode()


class DepositStatus(str, Enum):
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
    dataset_id: str = Field(foreign_key="dataset.id", index=True)
    name: str = Field(index=True)
    display_name: str = Field(index=True)
    configuration: str = ""
    url: str
    deposit_status: Optional[DepositStatus] = Field(default=DepositStatus.PENDING)  # Using Enum
    deposited_at: Optional[datetime]
    deposit_duration: float = 0.0 #the amount of time taken for the deposit process
    target_service_response: Optional[str]
    deposited_version: Optional[str]
    deposited_identifiers: Optional[str] = Field(default="", index=True)

    # Relationship to Parent
    dataset: Optional["Dataset"] = Relationship(back_populates="target_repos")

    def encrypt_config(self, cipher_suite):
        self.configuration = cipher_suite.encrypt(self.configuration.encode()).decode()

    def decrypt_config(self, cipher_suite):
        self.configuration = cipher_suite.decrypt(self.configuration.encode()).decode()


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
    dataset_id: str = Field(foreign_key="dataset.id", index=True)
    name: str = Field(index=True)
    path: Optional[str]
    size: Optional[int]
    mime_type: Optional[str]
    checksum: Optional[str]
    added_at: Optional[datetime]
    access_level: AccessLevel = Field(default=AccessLevel.PRIVATE)
    state: DataFileState = Field(default=DataFileState.REGISTERED)

    # Relationship to Parent
    dataset: Optional["Dataset"] = Relationship(back_populates="data_files")



class DatabaseManager:
    """
    Manages database operations including connection setup, encryption, and various CRUD operations.

    Attributes:
        cipher_suite (Fernet): The encryption suite used for encrypting and decrypting data.
    """
    cipher_suite = None

    def __init__(self, db_dialect: str, db_url: str, encryption_key: str, app_name: str = ""):
        """
        Initializes the DatabaseManager with the specified database dialect, URL, encryption key, and application name.

        Args:
            db_dialect (str): The database dialect (e.g., 'sqlite').
            db_url (str): The database URL.
            encryption_key (str): The encryption key used for data encryption.
            app_name (str): The application name to differentiate databases.
        """

        self.app_name = app_name
        if app_name.strip() != "":
            self.conn_url = f'{db_dialect}:{db_url}/acp-{app_name}.db'
        else:
            self.conn_url = f'{db_dialect}:{db_url}/acp.db'

        db_path = os.path.dirname(self.conn_url.split("///")[1])

        # Ensure the directory exists
        if not os.path.exists(db_path):
            os.makedirs(db_path)

        self.engine = create_engine(self.conn_url, pool_size=10, echo=False)
        # self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine, class_=AsyncSession)
        self.cipher_suite = Fernet(base64.urlsafe_b64encode(encryption_key.encode()))

    def create_db_and_tables(self):
        # Check if the table exists, if not create it
        if not inspect(self.engine).has_table("Dataset"):
            # Create tables here
            logging.info(f"Creating dataset table of database '{self.app_name}'")
            SQLModel.metadata.create_all(self.engine, checkfirst=True)
        else:
            print(f'TABLES ALREADY CREATED IN DATABASE: {self.app_name} which is located at {self.conn_url}')
            logging.info(f'TABLES ALREADY CREATED IN DATABASE: {self.app_name} which is located at {self.conn_url}')

    def create_initial_dataset_record(self, dataset_id: str, owner_id: str, title: Optional[str] = None) -> Dataset:
        """
        Create an initial dataset record.

        Args:
            dataset_id (str): The ID of the dataset.
            owner_id (str): The ID of the owner of the dataset.
            title (Optional[str]): The title of the dataset. Defaults to None.

        Returns:
            Dataset: The created dataset record.
        """
        dataset = Dataset(
            id=dataset_id,
            owner_id=owner_id,
            title=title,
            status=StateVersion.DRAFT,
            submission_ready=False
        )
        dataset.encrypt_metadata_content(self.cipher_suite)
        with Session(self.engine) as session:
            session.add(dataset)
            session.commit()
            session.refresh(dataset)
        return dataset

    def insert_dataset_and_target_repo(self, ds_record: Dataset, repo_records: List[TargetRepo]) -> Dataset:
        # Encrypt the metadata_content field of the Dataset
        ds_record.encrypt_metadata_content(self.cipher_suite)

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
                tr.dataset_id = ds_record.id
                session.add(tr)
            session.commit()
            session.refresh(ds_record)
        return ds_record

    def insert_datafiles(self, dataset_id, file_records: [DataFile]) -> None:
        try:
            with Session(self.engine) as session:
                for file_record in file_records:
                    file_record.dataset_id = dataset_id
                    session.add(file_record)
                    session.commit()
                    session.refresh(file_record)
        except IntegrityError as e:
            # Handle the unique constraint violation
            print(f"IntegrityError: {e.orig}")
            # Optionally, you can re-raise the exception or handle it as needed
            raise ValueError(f"------- IntegrityError: {e.orig}")
        except Exception as e:
            print(f"Exception: {e}")
            raise ValueError(f"------- Exception: {e}")


    def delete_datafile(self,dataset_id: str, filename: str) -> None:
        with Session(self.engine) as session:
            file_record = session.exec(select(DataFile).where(DataFile.dataset_id == dataset_id, DataFile.name == filename)).one_or_none()
            if file_record:
                session.delete(file_record)
                session.commit()

    def delete_all(self) -> dict:
        with Session(self.engine) as session:
            tabs = {cls.__qualname__: session.exec(delete(cls)).rowcount for cls in [DataFile, TargetRepo, Dataset]}
            session.commit()
        return tabs

    def delete_by_dataset_id(self,dataset_id: str) -> type(int):
        with Session(self.engine) as session:
            ds = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if ds:
                # Delete DataFiles and TargetRepos in a single transaction
                for model in [DataFile, TargetRepo]:
                    session.exec(delete(model).where(model.dataset_id == dataset_id))

                # Delete Dataset
                session.delete(session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none())
                session.commit()
                return 1
            return 0

    def find_draft_dataset(self, dataset: Dataset) -> Dataset:
        with Session(self.engine) as session:
            query = select(Dataset).where(
                and_(
                    Dataset.id == dataset.id,
                    Dataset.status == StateVersion.DRAFT
                )
            )
            return session.exec(query).first()

    def find_dataset_by_id(self, dataset_id: str) -> Optional[Dataset]:
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id).options(
                selectinload(Dataset.target_repos),
                selectinload(Dataset.data_files)
            )
            dataset = session.exec(statement).one_or_none()

            if dataset:
                # Ensure relationships are loaded
                session.refresh(dataset)
                for repo in dataset.target_repos:
                    session.refresh(repo)

                for files in dataset.data_files:
                    session.refresh(files)

                # Decrypt dataset metadata
                dataset.decrypt_metadata_content(self.cipher_suite)

                # Decrypt all target repo configs
                for repo in dataset.target_repos:
                    repo.decrypt_config(self.cipher_suite)

            return dataset

    def find_dataset_only_by_id(self, dataset_id: str) -> Optional[Dataset]:
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id)
            dataset = session.exec(statement).one_or_none()

            if dataset:
                # Ensure relationships are loaded
                session.refresh(dataset)
                for repo in dataset.target_repos:
                    session.refresh(repo)

                for files in dataset.data_files:
                    session.refresh(files)

                # Decrypt dataset metadata
                dataset.decrypt_metadata_content(self.cipher_suite)

                # Decrypt all target repo configs
                for repo in dataset.target_repos:
                    repo.decrypt_config(self.cipher_suite)

            return dataset
    def find_target_repo(self,dataset_id: str, target_name: str) -> TargetRepo:
        with Session(self.engine) as session:
            target_repo = session.exec(
                select(TargetRepo).where(TargetRepo.dataset_id == dataset_id, TargetRepo.name == target_name)).one_or_none()
            if target_repo:
                target_repo.decrypt_config(self.cipher_suite)
            return target_repo

    def find_dataset_and_targets(self, dataset_id: str, exclude_target=False) -> Asset:
        with Session(self.engine) as session:
            dataset = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if dataset:
                dataset.decrypt_metadata_content(self.cipher_suite)
                asset = Asset()
                asset.dataset_id = dataset.id
                # asset.release_version = dataset.release_version
                asset.title = dataset.title
                asset.md = dataset.metadata_content
                asset.created_at = dataset.created_at
                asset.saved_at = dataset.saved_at
                asset.submitted_at = dataset.submitted_at
                # asset.release_version = dataset.release_version
                # asset.version = dataset.version
                # Fetch TargetRepo objects associated with the Dataset and order them

                if not exclude_target:
                    targets_repo = session.exec(
                        select(TargetRepo).where(TargetRepo.dataset_id == dataset.id).order_by(TargetRepo.id)).all()
                    for target_repo in targets_repo:
                        target_repo.decrypt_config(self.cipher_suite)
                        target = TargetApp()
                        target.repo_name = target_repo.name
                        target.display_name = target_repo.display_name
                        target.deposit_status = target_repo.deposit_status
                        target.deposited_at = target_repo.deposited_at
                        target.deposit_duration = target_repo.deposit_duration
                        if target_repo.target_service_response is not None and target_repo.target_service_response != '':
                            target.output_response = json.loads(target_repo.target_service_response)
                        asset.targets.append(target)
                return asset
            return Asset()


    def find_dataset_and_targets_by_dataset_id(self,dataset_id: str) -> Asset:
        with Session(self.engine) as session:
            dataset = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if dataset:
                dataset.decrypt_metadata_content(self.cipher_suite)
                asset = Asset()
                asset.dataset_id = dataset.id
                asset.status = dataset.status
                asset.title = dataset.title
                asset.md = dataset.metadata_content
                asset.created_at = dataset.created_at
                asset.saved_at = dataset.saved_at
                asset.submitted_at = dataset.submitted_at
                # Fetch TargetRepo objects associated with the Dataset and order them
                targets_repo = session.exec(
                    select(TargetRepo).where(TargetRepo.dataset_id == dataset.id).order_by(TargetRepo.id)).all()
                for target_repo in targets_repo:
                    target_repo.decrypt_config(self.cipher_suite)
                    target = TargetApp()
                    target.repo_name = target_repo.name
                    target.display_name = target_repo.display_name
                    target.deposit_status = target_repo.deposit_status
                    target.deposited_at = target_repo.deposited_at
                    target.deposit_duration = target_repo.deposit_duration
                    if target_repo.target_service_response is not None and target_repo.target_service_response != '':
                        target.target_service_response = json.loads(target_repo.target_service_response)
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
                .order_by(Dataset.id)
                .limit(page_size)
                .offset((page - 1) * page_size)
            )
            results = session.exec(statement)
            result = results.all()
        return result

    def find_target_repos_by_dataset_id(self,dataset_id: str, status_not_in: [StateVersion]) -> [TargetRepo]:
        #Note: Dataset.status not in status
        with Session(self.engine) as session:
            statement = (
                select(TargetRepo)
                .join(Dataset, TargetRepo.dataset_id == Dataset.id)
                .where(Dataset.id == dataset_id,  Dataset.status.notin_(status_not_in))
                .order_by(TargetRepo.id)
            )
            results = session.exec(statement)
            target_repos = results.all()
            for target_repo in target_repos:
                target_repo.decrypt_config(self.cipher_suite)
            return target_repos

    def find_uploaded_files(self,dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.dataset_id == dataset_id, DataFile.state == DataFileState.UPLOADED)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_file_by_name(self,dataset_id: str, file_name: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.dataset_id == dataset_id, DataFile.name == file_name)
            results = session.exec(statement)
            result = results.one_or_none()
        return result

    def find_files(self,dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.dataset_id == dataset_id)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_registered_files(self,dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.dataset_id == dataset_id, DataFile.state == DataFileState.REGISTERED)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_non_registered_files(self, dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.dataset_id == dataset_id, DataFile.state != DataFileState.REGISTERED)
            results = session.exec(statement)
            result = results.all()
        return result

    # TODO: REFACTOR - CHANGE THE NAME
    def execute_l(self,dataset_id: str) -> Any:
        rst = []
        with Session(self.engine) as session:
            statement = select(DataFile.name).where(DataFile.dataset_id == dataset_id,
                                                    DataFile.state == DataFileState.UPLOADED)
            results = session.exec(statement).all()
            rst = results
        return rst

    def update_dataset(self, dataset: Dataset) -> Dataset:
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset.id)
            results = session.exec(statement)
            ds_record = results.one_or_none()
            if ds_record:
                ds_record.metadata_content = dataset.metadata_content
                ds_record.metadata_type = dataset.metadata_type
                ds_record.title = dataset.title
                ds_record.status = dataset.status
                ds_record.saved_at =  datetime.now(timezone.utc)
                ds_record.submission_ready = dataset.submission_ready
                ds_record.encrypt_metadata_content(self.cipher_suite)
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)
        return ds_record

    def set_dataset_ready_for_ingest(self,dataset_id: str, submission_ready: bool = False) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id)
            results = session.exec(statement)
            metadata_content_record = results.one_or_none()
            if metadata_content_record:
                # metadata_content_record.release_version = ReleaseVersion.PUBLISH
                metadata_content_record.submission_ready = submission_ready
                session.add(metadata_content_record)
                session.commit()
                session.refresh(metadata_content_record)

    def update_target_repo_deposit_status(self, target_repo: TargetRepo) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.dataset_id == target_repo.dataset_id,
                                                 TargetRepo.name == target_repo.name)
            results = session.exec(statement)
            target_repo_record = results.one_or_none()
            if target_repo:
                target_repo_record.deposit_status = target_repo.deposit_status
                target_repo_record.deposited_version = target_repo.deposited_version
                target_repo_record.deposited_identifiers = target_repo.deposited_identifiers
                if target_repo.target_service_response:
                    target_repo_record.target_service_response = target_repo.target_service_response
                target_repo_record.deposited_at = datetime.now(timezone.utc)
                target_repo_record.deposit_duration = target_repo.deposit_duration
                # target_repo_record.encrypt_config(self.cipher_suite)
                session.add(target_repo_record)
                session.commit()
                session.refresh(target_repo_record)



    def submitted_now(self,dataset_id: str) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id)
            results = session.exec(statement)
            metadata_content_record = results.one_or_none()
            if metadata_content_record:
                metadata_content_record.submitted_at =  datetime.now(timezone.utc)
                metadata_content_record.saved_at =  datetime.now(timezone.utc)
                session.add(metadata_content_record)
                session.commit()
                session.refresh(metadata_content_record)

    def update_file(self, df: DataFile) -> type(None):
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.dataset_id == df.dataset_id, DataFile.name == df.name)
            results = session.exec(statement)
            f_record = results.one_or_none()
            if f_record:
                f_record.added_at =  datetime.now(timezone.utc)
                f_record.path = df.path
                f_record.mime_type = df.mime_type
                f_record.size = df.size
                f_record.checksum = df.checksum
                f_record.state = df.state
                session.add(f_record)
                session.commit()
                session.refresh(f_record)

    def update_file_access_level(self, dataset_id: str, filename: str, access_level: AccessLevel) -> type(None):
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.dataset_id == dataset_id, DataFile.name == filename)
            results = session.exec(statement)
            f_record = results.one_or_none()
            if f_record:
                f_record.access_level = access_level
                session.add(f_record)
                session.commit()
                session.refresh(f_record)

    def replace_targets_record(self, dataset_id: str, target_repo_records: [TargetRepo]) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.dataset_id == dataset_id)
            results = session.exec(statement)
            trs = results.fetchall()
            for tr in trs:
                session.delete(tr)
            session.commit()
            for tr in target_repo_records:
                tr.dataset_id = dataset_id
                tr.encrypt_config(self.cipher_suite)
                session.add(tr)
            session.commit()


    def is_dataset_ready(self,dataset_id: str) -> bool:
        with Session(self.engine) as session:
            dataset_id_rec = session.exec(
                select(Dataset.id).where((Dataset.id == dataset_id) & Dataset.submission_ready &  (Dataset.status.notin_([StateVersion.DRAFT, StateVersion.DRAFT_RESUBMIT])))).one_or_none()
            return dataset_id_rec is not None

    def are_files_uploaded(self,dataset_id: str) -> bool:
        with Session(self.engine) as session:
            results = session.exec(select(DataFile).where(DataFile.dataset_id == dataset_id,
                                                          DataFile.state == DataFileState.REGISTERED)).all()

        return len(results) == 0

    def update_dataset_status(self, dataset_id: str, state: StateVersion) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id)
            results = session.exec(statement)
            ds_record = results.one_or_none()
            if ds_record:
                ds_record.status = state
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)

    def delete_generated_files(self,dataset_id: str) -> type(None):
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.dataset_id == dataset_id, DataFile.state == DataFileState.GENERATED)
            results = session.exec(statement)
            for file_record in results.all():
                session.delete(file_record)
            session.commit()

    def find_target_repo_by_indentifier(self, doi: str) -> Optional[TargetRepo]:
        print("doi:", doi)
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.deposited_identifiers.contains(doi))
            results = session.exec(statement)
            result = results.one_or_none()
        return result