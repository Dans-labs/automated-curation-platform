from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from src.acp.commons import app_settings
from src.acp.db.dbz import TargetRepo, DepositStatus, Dataset, DataFile, DatabaseManager
from src.acp.models.assistant_datamodel import Target
from src.acp.models.bridge_output_model import TargetDataModel


@dataclass(frozen=True, kw_only=True, slots=True)
class Bridge(ABC):
    """
    Abstract base class representing a bridge between the Assistant and a specific target repository.
    """

    dataset_id: str
    target: Target
    db_manager: DatabaseManager
    dataset_rec: Dataset = field(init=False)
    app_name: str
    dataset_dir: str = field(init=False)

    def __post_init__(self):
        """
        Initializes the Bridge object after its creation by setting up attributes.
        """
        object.__setattr__(self, 'dataset_rec', self.db_manager.find_dataset_by_id(self.dataset_id))
        object.__setattr__(self, 'dataset_dir', os.path.join(app_settings.DATA_TMP_BASE_DIR, self.app_name, str(self.dataset_id)))
        self.save_state()

    @classmethod
    @abstractmethod
    def job(cls) -> TargetDataModel:
        """
        Abstract method to be implemented by subclasses to perform a specific job.
        """
        ...

    def save_state(self, output_data_model: TargetDataModel = None) -> None:
        """
        Saves the state of the deposit process, updating the deposit status in the database.
        """
        deposit_status = output_data_model.deposit_status if output_data_model else DepositStatus.PROGRESS
        duration = output_data_model.response.duration if output_data_model else 0.0
        target_service_response = output_data_model.model_dump_json() if output_data_model else None
        deposited_version = output_data_model.deposited_version if output_data_model else None
        str_deposited_identifiers = (
            json.dumps([i.to_dict() for i in output_data_model.deposited_identifiers])
            if output_data_model and output_data_model.deposited_identifiers
            else None
        )

        if output_data_model:
            logging.info(
                f"Save state for dataset_id: {self.dataset_id}. Target: {self.target.repo_name}. "
                f"Deposited version: {deposited_version}. str_deposited_identifiers: {str_deposited_identifiers}"
            )

        self.db_manager.update_target_repo_deposit_status(
            TargetRepo(
                dataset_id=self.dataset_id,
                name=self.target.repo_name,
                deposit_status=deposit_status.upper(),
                target_service_response=target_service_response,
                deposit_duration=duration,
                deposited_version=deposited_version,
                deposited_identifiers=str_deposited_identifiers,
            )
        )