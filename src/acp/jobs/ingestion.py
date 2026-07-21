from __future__ import annotations

from typing import Any

from src.acp.plugins.harvesters.models import (
    HarvestedRecord,
)


def ingest_harvested_record(
    *,
    batch_id: str,
    assistant_config: dict[str, Any],
    harvested_record: dict[str, Any],
) -> dict[str, Any]:
    record = HarvestedRecord.model_validate(
        harvested_record
    )

    if record.deleted:
        return {
            "batch_id": batch_id,
            "identifier": record.provider_identifier,
            "status": "deleted",
        }

    source_metadata = record.metadata

    if source_metadata is None:
        raise ValueError(
            f"No metadata found for "
            f"{record.provider_identifier}"
        )

    # Replace this call with the existing synchronous ACP
    # dataset-ingestion function.
    result = ingest_dataset_from_plugin(
        assistant_config=assistant_config,
        provider_identifier=(
            record.provider_identifier
        ),
        metadata=source_metadata,
        metadata_type=record.metadata_prefix,
        provenance={
            "protocol": "OAI-PMH",
            "provider_identifier":
                record.provider_identifier,
            "source_datestamp": record.datestamp,
            "set_specs": record.set_specs,
            "batch_id": batch_id,
        },
    )

    return {
        "batch_id": batch_id,
        "identifier": record.provider_identifier,
        "status": "submitted",
        "result": result,
    }


def ingest_dataset_from_plugin(
    *,
    assistant_config: dict[str, Any],
    provider_identifier: str,
    metadata: dict[str, Any],
    metadata_type: str,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    """
    Adapter to ACP's existing dataset creation and processing
    implementation.

    This must call the synchronous ACP business function, not
    make an HTTP call back into the same ACP container.
    """
    raise NotImplementedError(
        "Connect this adapter to ACP dataset ingestion"
    )