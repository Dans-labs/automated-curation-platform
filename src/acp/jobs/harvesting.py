from __future__ import annotations

import json
from typing import Any

from rq import Retry, get_current_job

from src.acp.plugins.harvesters.models import (
    HarvestRequest,
)
from src.acp.plugins.harvesters.registry import (
    create_harvester_plugin,
)
from src.acp.queue.connection import get_ingest_queue
from src.acp.commons import retrieve_targets_configuration


def harvest_provider(
    *,
    batch_id: str,
    assistant_config_name: str,
    assistant_config_version: str | None = None,
    from_date: str | None = None,
    until_date: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    current_job = get_current_job()

    assistant_config = _load_assistant_config(
        assistant_config_name=assistant_config_name,
        assistant_config_version=assistant_config_version,
    )
    source_config = assistant_config["source"]
    plugin_name = source_config.get("plugin") or source_config.get("type")

    if not plugin_name:
        raise ValueError("Assistant config source must define plugin/type")

    plugin = create_harvester_plugin(
        plugin_name=plugin_name,
        config=source_config,
    )

    request = HarvestRequest.model_validate(
        {
            "batch_id": batch_id,
            "source": source_config,
            "from_date": from_date,
            "until_date": until_date,
            "limit": limit,
        }
    )

    if current_job:
        current_job.meta.update(
            {
                "batch_id": batch_id,
                "stage": "validating-source",
                "source_plugin": plugin_name,
            }
        )
        current_job.save_meta()

    plugin.validate_source()

    queue = get_ingest_queue()
    queued = 0
    deleted = 0

    for record in plugin.harvest(request):
        if record.deleted:
            deleted += 1

            # Later enqueue a deletion/tombstone workflow.
            continue

        safe_identifier = normalize_identifier(
            record.provider_identifier
        )

        queue.enqueue(
            "src.acp.jobs.ingestion.ingest_harvested_record",
            batch_id=batch_id,
            assistant_config_name=assistant_config_name,
            assistant_config_version=assistant_config_version,
            harvested_record=record.model_dump(
                mode="json"
            ),
            job_id=(
                f"ingest:{batch_id}:{safe_identifier}"
            ),
            retry=Retry(
                max=3,
                interval=[60, 300, 900],
            ),
            job_timeout=3600,
            result_ttl=7 * 24 * 3600,
            failure_ttl=30 * 24 * 3600,
            meta={
                "batch_id": batch_id,
                "provider_identifier":
                    record.provider_identifier,
                "stage": "queued-for-ingestion",
            },
        )

        queued += 1

        if current_job and queued % 100 == 0:
            current_job.meta.update(
                {
                    "stage": "harvesting",
                    "queued_records": queued,
                    "deleted_records": deleted,
                }
            )
            current_job.save_meta()

    return {
        "batch_id": batch_id,
        "assistant_config_name": assistant_config_name,
        "assistant_config_version": assistant_config_version,
        "queued_records": queued,
        "deleted_records": deleted,
    }


def _load_assistant_config(
    *,
    assistant_config_name: str,
    assistant_config_version: str | None,
) -> dict[str, Any]:
    raw_config = retrieve_targets_configuration(
        assistant_config_name,
        assistant_config_version=assistant_config_version,
    )

    try:
        config = json.loads(raw_config)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid assistant config payload for {assistant_config_name}"
        ) from exc

    if "source" not in config:
        raise ValueError(
            f"Assistant config {assistant_config_name} is missing source"
        )

    return config


def normalize_identifier(value: str) -> str:
    return "".join(
        character
        if character.isalnum()
        or character in "._-"
        else "-"
        for character in value
    )