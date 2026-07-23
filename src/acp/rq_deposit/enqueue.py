import logging
import re
from typing import Any

from rq import Retry
from rq.exceptions import NoSuchJobError
from rq.job import Job

from src.acp.rq_deposit.connection import (
    get_deposit_queue,
    get_redis_connection,
)


def normalize_job_id(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", value).strip("-")
    return normalized or "deposit-job"


def enqueue_dataset_deposit(
    *,
    app_name: str,
    dataset_id: str,
    request_source: str,
    force_requeue: bool = False,
) -> dict[str, Any]:
    connection = get_redis_connection()
    queue = get_deposit_queue()

    job_id = normalize_job_id(
        f"deposit:{app_name}:{dataset_id}"
    )
    logging.info(
        "Preparing ACP deposit enqueue: app=%s dataset_id=%s request_source=%s queue=%s job_id=%s force_requeue=%s",
        app_name,
        dataset_id,
        request_source,
        queue.name,
        job_id,
        force_requeue,
    )

    try:
        existing_job = Job.fetch(
            job_id,
            connection=connection,
        )

        existing_status = existing_job.get_status(refresh=True)
        logging.info(
            "Found existing ACP deposit job: job_id=%s status=%s",
            existing_job.id,
            existing_status,
        )

        if force_requeue and existing_status == "scheduled":
            logging.info(
                "Force requeue requested; deleting scheduled ACP deposit job: job_id=%s",
                existing_job.id,
            )
            existing_job.delete()
        elif existing_status in {
            "queued",
            "started",
            "deferred",
            "scheduled",
            "finished",
        }:
            return {
                "job_id": existing_job.id,
                "status": existing_status,
                "duplicate": True,
                "force_requeue": force_requeue,
            }

    except NoSuchJobError:
        logging.info("No existing ACP deposit job found for job_id=%s", job_id)

    job = queue.enqueue(
        "src.acp.rq_deposit.jobs.execute_dataset_deposit",
        app_name=app_name,
        dataset_id=dataset_id,
        request_source=request_source,
        job_id=job_id,
        retry=Retry(
            max=3,
            interval=[60, 300, 900],
        ),
        job_timeout=3600,
        result_ttl=7 * 24 * 3600,
        failure_ttl=30 * 24 * 3600,
        description=(
            f"Deposit ACP dataset {dataset_id} "
            f"for application {app_name}"
        ),
        meta={
            "app_name": app_name,
            "dataset_id": dataset_id,
            "stage": "queued-for-deposit",
            "request_source": request_source,
        },
    )
    try:
        queued_count = len(queue.get_job_ids())
    except Exception:
        queued_count = None

    logging.info(
        "Queued ACP deposit job: job_id=%s status=%s queue=%s queued_count=%s",
        job.id,
        job.get_status(),
        job.origin,
        queued_count,
    )

    return {
        "job_id": job.id,
        "queue": job.origin,
        "status": job.get_status(),
        "duplicate": False,
        "force_requeue": force_requeue,
    }