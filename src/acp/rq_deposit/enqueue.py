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
    return re.sub(r"[^A-Za-z0-9_.:-]+", "-", value)


def enqueue_dataset_deposit(
    *,
    app_name: str,
    dataset_id: str,
    request_source: str,
) -> dict[str, Any]:
    connection = get_redis_connection()
    queue = get_deposit_queue()

    job_id = normalize_job_id(
        f"deposit:{app_name}:{dataset_id}"
    )

    try:
        existing_job = Job.fetch(
            job_id,
            connection=connection,
        )

        existing_status = existing_job.get_status(refresh=True)

        if existing_status in {
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
            }

    except NoSuchJobError:
        pass

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

    return {
        "job_id": job.id,
        "queue": job.origin,
        "status": job.get_status(),
        "duplicate": False,
    }