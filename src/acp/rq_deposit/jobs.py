import logging
from typing import Any

from rq import get_current_job

from src.acp.commons import data


logger = logging.getLogger(__name__)


def execute_dataset_deposit(
    *,
    app_name: str,
    dataset_id: str,
    request_source: str,
) -> dict[str, Any]:
    """
    Execute one ACP deposit.

    The worker reconstructs ACP runtime objects using app_name and dataset_id.
    Database managers must not be passed through Redis.
    """
    job = get_current_job()

    if job is not None:
        job.meta.update(
            {
                "app_name": app_name,
                "dataset_id": dataset_id,
                "stage": "deposit-started",
                "request_source": request_source,
            }
        )
        job.save_meta()

    try:
        db_manager = data[app_name]
    except KeyError as exc:
        raise RuntimeError(
            f"ACP application configuration not loaded: {app_name}"
        ) from exc

    dataset = db_manager.find_dataset_by_id(dataset_id)

    if dataset is None:
        raise RuntimeError(
            f"Dataset {dataset_id!r} not found for ACP application "
            f"{app_name!r}"
        )

    if job is not None:
        job.meta["stage"] = "running-bridges"
        job.save_meta()

    # Move the synchronous implementation currently used by bridge_job
    # into this callable.
    from src.acp.api.protected import run_bridge_deposit

    result = run_bridge_deposit(
        db_manager=db_manager,
        app_name=app_name,
        dataset_id=dataset_id,
        request_source=request_source,
    )

    if job is not None:
        job.meta.update(
            {
                "stage": "deposit-completed",
                "result": result,
            }
        )
        job.save_meta()

    logger.info(
        "ACP deposit completed",
        extra={
            "app_name": app_name,
            "dataset_id": dataset_id,
            "rq_job_id": job.id if job else None,
        },
    )

    return {
        "app_name": app_name,
        "dataset_id": dataset_id,
        "status": "completed",
        "result": result,
    }