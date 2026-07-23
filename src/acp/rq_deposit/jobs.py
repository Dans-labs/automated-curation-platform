import logging
import os
from typing import Any

from rq import get_current_job

from src.acp.commons import app_settings, data, get_db_manager, inspect_bridge_plugin, retrieve_apps_list


logger = logging.getLogger(__name__)


def _update_job_meta(job, **values) -> None:
    if job is None:
        return
    job.meta.update(values)
    job.save_meta()


def _ensure_worker_logging() -> None:
    root_logger = logging.getLogger()
    log_file = os.path.abspath(app_settings.LOG_FILE)
    if any(
        isinstance(handler, logging.FileHandler)
        and getattr(handler, "baseFilename", None) == log_file
        for handler in root_logger.handlers
    ):
        return

    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(app_settings.LOG_FORMAT))
    root_logger.addHandler(file_handler)
    root_logger.setLevel(getattr(logging, str(app_settings.LOG_LEVEL).upper(), logging.INFO))
    logger.info("ACP worker attached file logging to %s", log_file)


def _initialize_worker_runtime() -> None:
    logger.info("ACP worker runtime bootstrap started")
    apps = retrieve_apps_list()
    logger.info("ACP worker runtime apps discovered: %s", apps)
    if not apps:
        raise RuntimeError("No ACP applications available for worker startup.")

    for app in apps:
        if app not in data:
            logger.info("ACP worker creating database manager for app=%s", app)
            db_manager = get_db_manager(app)
            db_manager.create_db_and_tables()
            data[app] = db_manager
        else:
            logger.info("ACP worker reusing database manager for app=%s", app)

    for filename in os.listdir(app_settings.PLUGINS_DIR):
        if filename.endswith(".py") and not filename.startswith("__"):
            plugins_path = os.path.join(app_settings.PLUGINS_DIR, filename)
            for cls_name in inspect_bridge_plugin(plugins_path):
                data.update(cls_name)
                logger.info("ACP worker registered bridge plugin: %s", cls_name)
    logger.info("ACP worker runtime bootstrap completed; data keys=%s", sorted(data.keys()))


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
    _ensure_worker_logging()
    job = get_current_job()
    logger.info(
        "ACP deposit worker picked up job: rq_job_id=%s app=%s dataset_id=%s request_source=%s",
        job.id if job else None,
        app_name,
        dataset_id,
        request_source,
    )

    _update_job_meta(
        job,
        app_name=app_name,
        dataset_id=dataset_id,
        stage="deposit-started",
        request_source=request_source,
    )

    _update_job_meta(job, stage="bootstrapping-runtime")
    _initialize_worker_runtime()

    try:
        db_manager = data[app_name]
    except KeyError as exc:
        raise RuntimeError(
            f"ACP application configuration not loaded: {app_name}"
        ) from exc

    logger.info("ACP deposit worker resolved db_manager for app=%s", app_name)
    _update_job_meta(job, stage="loading-dataset")
    dataset = db_manager.find_dataset_by_id(dataset_id)

    if dataset is None:
        raise RuntimeError(
            f"Dataset {dataset_id!r} not found for ACP application "
            f"{app_name!r}"
        )
    logger.info(
        "ACP deposit worker loaded dataset: dataset_id=%s status=%s title=%s",
        dataset_id,
        getattr(dataset, "status", None),
        getattr(dataset, "title", None),
    )

    _update_job_meta(job, stage="running-bridges")

    # Move the synchronous implementation currently used by bridge_job
    # into this callable.
    from src.acp.api.protected import run_bridge_deposit

    logger.info(
        "ACP deposit worker entering run_bridge_deposit: rq_job_id=%s dataset_id=%s",
        job.id if job else None,
        dataset_id,
    )
    try:
        result = run_bridge_deposit(
            db_manager=db_manager,
            app_name=app_name,
            dataset_id=dataset_id,
            request_source=request_source,
        )
    except Exception as exc:
        _update_job_meta(
            job,
            stage="deposit-failed",
            error=str(exc),
        )
        logger.exception(
            "ACP deposit worker failed during run_bridge_deposit: rq_job_id=%s dataset_id=%s",
            job.id if job else None,
            dataset_id,
        )
        raise
    logger.info(
        "ACP deposit worker completed run_bridge_deposit: rq_job_id=%s dataset_id=%s result=%s",
        job.id if job else None,
        dataset_id,
        result,
    )

    _update_job_meta(
        job,
        stage="deposit-completed",
        result=result,
    )

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