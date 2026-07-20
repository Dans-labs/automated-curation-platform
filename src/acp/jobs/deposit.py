"""
ACP deposit job handler for RQ workers.
"""
import logging

logger = logging.getLogger(__name__)


def execute_dataset_deposit(
    app_name: str,
    dataset_id: str,
    request_source: str,
) -> dict[str, str]:
    """
    Execute dataset deposit as a background RQ job.

    This is the entry point called by the RQ worker process.
    It reconstructs its own database manager (never passed via queue)
    and delegates to run_bridge_deposit — the pure synchronous deposit logic.

    Args:
        app_name: The name of the application/target
        dataset_id: The ID of the dataset to deposit
        request_source: The source of the deposit request (for logging)

    Returns:
        dict: Status information about the completed deposit
    """
    from src.acp.commons import get_db_manager
    from src.acp.api.protected import run_bridge_deposit

    logger.info(
        f"Worker: starting deposit job for dataset {dataset_id} (app={app_name}, source={request_source})"
    )

    try:
        # Reconstruct database manager inside the worker — never serialised through Redis
        db_manager = get_db_manager(app_name)

        result = run_bridge_deposit(
            db_manager=db_manager,
            app_name=app_name,
            dataset_id=dataset_id,
            request_source=request_source,
        )

        logger.info(f"Worker: deposit job completed for dataset {dataset_id}")
        return result

    except Exception as e:
        logger.error(
            f"Worker: deposit job failed for dataset {dataset_id}: {e}",
            exc_info=True,
        )
        raise
