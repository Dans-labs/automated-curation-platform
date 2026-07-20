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

    This function is called by RQ workers to process dataset deposits.
    It reconstructs its own database manager and follows the bridge process.

    Args:
        app_name: The name of the application/target
        dataset_id: The ID of the dataset to deposit
        request_source: The source of the deposit request (for logging)

    Returns:
        dict: Status information about the completed deposit
    """
    from src.acp.commons import get_db_manager
    from src.acp.api.protected import follow_bridge

    logger.info(f"Starting deposit job for dataset {dataset_id} in app {app_name}")
    logger.info(f"Request source: {request_source}")

    try:
        # Reconstruct database manager from environment
        db_manager = get_db_manager(app_name)

        # Execute the bridge process
        follow_bridge(
            db_manager=db_manager,
            app_name=app_name,
            dataset_id=dataset_id,
        )

        logger.info(f"Deposit job completed successfully for dataset {dataset_id}")

        return {
            "app_name": app_name,
            "dataset_id": dataset_id,
            "status": "completed",
        }

    except Exception as e:
        logger.error(
            f"Deposit job failed for dataset {dataset_id}: {e}",
            exc_info=True,
        )
        raise

