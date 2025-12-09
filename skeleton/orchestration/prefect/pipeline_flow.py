{%- if values.orchestrator == "prefect" %}
"""Prefect flow for ${{values.name}} pipeline."""

from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from src.core.config import settings
from src.pipelines.example import create_pipeline


@task(name="run_pipeline", retries=2, retry_delay_seconds=300)
def run_pipeline_task(dry_run: bool = False) -> dict:
    """Execute the pipeline."""
    logger = get_run_logger()
    logger.info(f"Starting pipeline: {settings.pipeline_name}")

    pipeline = create_pipeline(dry_run=dry_run)
    metrics = pipeline.run()

    logger.info(f"Pipeline completed: {metrics.records_written} records written")

    return {
        "records_read": metrics.records_read,
        "records_written": metrics.records_written,
        "duration_seconds": metrics.duration_seconds,
        "status": metrics.status,
    }


@flow(name="${{values.name}}", description="${{values.description}}")
def pipeline_flow(dry_run: bool = False) -> dict:
    """Main pipeline flow."""
    logger = get_run_logger()
    logger.info("Pipeline flow started")

    result = run_pipeline_task(dry_run=dry_run)

    logger.info(f"Pipeline flow completed with status: {result['status']}")
    return result


# Create deployment
if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=pipeline_flow,
        name="scheduled",
        schedule=CronSchedule(cron="0 * * * *"),  # Every hour
        work_queue_name="default",
        tags=["data-pipeline", "${{values.name}}"],
    )
    deployment.apply()
{%- endif %}
