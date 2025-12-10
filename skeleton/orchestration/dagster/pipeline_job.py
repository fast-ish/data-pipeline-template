{%- if values.orchestrator == "dagster" %}
"""Dagster job for ${{values.name}} pipeline."""

from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from src.core.config import settings
from src.core.logging import get_logger
from src.pipelines.example import create_pipeline

logger = get_logger(__name__)


@asset(
    group_name="${{values.name}}",
    description="${{values.description}}",
)
def pipeline_data(context: AssetExecutionContext) -> dict:
    """Run the data pipeline and return metrics."""
    context.log.info(f"Starting pipeline: {settings.pipeline_name}")

    pipeline = create_pipeline()
    metrics = pipeline.run()

    context.log.info(f"Pipeline completed: {metrics.records_written} records written")

    return {
        "records_read": metrics.records_read,
        "records_written": metrics.records_written,
        "duration_seconds": metrics.duration_seconds,
        "status": metrics.status,
    }


# Define the job
pipeline_job = define_asset_job(
    name="${{values.name}}_job",
    selection=["pipeline_data"],
    description="Run the ${{values.name}} pipeline",
)

# Define schedule
pipeline_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 * * * *",  # Every hour
    default_status=None,  # Start in stopped state
)

# Export definitions
defs = Definitions(
    assets=[pipeline_data],
    jobs=[pipeline_job],
    schedules=[pipeline_schedule],
)
{%- endif %}
