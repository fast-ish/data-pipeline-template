"""Pipeline CLI."""

import click
from ddtrace import patch_all

# Patch all libraries for tracing
patch_all()

from src.core.config import settings
from src.core.logging import get_logger
from src.pipeline import Pipeline

logger = get_logger(__name__)


@click.group()
@click.version_option(version=settings.version)
def cli() -> None:
    """${{values.name}} data pipeline."""
    pass


@cli.command()
@click.option("--dry-run", is_flag=True, help="Validate without loading data")
def run(dry_run: bool) -> None:
    """Run the pipeline."""
    logger.info("cli_run_started", dry_run=dry_run)

    # Import your configured pipeline here
    # from src.pipelines.example import create_pipeline
    # pipeline = create_pipeline(dry_run=dry_run)

    # Example placeholder
    pipeline = Pipeline(name=settings.pipeline_name)

    try:
        metrics = pipeline.run()
        click.echo(f"Pipeline completed: {metrics.records_written} records written")
    except Exception as e:
        click.echo(f"Pipeline failed: {e}", err=True)
        raise click.Abort()


@cli.command()
def validate() -> None:
    """Validate pipeline configuration."""
    click.echo("Configuration:")
    click.echo(f"  Pipeline: {settings.pipeline_name}")
    click.echo(f"  Environment: {settings.environment}")
    click.echo(f"  Version: {settings.version}")

    # Validate connections
    click.echo("\nConnections:")
    {%- if values.sourceType == "s3" or values.sinkType == "s3" %}
    click.echo(f"  S3 Bucket: {settings.s3_bucket}")
    {%- endif %}
    {%- if values.sourceType == "postgres" or values.sinkType == "postgres" %}
    click.echo(f"  PostgreSQL: {settings.postgres_host}:{settings.postgres_port}")
    {%- endif %}
    {%- if values.sourceType == "redshift" or values.sinkType == "redshift" %}
    click.echo(f"  Redshift: {settings.redshift_host}:{settings.redshift_port}")
    {%- endif %}
    {%- if values.sinkType == "snowflake" %}
    click.echo(f"  Snowflake: {settings.snowflake_account}")
    {%- endif %}
    {%- if values.sinkType == "bigquery" %}
    click.echo(f"  BigQuery: {settings.bigquery_project}.{settings.bigquery_dataset}")
    {%- endif %}

    click.echo("\nConfiguration valid!")


@cli.command()
def info() -> None:
    """Show pipeline information."""
    click.echo(f"Pipeline: {settings.pipeline_name}")
    click.echo(f"Version: {settings.version}")
    click.echo(f"Environment: {settings.environment}")
    click.echo(f"Datadog Service: {settings.dd_service}")


if __name__ == "__main__":
    cli()
