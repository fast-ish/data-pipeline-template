"""Example pipeline configuration."""

from typing import Any

from src.core.logging import get_logger
from src.pipeline import Pipeline
from src.transformers.common import (
    AddFieldTransformer,
    FilterTransformer,
    TimestampTransformer,
)
from src.transformers.base import ChainedTransformer
from src.quality.validators import NotNullValidator, SchemaValidator

{%- if values.sourceType == "s3" %}
from src.extractors.s3 import S3Extractor
{%- elif values.sourceType == "postgres" or values.sourceType == "redshift" %}
from src.extractors.database import DatabaseExtractor
{%- elif values.sourceType == "kinesis" %}
from src.extractors.streaming import KinesisExtractor
{%- elif values.sourceType == "kafka" %}
from src.extractors.streaming import KafkaExtractor
{%- elif values.sourceType == "msk" %}
from src.extractors.streaming import MSKExtractor
{%- elif values.sourceType == "api" %}
from src.extractors.api import APIExtractor
{%- endif %}

{%- if values.sinkType == "s3" %}
from src.loaders.s3 import S3Loader
{%- elif values.sinkType == "postgres" or values.sinkType == "redshift" %}
from src.loaders.database import DatabaseLoader
{%- elif values.sinkType == "snowflake" %}
from src.loaders.database import SnowflakeLoader
{%- elif values.sinkType == "bigquery" %}
from src.loaders.database import BigQueryLoader
{%- elif values.sinkType == "elasticsearch" %}
from src.loaders.database import ElasticsearchLoader
{%- elif values.sinkType == "dynamodb" %}
from src.loaders.database import DynamoDBLoader
{%- endif %}

logger = get_logger(__name__)


def create_pipeline(dry_run: bool = False) -> Pipeline:
    """Create the configured pipeline.

    Args:
        dry_run: If True, skip loading data.

    Returns:
        Configured pipeline instance.
    """
    # Configure extractor
    {%- if values.sourceType == "s3" %}
    extractor = S3Extractor(
        batch_size=1000,
    )
    {%- elif values.sourceType == "postgres" or values.sourceType == "redshift" %}
    extractor = DatabaseExtractor(
        query="SELECT * FROM source_table",
        batch_size=1000,
    )
    {%- elif values.sourceType == "kinesis" %}
    extractor = KinesisExtractor(
        batch_size=100,
    )
    {%- elif values.sourceType == "kafka" %}
    extractor = KafkaExtractor(
        batch_size=100,
    )
    {%- elif values.sourceType == "msk" %}
    extractor = MSKExtractor(
        batch_size=100,
    )
    {%- elif values.sourceType == "api" %}
    extractor = APIExtractor(
        endpoint="/data",
        batch_size=100,
    )
    {%- endif %}

    # Configure transformers
    transformers = [
        ChainedTransformer(
            # Filter out invalid records
            FilterTransformer(lambda r: r.get("status") == "active"),
            # Add processing timestamp
            TimestampTransformer(),
            # Add pipeline metadata
            AddFieldTransformer("_pipeline", "${{values.name}}"),
        ),
    ]

    # Configure validators
    validators = [
        SchemaValidator(
            required_fields=["id", "created_at"],
            field_types={"id": str},
            fail_on_error=True,
        ),
        NotNullValidator(
            fields=["id"],
            fail_on_error=True,
        ),
    ]

    # Configure loader (skip in dry run)
    loader = None
    if not dry_run:
        {%- if values.sinkType == "s3" %}
        loader = S3Loader(
            partition_by=["date"],
        )
        {%- elif values.sinkType == "postgres" or values.sinkType == "redshift" %}
        loader = DatabaseLoader(
            table_name="target_table",
            upsert_keys=["id"],
        )
        {%- elif values.sinkType == "snowflake" %}
        loader = SnowflakeLoader(
            table_name="target_table",
        )
        {%- elif values.sinkType == "bigquery" %}
        loader = BigQueryLoader(
            table_name="target_table",
        )
        {%- elif values.sinkType == "elasticsearch" %}
        loader = ElasticsearchLoader(
            id_field="id",
        )
        {%- elif values.sinkType == "dynamodb" %}
        loader = DynamoDBLoader(
            table_name="target_table",
        )
        {%- endif %}

    return Pipeline(
        extractor=extractor,
        transformers=transformers,
        validators=validators,
        loader=loader,
    )
