{%- if values.sourceType == "s3" %}
"""S3 data extractor."""

from collections.abc import Iterator
from typing import Any

import boto3
{%- if values.dataFormat == "parquet" %}
import pyarrow.parquet as pq
{%- elif values.dataFormat == "delta" %}
from deltalake import DeltaTable
{%- elif values.dataFormat == "iceberg" %}
from pyiceberg.catalog import load_catalog
{%- endif %}

from src.core.config import settings
from src.core.logging import get_logger
from src.extractors.base import BaseExtractor

logger = get_logger(__name__)


class S3Extractor(BaseExtractor[dict[str, Any]]):
    """Extract data from S3."""

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str | None = None,
        batch_size: int = 1000,
    ) -> None:
        super().__init__(batch_size=batch_size)
        self.bucket = bucket or settings.s3_bucket
        self.prefix = prefix or settings.s3_prefix
        self.s3 = boto3.client("s3", region_name=settings.aws_region)

    def _list_objects(self) -> list[str]:
        """List objects matching the prefix."""
        keys: list[str] = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        logger.info("s3_objects_listed", bucket=self.bucket, prefix=self.prefix, count=len(keys))
        return keys

    {%- if values.dataFormat == "parquet" %}
    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract data from Parquet files in S3."""
        keys = self._list_objects()

        for key in keys:
            if not key.endswith(".parquet"):
                continue

            s3_path = f"s3://{self.bucket}/{key}"
            logger.info("reading_parquet", path=s3_path)

            table = pq.read_table(s3_path)
            df = table.to_pandas()

            for i in range(0, len(df), self.batch_size):
                batch = df.iloc[i : i + self.batch_size].to_dict("records")
                yield self._track_batch(batch)
    {%- elif values.dataFormat == "delta" %}
    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract data from Delta Lake table in S3."""
        s3_path = f"s3://{self.bucket}/{self.prefix}"
        logger.info("reading_delta", path=s3_path)

        dt = DeltaTable(s3_path)
        df = dt.to_pandas()

        for i in range(0, len(df), self.batch_size):
            batch = df.iloc[i : i + self.batch_size].to_dict("records")
            yield self._track_batch(batch)
    {%- elif values.dataFormat == "iceberg" %}
    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract data from Iceberg table."""
        catalog = load_catalog("default")
        table = catalog.load_table(self.prefix)
        logger.info("reading_iceberg", table=self.prefix)

        df = table.scan().to_pandas()

        for i in range(0, len(df), self.batch_size):
            batch = df.iloc[i : i + self.batch_size].to_dict("records")
            yield self._track_batch(batch)
    {%- else %}
    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract data from JSON/CSV files in S3."""
        import json
        import csv
        from io import StringIO

        keys = self._list_objects()

        for key in keys:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read().decode("utf-8")

            if key.endswith(".json"):
                data = json.loads(content)
                records = data if isinstance(data, list) else [data]
            elif key.endswith(".csv"):
                reader = csv.DictReader(StringIO(content))
                records = list(reader)
            else:
                continue

            logger.info("reading_file", key=key, records=len(records))

            for i in range(0, len(records), self.batch_size):
                batch = records[i : i + self.batch_size]
                yield self._track_batch(batch)
    {%- endif %}
{%- endif %}
