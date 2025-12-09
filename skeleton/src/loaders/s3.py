{%- if values.sinkType == "s3" %}
"""S3 data loader."""

import json
from datetime import datetime
from typing import Any
from uuid import uuid4

import boto3
{%- if values.dataFormat == "parquet" %}
import pyarrow as pa
import pyarrow.parquet as pq
{%- elif values.dataFormat == "delta" %}
from deltalake import write_deltalake
import pandas as pd
{%- elif values.dataFormat == "iceberg" %}
from pyiceberg.catalog import load_catalog
import pyarrow as pa
{%- endif %}

from src.core.config import settings
from src.core.logging import get_logger
from src.loaders.base import BaseLoader

logger = get_logger(__name__)


class S3Loader(BaseLoader[dict[str, Any]]):
    """Load data to S3."""

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        super().__init__()
        self.bucket = bucket or settings.s3_bucket
        self.prefix = prefix or settings.s3_prefix
        self.partition_by = partition_by
        self.s3 = boto3.client("s3", region_name=settings.aws_region)

    def _get_partition_path(self, record: dict[str, Any]) -> str:
        """Generate partition path from record."""
        if not self.partition_by:
            return ""
        parts = [f"{key}={record.get(key, 'unknown')}" for key in self.partition_by]
        return "/".join(parts)

    def _generate_key(self, partition_path: str = "") -> str:
        """Generate unique S3 key."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid4())[:8]
        {%- if values.dataFormat == "parquet" %}
        extension = "parquet"
        {%- elif values.dataFormat == "json" %}
        extension = "json"
        {%- elif values.dataFormat == "csv" %}
        extension = "csv"
        {%- else %}
        extension = "parquet"
        {%- endif %}

        if partition_path:
            return f"{self.prefix}/{partition_path}/{timestamp}_{unique_id}.{extension}"
        return f"{self.prefix}/{timestamp}_{unique_id}.{extension}"

    {%- if values.dataFormat == "parquet" %}
    def load(self, data: list[dict[str, Any]]) -> None:
        """Write data as Parquet to S3."""
        if not data:
            return

        table = pa.Table.from_pylist(data)
        key = self._generate_key(self._get_partition_path(data[0]) if data else "")

        with pa.BufferOutputStream() as sink:
            pq.write_table(table, sink)
            body = sink.getvalue().to_pybytes()

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
        self._bytes_written += len(body)

        logger.info("s3_parquet_written", bucket=self.bucket, key=key, records=len(data))
    {%- elif values.dataFormat == "delta" %}
    def load(self, data: list[dict[str, Any]]) -> None:
        """Write data as Delta Lake to S3."""
        if not data:
            return

        df = pd.DataFrame(data)
        s3_path = f"s3://{self.bucket}/{self.prefix}"

        write_deltalake(
            s3_path,
            df,
            mode="append",
            partition_by=self.partition_by,
        )

        logger.info("s3_delta_written", path=s3_path, records=len(data))
    {%- elif values.dataFormat == "iceberg" %}
    def load(self, data: list[dict[str, Any]]) -> None:
        """Write data to Iceberg table."""
        if not data:
            return

        catalog = load_catalog("default")
        table = catalog.load_table(self.prefix)

        arrow_table = pa.Table.from_pylist(data)
        table.append(arrow_table)

        logger.info("iceberg_written", table=self.prefix, records=len(data))
    {%- else %}
    def load(self, data: list[dict[str, Any]]) -> None:
        """Write data as JSON to S3."""
        if not data:
            return

        key = self._generate_key(self._get_partition_path(data[0]) if data else "")
        body = json.dumps(data, default=str).encode("utf-8")

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
        self._bytes_written += len(body)

        logger.info("s3_json_written", bucket=self.bucket, key=key, records=len(data))
    {%- endif %}
{%- endif %}
