# Pipeline Patterns

This document describes common patterns and best practices for building data pipelines.

## Architecture Overview

```
┌────────────┐     ┌──────────────┐     ┌────────────┐     ┌────────┐
│  Extractor │────▶│ Transformers │────▶│ Validators │────▶│ Loader │
└────────────┘     └──────────────┘     └────────────┘     └────────┘
      │                   │                   │                 │
      └───────────────────┴───────────────────┴─────────────────┘
                                    │
                             ┌──────┴──────┐
                             │   Metrics   │
                             └─────────────┘
```

## Extractors

### Batch Extraction

For large datasets, use server-side cursors and streaming:

```python
from src.extractors.base import BaseExtractor

class MyExtractor(BaseExtractor[dict]):
    def extract(self) -> Iterator[list[dict]]:
        # Stream data in batches
        for batch in self._fetch_paginated():
            yield self._track_batch(batch)
```

### Incremental Extraction

Track watermarks for incremental loads:

```python
class IncrementalExtractor(BaseExtractor[dict]):
    def __init__(self, watermark_column: str):
        super().__init__()
        self.watermark_column = watermark_column
        self.last_watermark = self._load_watermark()

    def extract(self) -> Iterator[list[dict]]:
        query = f"SELECT * FROM table WHERE {self.watermark_column} > '{self.last_watermark}'"
        # ... extract new data
```

## Transformers

### Chaining Transformers

Compose transformers for complex logic:

```python
from src.transformers.base import ChainedTransformer
from src.transformers.common import (
    FilterTransformer,
    MapTransformer,
    TimestampTransformer,
)

transformer = ChainedTransformer(
    FilterTransformer(lambda r: r["status"] == "active"),
    MapTransformer(enrich_record),
    TimestampTransformer(),
)
```

### DataFrame Transformations

{%- if values.computeEngine == "polars" %}

Use Polars for high-performance transformations:

```python
import polars as pl
from src.transformers.dataframe import PolarsTransformer

def transform_df(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .filter(pl.col("amount") > 0)
        .with_columns([
            pl.col("amount").cast(pl.Float64),
            pl.col("date").str.to_datetime(),
        ])
        .group_by("category")
        .agg(pl.col("amount").sum())
    )

transformer = PolarsTransformer(transformations=transform_df)
```
{%- endif %}

{%- if values.computeEngine == "spark" %}

Use Spark for distributed processing:

```python
from pyspark.sql import DataFrame
from src.transformers.dataframe import SparkTransformer

def transform_df(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(df.amount > 0)
        .groupBy("category")
        .agg({"amount": "sum"})
    )

transformer = SparkTransformer(transformations=transform_df)
```
{%- endif %}

## Validators

### Schema Validation

Validate data structure:

```python
from src.quality.validators import SchemaValidator

validator = SchemaValidator(
    required_fields=["id", "email", "created_at"],
    field_types={
        "id": str,
        "amount": (int, float),
    },
    fail_on_error=True,
)
```

### Business Rules

Implement custom validation:

```python
from src.quality.validators import CustomValidator

validator = CustomValidator(
    check_name="valid_email",
    predicate=lambda r: "@" in r.get("email", ""),
    fail_on_error=True,
)
```

{%- if values.dataQuality == "great-expectations" %}

### Great Expectations

Use expectation suites:

```python
import great_expectations as gx
from src.quality.validators import GreatExpectationsValidator

suite = gx.ExpectationSuite(
    expectation_suite_name="my_suite",
    expectations=[
        gx.ExpectColumnValuesToNotBeNull(column="id"),
        gx.ExpectColumnValuesToBeUnique(column="id"),
    ],
)

validator = GreatExpectationsValidator(suite)
```
{%- endif %}

## Loaders

### Upsert Pattern

Handle duplicates with upserts:

```python
from src.loaders.database import DatabaseLoader

loader = DatabaseLoader(
    table_name="target_table",
    upsert_keys=["id"],  # Primary key columns
)
```

### Partitioned Writes

Partition data for efficient queries:

```python
from src.loaders.s3 import S3Loader

loader = S3Loader(
    partition_by=["year", "month", "day"],
)
```

## Error Handling

### Dead Letter Queue

Handle failed records:

```python
class PipelineWithDLQ(Pipeline):
    def __init__(self, dlq_loader: BaseLoader, **kwargs):
        super().__init__(**kwargs)
        self.dlq_loader = dlq_loader

    def _apply_validators(self, data):
        valid = []
        invalid = []

        for record in data:
            try:
                self._validate_record(record)
                valid.append(record)
            except ValidationError as e:
                record["_error"] = str(e)
                invalid.append(record)

        if invalid:
            self.dlq_loader(invalid)

        return valid
```

## Idempotency

Ensure safe reruns:

```python
class IdempotentLoader(BaseLoader):
    def load(self, data: list[dict]) -> None:
        # Use run_id for deduplication
        run_id = os.environ.get("RUN_ID", str(uuid4()))

        for record in data:
            record["_run_id"] = run_id

        # Upsert with run_id included in key
        self._upsert(data, keys=["id", "_run_id"])
```

## Backfilling

Handle historical data loads:

```python
from datetime import date, timedelta

def backfill(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        extractor = DateRangeExtractor(
            start=current,
            end=current + timedelta(days=1),
        )
        pipeline = Pipeline(extractor=extractor, ...)
        pipeline.run()
        current += timedelta(days=1)
```

## Performance Tips

1. **Batch Size**: Start with 1000 records, tune based on memory/throughput
2. **Parallelism**: Use Dask or Spark for parallel processing
3. **Compression**: Use Parquet with snappy compression
4. **Partitioning**: Partition by date for time-series data
5. **Indexes**: Ensure target tables have appropriate indexes
