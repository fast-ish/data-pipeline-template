# Decision Guide

How to choose the right options for your data pipeline.

## Pipeline Type

| Choose | When |
|--------|------|
| **Batch** | Processing historical data, scheduled jobs, data warehouse loads |
| **Streaming** | Real-time analytics, event processing, low-latency requirements |
| **Hybrid** | Need both real-time updates and historical backfills |

## Orchestrator

| Choose | When |
|--------|------|
| **Airflow** | Standard choice, complex dependencies, existing Airflow infrastructure |
| **Dagster** | Asset-centric pipelines, strong data lineage needs |
| **Prefect** | Python-first team, simple workflows, quick setup |
| **Step Functions** | AWS-native, serverless, event-driven |
| **None** | Simple cron/EventBridge triggers, single-job pipelines |

## Compute Engine

| Choose | When |
|--------|------|
| **Polars** | Single-node, fast, memory-efficient (default choice) |
| **Pandas** | Small data, team familiarity, extensive library ecosystem |
| **Spark** | Large datasets (100GB+), distributed processing, EMR/Glue |
| **Dask** | Medium-large data, want Pandas API with parallelism |

## Source Type

| Choose | When |
|--------|------|
| **S3** | Data lake, file-based sources |
| **PostgreSQL/Redshift** | Relational database sources |
| **Kafka** | Self-managed Kafka clusters |
| **MSK** | Amazon Managed Streaming for Kafka (IAM auth) |
| **Kinesis** | AWS-native streaming |
| **API** | REST API data sources |

## Data Format

| Choose | When |
|--------|------|
| **Parquet** | Default choice, columnar, efficient compression |
| **Delta Lake** | Need ACID transactions, time travel, schema evolution |
| **Iceberg** | Multi-engine support, large tables, partition evolution |
| **JSON** | Schema flexibility, human-readable, small data |
| **CSV** | Legacy systems, simple interchange |

## Data Quality

| Choose | When |
|--------|------|
| **Pandera** | DataFrame validation, Polars/Pandas native (default) |
| **Great Expectations** | Comprehensive suites, data docs, existing GE usage |
| **Soda** | SaaS integration, SQL-like checks |
| **None** | Custom validation, simple pipelines |

## Quick Recommendations

### Analytics Pipeline
- Type: Batch
- Orchestrator: Airflow
- Compute: Polars
- Format: Parquet
- Quality: Pandera

### Real-time Events
- Type: Streaming
- Orchestrator: None (K8s Deployment)
- Source: MSK
- Compute: Polars
- Quality: Pandera

### Data Warehouse Load
- Type: Batch
- Orchestrator: Airflow
- Compute: Polars
- Sink: Snowflake/Redshift
- Format: Parquet
