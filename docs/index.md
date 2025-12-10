# Data Pipeline Golden Path

## What is a Golden Path?

A golden path is a standardized, well-supported way to accomplish a common task. For data pipelines, this means:

- **Consistent patterns** across all pipelines
- **Built-in observability** with OpenTelemetry + Grafana
- **Security by default** with proper credential management
- **Production-ready** from day one

## Why Use This Template?

### Before (DIY Pipeline)
- Weeks to set up infrastructure
- Inconsistent patterns across teams
- Missing observability
- Security gaps
- No standardized testing

### After (Golden Path)
- Minutes to scaffold
- Consistent, proven patterns
- Full Grafana integration
- Security best practices
- Test suite included

## Architecture

```
┌────────────┐     ┌──────────────┐     ┌────────────┐     ┌────────┐
│  Extractor │────▶│ Transformers │────▶│ Validators │────▶│ Loader │
└────────────┘     └──────────────┘     └────────────┘     └────────┘
```

### Components

| Component | Purpose |
|-----------|---------|
| Extractor | Read data from sources (S3, databases, streams) |
| Transformer | Apply business logic and data transformations |
| Validator | Ensure data quality before loading |
| Loader | Write data to destinations |

## Supported Options

### Pipeline Types
- **Batch**: Scheduled execution (hourly, daily)
- **Streaming**: Real-time processing from Kafka/Kinesis
- **Hybrid**: Batch backfills with streaming updates

### Orchestrators
- **Airflow**: Industry standard, MWAA on AWS
- **Dagster**: Modern, asset-based orchestration
- **Prefect**: Python-native, easy to use
- **Step Functions**: AWS-native, serverless

### Compute Engines
- **Polars**: Fast, memory-efficient (recommended for most)
- **Pandas**: Familiar, extensive ecosystem
- **Spark**: Distributed processing for large data
- **Dask**: Parallel processing, Pandas-like API

## Getting Started

1. Create pipeline from Backstage
2. Clone repository
3. Configure environment variables
4. Run locally: `uv run python -m src.cli run`
5. Deploy to Kubernetes

## Support

- **Slack**: #data-eng
- **Documentation**: This repo
- **Office Hours**: Wednesdays 2-3pm
