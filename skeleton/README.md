# ${{values.name}}

${{values.description}}

## Overview

This is a data pipeline that:
{%- if values.sourceType == "s3" %}
- Extracts data from S3
{%- elif values.sourceType == "postgres" %}
- Extracts data from PostgreSQL
{%- elif values.sourceType == "redshift" %}
- Extracts data from Redshift
{%- elif values.sourceType == "kafka" %}
- Extracts data from Kafka
{%- elif values.sourceType == "kinesis" %}
- Extracts data from Kinesis
{%- elif values.sourceType == "api" %}
- Extracts data from REST API
{%- endif %}
- Transforms and validates the data
{%- if values.sinkType == "s3" %}
- Loads to S3 ({{ values.dataFormat }})
{%- elif values.sinkType == "postgres" %}
- Loads to PostgreSQL
{%- elif values.sinkType == "redshift" %}
- Loads to Redshift
{%- elif values.sinkType == "snowflake" %}
- Loads to Snowflake
{%- elif values.sinkType == "bigquery" %}
- Loads to BigQuery
{%- elif values.sinkType == "elasticsearch" %}
- Loads to Elasticsearch
{%- elif values.sinkType == "dynamodb" %}
- Loads to DynamoDB
{%- endif %}

## Quick Start

```bash
# Install dependencies
uv sync

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Validate configuration
uv run python -m src.cli validate

# Run the pipeline
uv run python -m src.cli run
```

## Architecture

```
┌────────────┐     ┌──────────────┐     ┌────────────┐     ┌────────┐
│  Extractor │────▶│ Transformers │────▶│ Validators │────▶│ Loader │
└────────────┘     └──────────────┘     └────────────┘     └────────┘
```

## Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| `ENVIRONMENT` | Environment (development/staging/production) | Yes |
{%- if values.sourceType == "s3" or values.sinkType == "s3" %}
| `S3_BUCKET` | S3 bucket name | Yes |
| `AWS_REGION` | AWS region | Yes |
{%- endif %}
{%- if values.sourceType == "postgres" or values.sinkType == "postgres" %}
| `POSTGRES_HOST` | PostgreSQL host | Yes |
| `POSTGRES_DATABASE` | Database name | Yes |
| `POSTGRES_USER` | Database user | Yes |
| `POSTGRES_PASSWORD` | Database password | Yes |
{%- endif %}
{%- if values.sourceType == "kafka" %}
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers | Yes |
| `KAFKA_TOPIC` | Kafka topic | Yes |
{%- endif %}
| `OTEL_SERVICE_NAME` | Service name for tracing | No (default: ${{values.name}}) |

See `.env.example` for all configuration options.

## Development

```bash
# Run tests
uv run pytest

# Lint
uv run ruff check .

# Format
uv run ruff format .

# Type check
uv run mypy src
```

## Deployment

{%- if values.orchestrator == "airflow" %}

### Airflow

The pipeline is orchestrated via Airflow. See `orchestration/airflow/` for DAG configuration.
{%- endif %}

{%- if values.orchestrator == "dagster" %}

### Dagster

The pipeline is orchestrated via Dagster. See `orchestration/dagster/` for job configuration.
{%- endif %}

### Kubernetes

```bash
# Create secrets
kubectl create secret generic ${{values.name}}-secrets --from-env-file=.env

# Deploy
kubectl apply -k k8s/base/
```

## Monitoring

- **Grafana Service**: `${{values.name}}`
- **Metrics**: `pipeline.duration_seconds`, `pipeline.records.*`
- **Logs**: Structured JSON with OpenTelemetry trace correlation

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [Architecture](docs/architecture.md)
- [Patterns](docs/PATTERNS.md)
- [Extending](docs/EXTENDING.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)

## Support

For issues and questions, contact the data engineering team.
