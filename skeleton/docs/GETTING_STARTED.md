# Getting Started

This guide helps you get the ${{values.name}} pipeline running locally and deployed to production.

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- Docker (for containerized deployment)
- Access to required data sources and sinks

## Local Development

### 1. Install Dependencies

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync
```

### 2. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit with your credentials
vim .env
```

Required environment variables:

{%- if values.sourceType == "s3" or values.sinkType == "s3" %}
- `S3_BUCKET`: S3 bucket name
- `S3_PREFIX`: Object prefix (default: `data`)
- `AWS_REGION`: AWS region
{%- endif %}
{%- if values.sourceType == "postgres" or values.sinkType == "postgres" %}
- `POSTGRES_HOST`: PostgreSQL host
- `POSTGRES_PORT`: PostgreSQL port (default: 5432)
- `POSTGRES_DATABASE`: Database name
- `POSTGRES_USER`: Database user
- `POSTGRES_PASSWORD`: Database password
{%- endif %}
{%- if values.sourceType == "kafka" %}
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_TOPIC`: Topic to consume from
- `KAFKA_CONSUMER_GROUP`: Consumer group ID
{%- endif %}
{%- if values.sinkType == "snowflake" %}
- `SNOWFLAKE_ACCOUNT`: Snowflake account identifier
- `SNOWFLAKE_USER`: Snowflake username
- `SNOWFLAKE_PASSWORD`: Snowflake password
- `SNOWFLAKE_WAREHOUSE`: Warehouse name
- `SNOWFLAKE_DATABASE`: Database name
- `SNOWFLAKE_SCHEMA`: Schema name (default: PUBLIC)
{%- endif %}

### 3. Validate Configuration

```bash
uv run python -m src.cli validate
```

### 4. Run the Pipeline

```bash
# Full run
uv run python -m src.cli run

# Dry run (validate without loading)
uv run python -m src.cli run --dry-run
```

## Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_transformers.py -v
```

## Code Quality

```bash
# Lint
uv run ruff check .

# Format
uv run ruff format .

# Type check
uv run mypy src
```

## Docker

### Build Image

```bash
docker build -t ${{values.name}}:latest .
```

### Run Container

```bash
docker run --env-file .env ${{values.name}}:latest run
```

## Deployment

{%- if values.orchestrator == "airflow" %}

### Airflow

1. Copy the DAG file to your Airflow DAGs folder:
   ```bash
   cp orchestration/airflow/dags/pipeline_dag.py $AIRFLOW_HOME/dags/
   ```

2. Set Airflow variables:
   ```bash
   airflow variables set environment production
   airflow variables set s3_bucket your-bucket-name
   ```

3. Enable the DAG in Airflow UI
{%- endif %}

{%- if values.orchestrator == "dagster" %}

### Dagster

1. Start the Dagster daemon:
   ```bash
   dagster dev -f orchestration/dagster/pipeline_job.py
   ```

2. Access the UI at http://localhost:3000

3. Enable the schedule in the Dagster UI
{%- endif %}

{%- if values.orchestrator == "prefect" %}

### Prefect

1. Create the deployment:
   ```bash
   python orchestration/prefect/pipeline_flow.py
   ```

2. Start a work agent:
   ```bash
   prefect agent start -q default
   ```

3. Enable the schedule in Prefect UI
{%- endif %}

### Kubernetes

1. Create secrets:
   ```bash
   kubectl create secret generic ${{values.name}}-secrets \
     --from-env-file=.env
   ```

2. Deploy:
   ```bash
   kubectl apply -k k8s/base/
   ```

## Monitoring

### Grafana

The pipeline automatically sends metrics and traces via OpenTelemetry:

- **Service**: `${{values.name}}`
- **Dashboard**: Search for "${{values.name}}" in Grafana
- **Traces**: Explore > Tempo > Filter by service

### Metrics

- `pipeline.duration_seconds`: Pipeline execution time
- `pipeline.records_read`: Total records extracted
- `pipeline.records_written`: Total records loaded
- `pipeline.records_failed`: Failed records count
- `pipeline.success_rate`: Success percentage

## Troubleshooting

See [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) for common issues and solutions.
