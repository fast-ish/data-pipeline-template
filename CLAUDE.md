# Data Pipeline Golden Path Template

Backstage software template for generating production-ready data pipelines with OpenTelemetry observability (Grafana stack).

## Structure

```
/template.yaml          # Backstage scaffolder definition (all parameters here)
/skeleton/              # Generated pipeline template (Jinja2 templated)
/docs/                  # Template-level documentation
```

## Key Files

- `template.yaml` - Template parameters and steps (scaffolder.backstage.io/v1beta3)
- `skeleton/pyproject.toml` - Dependencies with conditional inclusions
- `skeleton/src/pipeline.py` - Pipeline orchestrator
- `skeleton/src/cli.py` - CLI entry point
- `skeleton/src/core/config.py` - Pydantic settings

## Template Syntax

Uses Jinja2 via Backstage:
- Variables: `${{values.name}}`, `${{values.owner}}`
- Conditionals: `{%- if values.sourceType == "s3" %}...{%- endif %}`

## Testing Template Changes

```bash
cd skeleton
uv sync --dev
uv run python -m src.cli validate
uv run pytest
uv run ruff check .
uv run mypy src
```

## Template Options

| Parameter | Values |
|-----------|--------|
| pipelineType | batch, streaming, hybrid |
| orchestrator | airflow, dagster, prefect, step-functions, none |
| pythonVersion | 3.12, 3.11 |
| computeEngine | spark, dask, polars, pandas |
| sourceType | s3, postgres, redshift, kinesis, kafka, msk, api |
| sinkType | s3, redshift, snowflake, bigquery, postgres, dynamodb, elasticsearch |
| dataFormat | parquet, delta, iceberg, json, csv |
| dataQuality | great-expectations, pandera, soda, none |
| notifications | sns, slack, none |

## Conventions

- src/ layout with modular ETL components
- Base classes for extractors, transformers, validators, loaders
- Pydantic v2 for all configuration
- Structured JSON logging with structlog
- Type hints required (mypy strict)
- Ruff for linting and formatting

## Version Pinning

Keep these current:
- Polars: 1.16+
- Pandas: 2.2+
- PyArrow: 18.1+
- opentelemetry-sdk: 1.28+
- confluent-kafka: 2.6+

## Don't

- Use sync operations in extractors/loaders
- Skip type hints
- Add backwards-compatibility shims
- Use placeholder versions - verify on PyPI
