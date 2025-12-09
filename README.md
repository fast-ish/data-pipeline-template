# Data Pipeline Golden Path Template

> The recommended way to build data pipelines at our organization.

[![Backstage](https://img.shields.io/badge/Backstage-Template-blue)](https://backstage.io)
[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org/)
[![License](https://img.shields.io/badge/License-Internal-red)]()

## What's Included

| Category | Features |
|----------|----------|
| **Core** | Python 3.12, Pydantic 2.0, modular ETL architecture |
| **Pipeline Types** | Batch, Streaming, Hybrid |
| **Orchestrators** | Airflow, Dagster, Prefect, Step Functions |
| **Compute** | Spark, Dask, Polars, Pandas |
| **Sources** | S3, PostgreSQL, Redshift, Kafka, MSK, Kinesis, REST API |
| **Sinks** | S3, Redshift, Snowflake, BigQuery, Elasticsearch, DynamoDB |
| **Data Formats** | Parquet, Delta Lake, Iceberg, JSON, CSV |
| **Data Quality** | Great Expectations, Pandera, Soda |
| **Observability** | OpenTelemetry + Grafana, structured logging, pipeline metrics |
| **Quality** | Ruff, mypy, Bandit, pytest |
| **DevEx** | uv, pre-commit, VS Code config |

## Quick Start

1. Go to [Backstage Software Catalog](https://backstage.yourcompany.com/create)
2. Select "Data Pipeline (Golden Path)"
3. Fill in the form
4. Click "Create"
5. Clone and start building

## What You'll Get

```
your-pipeline/
├── src/
│   ├── core/             # Config, logging, metrics, notifications
│   ├── extractors/       # Data source readers
│   ├── transformers/     # Transformation logic
│   ├── quality/          # Data validators
│   ├── loaders/          # Destination writers
│   ├── pipelines/        # Pipeline definitions
│   ├── pipeline.py       # Pipeline orchestrator
│   └── cli.py            # CLI entry point
├── tests/                # Test suite
├── orchestration/        # Airflow/Dagster/Prefect configs
├── k8s/                  # Kubernetes manifests
├── .github/              # CI/CD workflows
├── docs/                 # Documentation
├── Dockerfile            # Multi-stage build
└── pyproject.toml        # Dependencies
```

## Documentation

| Document | Description |
|----------|-------------|
| [Decision Guide](./docs/DECISIONS.md) | How to choose template options |
| [Golden Path Overview](./docs/index.md) | What and why |
| [Getting Started](./skeleton/docs/GETTING_STARTED.md) | First steps |
| [Patterns Guide](./skeleton/docs/PATTERNS.md) | Pipeline patterns |

## Support

- **Slack**: #data-eng
- **Office Hours**: Wednesdays 2-3pm

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-12 | Initial release |

---

🤘 Platform Team
