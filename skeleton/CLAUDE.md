# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/claude-code) when working with this codebase.

## Project Overview

This is a **data pipeline** project built with Python 3.12. It implements an ETL pattern for extracting data from sources, transforming it, validating quality, and loading to destinations.

## Technology Stack

- **Language**: Python 3.12
- **Package Manager**: uv
- **Configuration**: Pydantic Settings
- **Logging**: structlog (JSON structured logging)
- **Tracing**: OpenTelemetry (Grafana Tempo)
{%- if values.computeEngine == "polars" %}
- **Data Processing**: Polars
{%- elif values.computeEngine == "spark" %}
- **Data Processing**: PySpark
{%- elif values.computeEngine == "dask" %}
- **Data Processing**: Dask
{%- else %}
- **Data Processing**: Pandas
{%- endif %}
{%- if values.dataQuality == "great-expectations" %}
- **Data Quality**: Great Expectations
{%- elif values.dataQuality == "pandera" %}
- **Data Quality**: Pandera
{%- elif values.dataQuality == "soda" %}
- **Data Quality**: Soda
{%- endif %}
- **Testing**: pytest

## Common Commands

```bash
# Install dependencies
uv sync

# Run the pipeline
uv run python -m src.cli run

# Run with dry-run (no loading)
uv run python -m src.cli run --dry-run

# Validate configuration
uv run python -m src.cli validate

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src --cov-report=html

# Lint code
uv run ruff check .

# Format code
uv run ruff format .

# Type check
uv run mypy src
```

## Architecture

The pipeline follows a modular ETL architecture:

```
Extractor → Transformers → Validators → Loader
```

### Key Components

- **`src/extractors/`**: Data source readers (S3, databases, Kafka, APIs)
- **`src/transformers/`**: Data transformation logic
- **`src/quality/`**: Data validation and quality checks
- **`src/loaders/`**: Data destination writers
- **`src/pipeline.py`**: Pipeline orchestration
- **`src/core/`**: Configuration, logging, metrics

### Component Base Classes

```python
# Extractors yield batches of records
class BaseExtractor(ABC):
    def extract(self) -> Iterator[list[T]]: ...

# Transformers process batches
class BaseTransformer(ABC):
    def transform(self, data: list[T]) -> list[T]: ...

# Validators check data quality
class BaseValidator(ABC):
    def validate(self, data: list[T]) -> ValidationReport: ...

# Loaders write batches to destinations
class BaseLoader(ABC):
    def load(self, data: list[T]) -> None: ...
```

## Configuration

Configuration is loaded from environment variables via Pydantic Settings:

```python
from src.core.config import settings

# Access settings
print(settings.pipeline_name)
print(settings.environment)
```

Required environment variables are documented in `.env.example`.

## Testing Patterns

```python
# Use fixtures from conftest.py
def test_pipeline(mock_extractor, mock_loader):
    pipeline = Pipeline(
        extractor=mock_extractor,
        loader=mock_loader,
    )
    metrics = pipeline.run()
    assert metrics.status == "success"

# Mock notifications in tests
with patch("src.pipeline.get_notification_service") as mock:
    mock.return_value = MagicMock()
    pipeline.run()
```

## Adding New Components

1. **New Extractor**: Extend `BaseExtractor`, implement `extract()` method
2. **New Transformer**: Extend `BaseTransformer`, implement `transform()` method
3. **New Validator**: Extend `BaseValidator`, implement `validate()` method
4. **New Loader**: Extend `BaseLoader`, implement `load()` method

See `docs/EXTENDING.md` for detailed examples.

## Observability

All pipeline operations are traced with OpenTelemetry:

- Traces appear in Grafana Tempo under service `${{values.name}}`
- Logs include trace IDs for correlation
- Metrics: `pipeline.duration_seconds`, `pipeline.records.*`

## Important Notes

- Always use `uv run` to run Python commands to ensure correct environment
- The pipeline is designed for batch processing with configurable batch sizes
- OpenTelemetry tracing is enabled by default; set `OTEL_SDK_DISABLED=true` to disable
- Tests mock external dependencies; integration tests require real connections
