# Extending the Pipeline

This guide covers how to extend and customize the pipeline.

## Adding a New Extractor

1. Create a new extractor class:

```python
# src/extractors/my_source.py
from collections.abc import Iterator
from typing import Any

from src.extractors.base import BaseExtractor
from src.core.logging import get_logger

logger = get_logger(__name__)


class MySourceExtractor(BaseExtractor[dict[str, Any]]):
    """Extract data from MySource."""

    def __init__(self, config: dict, batch_size: int = 1000) -> None:
        super().__init__(batch_size=batch_size)
        self.config = config
        # Initialize your client

    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract data in batches."""
        # Implement pagination/streaming
        while has_more_data:
            batch = self._fetch_next_batch()
            yield self._track_batch(batch)

        logger.info("extraction_complete", total=self.records_read)
```

2. Register in `__init__.py` if needed.

## Adding a New Transformer

1. Create a transformer class:

```python
# src/transformers/my_transform.py
from typing import Any

from src.transformers.base import BaseTransformer
from src.core.logging import get_logger

logger = get_logger(__name__)


class MyTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Custom transformation logic."""

    def __init__(self, param: str) -> None:
        super().__init__()
        self.param = param

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply transformation."""
        results = []
        for record in data:
            transformed = self._transform_record(record)
            results.append(transformed)
        return results

    def _transform_record(self, record: dict) -> dict:
        # Your logic here
        return {**record, "transformed": True}
```

## Adding a New Validator

1. Create a validator class:

```python
# src/quality/my_validator.py
from typing import Any

from src.quality.base import BaseValidator, ValidationReport, ValidationResult


class MyValidator(BaseValidator[dict[str, Any]]):
    """Custom validation logic."""

    def __init__(self, threshold: float, fail_on_error: bool = True) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.threshold = threshold

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate data against custom rules."""
        failed = []

        for record in data:
            if not self._check_record(record):
                failed.append(record)

        return ValidationReport(
            results=[
                ValidationResult(
                    success=len(failed) == 0,
                    check_name="my_custom_check",
                    details={"threshold": self.threshold},
                    failed_records=failed,
                )
            ]
        )

    def _check_record(self, record: dict) -> bool:
        # Your validation logic
        return record.get("score", 0) >= self.threshold
```

## Adding a New Loader

1. Create a loader class:

```python
# src/loaders/my_sink.py
from typing import Any

from src.loaders.base import BaseLoader
from src.core.logging import get_logger

logger = get_logger(__name__)


class MySinkLoader(BaseLoader[dict[str, Any]]):
    """Load data to MySink."""

    def __init__(self, connection_string: str) -> None:
        super().__init__()
        self.connection_string = connection_string
        # Initialize your client

    def load(self, data: list[dict[str, Any]]) -> None:
        """Load batch to destination."""
        if not data:
            return

        # Your loading logic
        self._write_batch(data)

        logger.info("batch_loaded", records=len(data))

    def close(self) -> None:
        """Clean up resources."""
        # Close connections
        pass
```

## Creating a Custom Pipeline

Combine components into a pipeline:

```python
# src/pipelines/custom.py
from src.pipeline import Pipeline
from src.extractors.my_source import MySourceExtractor
from src.transformers.my_transform import MyTransformer
from src.quality.my_validator import MyValidator
from src.loaders.my_sink import MySinkLoader


def create_custom_pipeline() -> Pipeline:
    """Create custom pipeline."""
    return Pipeline(
        name="custom-pipeline",
        extractor=MySourceExtractor(
            config={"endpoint": "https://api.example.com"},
            batch_size=500,
        ),
        transformers=[
            MyTransformer(param="value"),
        ],
        validators=[
            MyValidator(threshold=0.8, fail_on_error=True),
        ],
        loader=MySinkLoader(
            connection_string="postgres://...",
        ),
    )
```

## Adding Configuration Options

1. Extend settings in `src/core/config.py`:

```python
class Settings(BaseSettings):
    # Existing settings...

    # Add your settings
    my_source_url: str = Field(default="", alias="MY_SOURCE_URL")
    my_source_api_key: str = Field(default="", alias="MY_SOURCE_API_KEY")
```

2. Add to `.env.example`:

```
MY_SOURCE_URL=https://api.example.com
MY_SOURCE_API_KEY=your-api-key
```

## Adding Custom Metrics

Emit custom metrics via OpenTelemetry:

```python
from src.core.metrics import emit_metric

# In your component
emit_metric(
    name="custom.metric_name",
    value=123.45,
    tags={"dimension": "value"},
)
```

## Adding Tests

1. Create test file:

```python
# tests/test_my_component.py
import pytest
from src.transformers.my_transform import MyTransformer


class TestMyTransformer:
    def test_transforms_data(self):
        transformer = MyTransformer(param="value")
        data = [{"id": "1", "value": 100}]

        result = transformer(data)

        assert result[0]["transformed"] is True

    def test_handles_empty_data(self):
        transformer = MyTransformer(param="value")

        result = transformer([])

        assert result == []
```

2. Run tests:

```bash
uv run pytest tests/test_my_component.py -v
```

## Best Practices

1. **Type Hints**: Always use type hints for better IDE support and documentation
2. **Logging**: Use structured logging with context
3. **Metrics**: Emit metrics for monitoring
4. **Tests**: Write unit tests for all components
5. **Documentation**: Document public APIs with docstrings
6. **Error Handling**: Raise meaningful exceptions
