"""Test fixtures."""

from datetime import datetime
from typing import Any
from collections.abc import Iterator

import pytest

from src.extractors.base import BaseExtractor
from src.loaders.base import BaseLoader


class MockExtractor(BaseExtractor[dict[str, Any]]):
    """Mock extractor for testing."""

    def __init__(self, data: list[dict[str, Any]], batch_size: int = 10) -> None:
        super().__init__(batch_size=batch_size)
        self.data = data

    def extract(self) -> Iterator[list[dict[str, Any]]]:
        for i in range(0, len(self.data), self.batch_size):
            batch = self.data[i : i + self.batch_size]
            yield self._track_batch(batch)


class MockLoader(BaseLoader[dict[str, Any]]):
    """Mock loader for testing."""

    def __init__(self) -> None:
        super().__init__()
        self.loaded_data: list[dict[str, Any]] = []

    def load(self, data: list[dict[str, Any]]) -> None:
        self.loaded_data.extend(data)


@pytest.fixture
def sample_records() -> list[dict[str, Any]]:
    """Sample records for testing."""
    return [
        {"id": "1", "name": "Record 1", "status": "active", "value": 100, "created_at": datetime.utcnow().isoformat()},
        {"id": "2", "name": "Record 2", "status": "active", "value": 200, "created_at": datetime.utcnow().isoformat()},
        {"id": "3", "name": "Record 3", "status": "inactive", "value": 300, "created_at": datetime.utcnow().isoformat()},
        {"id": "4", "name": "Record 4", "status": "active", "value": 400, "created_at": datetime.utcnow().isoformat()},
        {"id": "5", "name": "Record 5", "status": "active", "value": 500, "created_at": datetime.utcnow().isoformat()},
    ]


@pytest.fixture
def mock_extractor(sample_records: list[dict[str, Any]]) -> MockExtractor:
    """Create mock extractor with sample data."""
    return MockExtractor(sample_records)


@pytest.fixture
def mock_loader() -> MockLoader:
    """Create mock loader."""
    return MockLoader()
