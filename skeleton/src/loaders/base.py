"""Base loader interface."""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from ddtrace import tracer

from src.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class BaseLoader(ABC, Generic[T]):
    """Base class for all data loaders."""

    def __init__(self) -> None:
        self._records_written = 0
        self._bytes_written = 0

    @property
    def records_written(self) -> int:
        return self._records_written

    @property
    def bytes_written(self) -> int:
        return self._bytes_written

    @abstractmethod
    def load(self, data: list[T]) -> None:
        """Load a batch of records.

        Args:
            data: Batch of records to load.
        """
        pass

    @tracer.wrap(name="loader.write_batch")
    def __call__(self, data: list[T]) -> None:
        """Load data and track metrics."""
        self.load(data)
        self._records_written += len(data)
        logger.info(
            "batch_loaded",
            batch_size=len(data),
            total_records=self._records_written,
        )

    def close(self) -> None:
        """Clean up resources. Override in subclasses."""
        pass
