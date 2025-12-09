"""Base extractor interface."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, Generic, TypeVar

from ddtrace import tracer

from src.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class BaseExtractor(ABC, Generic[T]):
    """Base class for all data extractors."""

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = batch_size
        self._records_read = 0

    @property
    def records_read(self) -> int:
        return self._records_read

    @abstractmethod
    def extract(self) -> Iterator[list[T]]:
        """Extract data in batches.

        Yields:
            Batches of records.
        """
        pass

    @tracer.wrap(name="extractor.read_batch")
    def _track_batch(self, batch: list[T]) -> list[T]:
        """Track batch read metrics."""
        self._records_read += len(batch)
        logger.info(
            "batch_extracted",
            batch_size=len(batch),
            total_records=self._records_read,
        )
        return batch

    def __iter__(self) -> Iterator[list[T]]:
        return self.extract()
