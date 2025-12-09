"""Base transformer interface."""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from ddtrace import tracer

from src.core.logging import get_logger

logger = get_logger(__name__)

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class BaseTransformer(ABC, Generic[InputT, OutputT]):
    """Base class for all data transformers."""

    def __init__(self) -> None:
        self._records_transformed = 0
        self._records_failed = 0

    @property
    def records_transformed(self) -> int:
        return self._records_transformed

    @property
    def records_failed(self) -> int:
        return self._records_failed

    @abstractmethod
    def transform(self, data: list[InputT]) -> list[OutputT]:
        """Transform a batch of records.

        Args:
            data: Input batch of records.

        Returns:
            Transformed batch of records.
        """
        pass

    @tracer.wrap(name="transformer.transform_batch")
    def __call__(self, data: list[InputT]) -> list[OutputT]:
        """Transform data and track metrics."""
        try:
            result = self.transform(data)
            self._records_transformed += len(result)
            logger.info(
                "batch_transformed",
                input_size=len(data),
                output_size=len(result),
                total_transformed=self._records_transformed,
            )
            return result
        except Exception as e:
            self._records_failed += len(data)
            logger.error(
                "transform_failed",
                error=str(e),
                batch_size=len(data),
            )
            raise


class ChainedTransformer(BaseTransformer[Any, Any]):
    """Chain multiple transformers together."""

    def __init__(self, *transformers: BaseTransformer) -> None:
        super().__init__()
        self.transformers = transformers

    def transform(self, data: list[Any]) -> list[Any]:
        """Apply all transformers in sequence."""
        result = data
        for transformer in self.transformers:
            result = transformer(result)
        return result
