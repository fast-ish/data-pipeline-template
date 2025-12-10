"""Base data quality validator."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from ddtrace import tracer

from src.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


@dataclass
class ValidationResult:
    """Result of a validation check."""

    success: bool
    check_name: str
    details: dict[str, Any] = field(default_factory=dict)
    failed_records: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Aggregated validation results."""

    results: list[ValidationResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(r.success for r in self.results)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.success)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.success)


class BaseValidator(ABC, Generic[T]):
    """Base class for data validators."""

    def __init__(self, fail_on_error: bool = True) -> None:
        self.fail_on_error = fail_on_error

    @abstractmethod
    def validate(self, data: list[T]) -> ValidationReport:
        """Validate a batch of records.

        Args:
            data: Batch of records to validate.

        Returns:
            Validation report with results.
        """
        pass

    @tracer.wrap(name="validator.validate_batch")
    def __call__(self, data: list[T]) -> list[T]:
        """Validate data and optionally fail on errors."""
        report = self.validate(data)

        logger.info(
            "validation_complete",
            passed=report.passed_count,
            failed=report.failed_count,
            all_passed=report.all_passed,
        )

        if not report.all_passed and self.fail_on_error:
            failed_checks = [r.check_name for r in report.results if not r.success]
            raise ValueError(f"Validation failed: {failed_checks}")

        return data
