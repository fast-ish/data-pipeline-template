"""Data quality validators."""

from typing import Any, Callable

{%- if values.dataQuality == "great-expectations" %}
import great_expectations as gx
from great_expectations.core import ExpectationSuite
{%- endif %}
{%- if values.dataQuality == "pandera" %}
import pandas as pd
import pandera as pa
{%- endif %}
{%- if values.dataQuality == "soda" %}
from soda.core.scan import Scan
{%- endif %}

from src.core.logging import get_logger
from src.quality.base import BaseValidator, ValidationReport, ValidationResult

logger = get_logger(__name__)


class SchemaValidator(BaseValidator[dict[str, Any]]):
    """Validate records against a schema."""

    def __init__(
        self,
        required_fields: list[str],
        field_types: dict[str, type] | None = None,
        fail_on_error: bool = True,
    ) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.required_fields = required_fields
        self.field_types = field_types or {}

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate schema of records."""
        results: list[ValidationResult] = []

        # Check required fields
        missing_fields: list[dict[str, Any]] = []
        for record in data:
            missing = [f for f in self.required_fields if f not in record]
            if missing:
                missing_fields.append({"record": record, "missing": missing})

        results.append(
            ValidationResult(
                success=len(missing_fields) == 0,
                check_name="required_fields",
                details={"required": self.required_fields},
                failed_records=missing_fields,
            )
        )

        # Check field types
        if self.field_types:
            type_errors: list[dict[str, Any]] = []
            for record in data:
                for field, expected_type in self.field_types.items():
                    if field in record and record[field] is not None:
                        if not isinstance(record[field], expected_type):
                            type_errors.append({
                                "record": record,
                                "field": field,
                                "expected": expected_type.__name__,
                                "actual": type(record[field]).__name__,
                            })

            results.append(
                ValidationResult(
                    success=len(type_errors) == 0,
                    check_name="field_types",
                    details={"type_mapping": {k: v.__name__ for k, v in self.field_types.items()}},
                    failed_records=type_errors,
                )
            )

        return ValidationReport(results=results)


class RangeValidator(BaseValidator[dict[str, Any]]):
    """Validate numeric fields are within range."""

    def __init__(
        self,
        field: str,
        min_value: float | None = None,
        max_value: float | None = None,
        fail_on_error: bool = True,
    ) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.field = field
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate field values are in range."""
        out_of_range: list[dict[str, Any]] = []

        for record in data:
            if self.field not in record:
                continue

            value = record[self.field]
            if value is None:
                continue

            if self.min_value is not None and value < self.min_value:
                out_of_range.append({"record": record, "value": value, "violation": "below_min"})
            elif self.max_value is not None and value > self.max_value:
                out_of_range.append({"record": record, "value": value, "violation": "above_max"})

        return ValidationReport(
            results=[
                ValidationResult(
                    success=len(out_of_range) == 0,
                    check_name=f"range_{self.field}",
                    details={"field": self.field, "min": self.min_value, "max": self.max_value},
                    failed_records=out_of_range,
                )
            ]
        )


class NotNullValidator(BaseValidator[dict[str, Any]]):
    """Validate fields are not null."""

    def __init__(self, fields: list[str], fail_on_error: bool = True) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.fields = fields

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate fields are not null."""
        null_records: list[dict[str, Any]] = []

        for record in data:
            null_fields = [f for f in self.fields if record.get(f) is None]
            if null_fields:
                null_records.append({"record": record, "null_fields": null_fields})

        return ValidationReport(
            results=[
                ValidationResult(
                    success=len(null_records) == 0,
                    check_name="not_null",
                    details={"fields": self.fields},
                    failed_records=null_records,
                )
            ]
        )


class UniqueValidator(BaseValidator[dict[str, Any]]):
    """Validate field values are unique."""

    def __init__(self, field: str, fail_on_error: bool = True) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.field = field

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate field values are unique within batch."""
        seen: dict[Any, list[dict[str, Any]]] = {}

        for record in data:
            value = record.get(self.field)
            if value is not None:
                if value not in seen:
                    seen[value] = []
                seen[value].append(record)

        duplicates = [
            {"value": value, "records": records}
            for value, records in seen.items()
            if len(records) > 1
        ]

        return ValidationReport(
            results=[
                ValidationResult(
                    success=len(duplicates) == 0,
                    check_name=f"unique_{self.field}",
                    details={"field": self.field},
                    failed_records=duplicates,
                )
            ]
        )


class CustomValidator(BaseValidator[dict[str, Any]]):
    """Custom validation using a predicate function."""

    def __init__(
        self,
        check_name: str,
        predicate: Callable[[dict[str, Any]], bool],
        fail_on_error: bool = True,
    ) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.check_name = check_name
        self.predicate = predicate

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate records using custom predicate."""
        failed = [record for record in data if not self.predicate(record)]

        return ValidationReport(
            results=[
                ValidationResult(
                    success=len(failed) == 0,
                    check_name=self.check_name,
                    details={},
                    failed_records=failed,
                )
            ]
        )


{%- if values.dataQuality == "great-expectations" %}


class GreatExpectationsValidator(BaseValidator[dict[str, Any]]):
    """Validate data using Great Expectations."""

    def __init__(
        self,
        expectation_suite: ExpectationSuite,
        fail_on_error: bool = True,
    ) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.expectation_suite = expectation_suite
        self.context = gx.get_context()

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate data using Great Expectations suite."""
        import pandas as pd

        df = pd.DataFrame(data)
        batch = self.context.sources.pandas_default.read_dataframe(df)

        validation_result = batch.validate(self.expectation_suite)

        results = []
        for result in validation_result.results:
            results.append(
                ValidationResult(
                    success=result.success,
                    check_name=result.expectation_config.expectation_type,
                    details=result.result,
                )
            )

        return ValidationReport(results=results)
{%- endif %}


{%- if values.dataQuality == "pandera" %}


class PanderaValidator(BaseValidator[dict[str, Any]]):
    """Validate data using Pandera schema."""

    def __init__(
        self,
        schema: pa.DataFrameSchema,
        fail_on_error: bool = True,
    ) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.schema = schema

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate data using Pandera schema."""
        df = pd.DataFrame(data)

        try:
            self.schema.validate(df, lazy=True)
            return ValidationReport(
                results=[
                    ValidationResult(success=True, check_name="pandera_schema")
                ]
            )
        except pa.errors.SchemaErrors as e:
            results = []
            for error in e.schema_errors:
                results.append(
                    ValidationResult(
                        success=False,
                        check_name=str(error.check),
                        details={"column": error.column, "error": str(error)},
                    )
                )
            return ValidationReport(results=results)
{%- endif %}


{%- if values.dataQuality == "soda" %}


class SodaValidator(BaseValidator[dict[str, Any]]):
    """Validate data using Soda checks."""

    def __init__(
        self,
        checks_yaml: str,
        data_source_name: str = "pipeline_data",
        fail_on_error: bool = True,
    ) -> None:
        super().__init__(fail_on_error=fail_on_error)
        self.checks_yaml = checks_yaml
        self.data_source_name = data_source_name

    def validate(self, data: list[dict[str, Any]]) -> ValidationReport:
        """Validate data using Soda checks."""
        import pandas as pd

        df = pd.DataFrame(data)

        scan = Scan()
        scan.set_data_source_name(self.data_source_name)
        scan.add_pandas_dataframe(dataset_name="data", pandas_df=df)
        scan.add_sodacl_yaml_str(self.checks_yaml)
        scan.execute()

        results = []
        for check in scan.get_checks():
            results.append(
                ValidationResult(
                    success=check.outcome.is_pass(),
                    check_name=check.name,
                    details={"outcome": str(check.outcome)},
                )
            )

        return ValidationReport(results=results)
{%- endif %}
