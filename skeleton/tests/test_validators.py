"""Tests for validators."""

from typing import Any

import pytest

from src.quality.validators import (
    CustomValidator,
    NotNullValidator,
    RangeValidator,
    SchemaValidator,
    UniqueValidator,
)


class TestSchemaValidator:
    def test_validates_required_fields(self) -> None:
        validator = SchemaValidator(required_fields=["id", "name"], fail_on_error=False)
        data = [{"id": "1", "name": "Test"}]
        result = validator(data)

        assert len(result) == 1

    def test_fails_missing_required_fields(self) -> None:
        validator = SchemaValidator(required_fields=["id", "missing"], fail_on_error=True)
        data = [{"id": "1"}]

        with pytest.raises(ValueError, match="Validation failed"):
            validator(data)

    def test_validates_field_types(self) -> None:
        validator = SchemaValidator(
            required_fields=[],
            field_types={"value": int},
            fail_on_error=True,
        )
        data = [{"value": 123}]
        result = validator(data)

        assert len(result) == 1

    def test_fails_wrong_field_type(self) -> None:
        validator = SchemaValidator(
            required_fields=[],
            field_types={"value": int},
            fail_on_error=True,
        )
        data = [{"value": "not an int"}]

        with pytest.raises(ValueError, match="Validation failed"):
            validator(data)


class TestRangeValidator:
    def test_validates_in_range(self) -> None:
        validator = RangeValidator(field="value", min_value=0, max_value=100, fail_on_error=True)
        data = [{"value": 50}]
        result = validator(data)

        assert len(result) == 1

    def test_fails_below_min(self) -> None:
        validator = RangeValidator(field="value", min_value=10, fail_on_error=True)
        data = [{"value": 5}]

        with pytest.raises(ValueError, match="Validation failed"):
            validator(data)

    def test_fails_above_max(self) -> None:
        validator = RangeValidator(field="value", max_value=100, fail_on_error=True)
        data = [{"value": 150}]

        with pytest.raises(ValueError, match="Validation failed"):
            validator(data)


class TestNotNullValidator:
    def test_validates_not_null(self) -> None:
        validator = NotNullValidator(fields=["id"], fail_on_error=True)
        data = [{"id": "1"}]
        result = validator(data)

        assert len(result) == 1

    def test_fails_null_value(self) -> None:
        validator = NotNullValidator(fields=["id"], fail_on_error=True)
        data = [{"id": None}]

        with pytest.raises(ValueError, match="Validation failed"):
            validator(data)


class TestUniqueValidator:
    def test_validates_unique(self) -> None:
        validator = UniqueValidator(field="id", fail_on_error=True)
        data = [{"id": "1"}, {"id": "2"}]
        result = validator(data)

        assert len(result) == 2

    def test_fails_duplicates(self) -> None:
        validator = UniqueValidator(field="id", fail_on_error=True)
        data = [{"id": "1"}, {"id": "1"}]

        with pytest.raises(ValueError, match="Validation failed"):
            validator(data)


class TestCustomValidator:
    def test_validates_with_predicate(self) -> None:
        validator = CustomValidator(
            check_name="is_positive",
            predicate=lambda r: r.get("value", 0) > 0,
            fail_on_error=True,
        )
        data = [{"value": 10}]
        result = validator(data)

        assert len(result) == 1

    def test_fails_custom_predicate(self) -> None:
        validator = CustomValidator(
            check_name="is_positive",
            predicate=lambda r: r.get("value", 0) > 0,
            fail_on_error=True,
        )
        data = [{"value": -5}]

        with pytest.raises(ValueError, match="Validation failed"):
            validator(data)
