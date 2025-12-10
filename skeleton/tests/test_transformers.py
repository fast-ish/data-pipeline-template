"""Tests for transformers."""

from typing import Any

import pytest

from src.transformers.common import (
    AddFieldTransformer,
    DeduplicateTransformer,
    DropFieldsTransformer,
    FilterTransformer,
    MapTransformer,
    RenameFieldsTransformer,
    TimestampTransformer,
    TypeCastTransformer,
)
from src.transformers.base import ChainedTransformer


class TestFilterTransformer:
    def test_filters_records(self, sample_records: list[dict[str, Any]]) -> None:
        transformer = FilterTransformer(lambda r: r["status"] == "active")
        result = transformer(sample_records)

        assert len(result) == 4
        assert all(r["status"] == "active" for r in result)

    def test_empty_result(self) -> None:
        transformer = FilterTransformer(lambda r: False)
        result = transformer([{"id": "1"}])

        assert len(result) == 0


class TestMapTransformer:
    def test_maps_records(self) -> None:
        data = [{"value": 1}, {"value": 2}]
        transformer = MapTransformer(lambda r: {**r, "doubled": r["value"] * 2})
        result = transformer(data)

        assert result[0]["doubled"] == 2
        assert result[1]["doubled"] == 4


class TestAddFieldTransformer:
    def test_adds_static_field(self) -> None:
        data = [{"id": "1"}]
        transformer = AddFieldTransformer("source", "test")
        result = transformer(data)

        assert result[0]["source"] == "test"

    def test_adds_computed_field(self) -> None:
        data = [{"value": 10}]
        transformer = AddFieldTransformer("doubled", lambda r: r["value"] * 2)
        result = transformer(data)

        assert result[0]["doubled"] == 20


class TestRenameFieldsTransformer:
    def test_renames_fields(self) -> None:
        data = [{"old_name": "value"}]
        transformer = RenameFieldsTransformer({"old_name": "new_name"})
        result = transformer(data)

        assert "new_name" in result[0]
        assert "old_name" not in result[0]


class TestDropFieldsTransformer:
    def test_drops_fields(self) -> None:
        data = [{"keep": 1, "drop": 2}]
        transformer = DropFieldsTransformer(["drop"])
        result = transformer(data)

        assert "keep" in result[0]
        assert "drop" not in result[0]


class TestTypeCastTransformer:
    def test_casts_types(self) -> None:
        data = [{"value": "123"}]
        transformer = TypeCastTransformer({"value": int})
        result = transformer(data)

        assert result[0]["value"] == 123
        assert isinstance(result[0]["value"], int)


class TestTimestampTransformer:
    def test_adds_timestamp(self) -> None:
        data = [{"id": "1"}]
        transformer = TimestampTransformer()
        result = transformer(data)

        assert "_processed_at" in result[0]


class TestDeduplicateTransformer:
    def test_removes_duplicates(self) -> None:
        data = [
            {"id": "1", "value": 1},
            {"id": "1", "value": 2},
            {"id": "2", "value": 3},
        ]
        transformer = DeduplicateTransformer(["id"])
        result = transformer(data)

        assert len(result) == 2
        assert result[0]["value"] == 1  # Keeps first occurrence


class TestChainedTransformer:
    def test_chains_transformers(self) -> None:
        data = [{"value": 10}]
        transformer = ChainedTransformer(
            MapTransformer(lambda r: {**r, "doubled": r["value"] * 2}),
            AddFieldTransformer("processed", True),
        )
        result = transformer(data)

        assert result[0]["doubled"] == 20
        assert result[0]["processed"] is True
