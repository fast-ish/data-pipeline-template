"""Common transformers that work with any compute engine."""

from datetime import datetime
from typing import Any, Callable

from src.core.logging import get_logger
from src.transformers.base import BaseTransformer

logger = get_logger(__name__)


class MapTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Apply a function to each record."""

    def __init__(self, func: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
        super().__init__()
        self.func = func

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply function to each record."""
        return [self.func(record) for record in data]


class FilterTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Filter records based on a predicate."""

    def __init__(self, predicate: Callable[[dict[str, Any]], bool]) -> None:
        super().__init__()
        self.predicate = predicate

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter records matching the predicate."""
        return [record for record in data if self.predicate(record)]


class AddFieldTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Add a field to each record."""

    def __init__(
        self,
        field_name: str,
        value: Any | Callable[[dict[str, Any]], Any],
    ) -> None:
        super().__init__()
        self.field_name = field_name
        self.value = value

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add field to each record."""
        results = []
        for record in data:
            new_record = record.copy()
            if callable(self.value):
                new_record[self.field_name] = self.value(record)
            else:
                new_record[self.field_name] = self.value
            results.append(new_record)
        return results


class RenameFieldsTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Rename fields in records."""

    def __init__(self, field_mapping: dict[str, str]) -> None:
        super().__init__()
        self.field_mapping = field_mapping

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Rename fields according to mapping."""
        results = []
        for record in data:
            new_record = {}
            for key, value in record.items():
                new_key = self.field_mapping.get(key, key)
                new_record[new_key] = value
            results.append(new_record)
        return results


class DropFieldsTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Remove fields from records."""

    def __init__(self, fields: list[str]) -> None:
        super().__init__()
        self.fields = set(fields)

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Remove specified fields from records."""
        return [
            {k: v for k, v in record.items() if k not in self.fields}
            for record in data
        ]


class TypeCastTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Cast field types."""

    def __init__(self, type_mapping: dict[str, type]) -> None:
        super().__init__()
        self.type_mapping = type_mapping

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Cast field types according to mapping."""
        results = []
        for record in data:
            new_record = record.copy()
            for field, target_type in self.type_mapping.items():
                if field in new_record and new_record[field] is not None:
                    new_record[field] = target_type(new_record[field])
            results.append(new_record)
        return results


class TimestampTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Add processing timestamp to records."""

    def __init__(self, field_name: str = "_processed_at") -> None:
        super().__init__()
        self.field_name = field_name

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add processing timestamp."""
        timestamp = datetime.utcnow().isoformat()
        return [
            {**record, self.field_name: timestamp}
            for record in data
        ]


class DeduplicateTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Remove duplicate records based on key fields."""

    def __init__(self, key_fields: list[str]) -> None:
        super().__init__()
        self.key_fields = key_fields

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Remove duplicates keeping first occurrence."""
        seen: set[tuple] = set()
        results = []

        for record in data:
            key = tuple(record.get(f) for f in self.key_fields)
            if key not in seen:
                seen.add(key)
                results.append(record)

        logger.info(
            "deduplicated",
            input_count=len(data),
            output_count=len(results),
            duplicates_removed=len(data) - len(results),
        )
        return results
