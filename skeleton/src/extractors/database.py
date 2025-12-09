{%- if values.sourceType == "postgres" or values.sourceType == "redshift" %}
"""Database extractor."""

from collections.abc import Iterator
from typing import Any

import sqlalchemy as sa
from sqlalchemy import text

from src.core.config import settings
from src.core.logging import get_logger
from src.extractors.base import BaseExtractor

logger = get_logger(__name__)


class DatabaseExtractor(BaseExtractor[dict[str, Any]]):
    """Extract data from PostgreSQL or Redshift."""

    def __init__(
        self,
        query: str,
        connection_url: str | None = None,
        batch_size: int = 1000,
    ) -> None:
        super().__init__(batch_size=batch_size)
        self.query = query
        {%- if values.sourceType == "postgres" %}
        self.connection_url = connection_url or settings.postgres_url
        {%- else %}
        self.connection_url = connection_url or f"redshift+redshift_connector://{settings.redshift_user}:{settings.redshift_password}@{settings.redshift_host}:{settings.redshift_port}/{settings.redshift_database}"
        {%- endif %}
        self.engine = sa.create_engine(self.connection_url)

    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract data from database using server-side cursor."""
        logger.info("database_query_start", query=self.query[:100])

        with self.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(text(self.query))

            batch: list[dict[str, Any]] = []
            columns = list(result.keys())

            for row in result:
                record = dict(zip(columns, row))
                batch.append(record)

                if len(batch) >= self.batch_size:
                    yield self._track_batch(batch)
                    batch = []

            if batch:
                yield self._track_batch(batch)

        logger.info("database_query_complete", total_records=self._records_read)
{%- endif %}
