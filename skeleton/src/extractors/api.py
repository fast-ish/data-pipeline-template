{%- if values.sourceType == "api" %}
"""API data extractor."""

from collections.abc import Iterator
from typing import Any

import httpx

from src.core.config import settings
from src.core.logging import get_logger
from src.extractors.base import BaseExtractor

logger = get_logger(__name__)


class APIExtractor(BaseExtractor[dict[str, Any]]):
    """Extract data from REST API with pagination support."""

    def __init__(
        self,
        endpoint: str,
        base_url: str | None = None,
        api_key: str | None = None,
        batch_size: int = 100,
        page_param: str = "page",
        limit_param: str = "limit",
        data_key: str | None = "data",
        max_pages: int | None = None,
    ) -> None:
        super().__init__(batch_size=batch_size)
        self.base_url = base_url or settings.api_base_url
        self.api_key = api_key or settings.api_key
        self.endpoint = endpoint
        self.page_param = page_param
        self.limit_param = limit_param
        self.data_key = data_key
        self.max_pages = max_pages

        self.client = httpx.Client(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=30.0,
        )

    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract data from paginated API."""
        page = 1
        pages_fetched = 0

        while True:
            if self.max_pages and pages_fetched >= self.max_pages:
                break

            params = {
                self.page_param: page,
                self.limit_param: self.batch_size,
            }

            logger.info("api_request", endpoint=self.endpoint, page=page)

            response = self.client.get(self.endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract records from response
            if self.data_key:
                records = data.get(self.data_key, [])
            else:
                records = data if isinstance(data, list) else [data]

            if not records:
                logger.info("api_pagination_complete", total_pages=pages_fetched)
                break

            yield self._track_batch(records)

            page += 1
            pages_fetched += 1

    def __del__(self) -> None:
        self.client.close()
{%- endif %}
