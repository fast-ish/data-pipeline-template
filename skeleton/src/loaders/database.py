{%- if values.sinkType == "postgres" or values.sinkType == "redshift" %}
"""Database loader."""

from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from src.core.config import settings
from src.core.logging import get_logger
from src.loaders.base import BaseLoader

logger = get_logger(__name__)


class DatabaseLoader(BaseLoader[dict[str, Any]]):
    """Load data to PostgreSQL or Redshift."""

    def __init__(
        self,
        table_name: str,
        connection_url: str | None = None,
        upsert_keys: list[str] | None = None,
        schema: str | None = None,
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.upsert_keys = upsert_keys
        self.schema = schema

        {%- if values.sinkType == "postgres" %}
        self.connection_url = connection_url or settings.postgres_url
        {%- else %}
        self.connection_url = connection_url or f"redshift+redshift_connector://{settings.redshift_user}:{settings.redshift_password}@{settings.redshift_host}:{settings.redshift_port}/{settings.redshift_database}"
        {%- endif %}

        self.engine = sa.create_engine(self.connection_url)
        self.metadata = sa.MetaData()
        self._table: sa.Table | None = None

    def _get_table(self) -> sa.Table:
        """Get or reflect the table."""
        if self._table is None:
            self._table = sa.Table(
                self.table_name,
                self.metadata,
                autoload_with=self.engine,
                schema=self.schema,
            )
        return self._table

    def load(self, data: list[dict[str, Any]]) -> None:
        """Load data to database."""
        if not data:
            return

        table = self._get_table()

        with self.engine.begin() as conn:
            if self.upsert_keys:
                # Upsert (INSERT ... ON CONFLICT)
                stmt = insert(table).values(data)
                update_cols = {
                    c.name: c for c in stmt.excluded if c.name not in self.upsert_keys
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=self.upsert_keys,
                    set_=update_cols,
                )
                conn.execute(stmt)
            else:
                # Simple insert
                conn.execute(table.insert(), data)

        logger.info(
            "database_loaded",
            table=self.table_name,
            records=len(data),
            upsert=bool(self.upsert_keys),
        )

    def close(self) -> None:
        """Dispose engine."""
        self.engine.dispose()
{%- endif %}


{%- if values.sinkType == "snowflake" %}
"""Snowflake loader."""

from typing import Any

import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

from src.core.config import settings
from src.core.logging import get_logger
from src.loaders.base import BaseLoader

logger = get_logger(__name__)


class SnowflakeLoader(BaseLoader[dict[str, Any]]):
    """Load data to Snowflake."""

    def __init__(
        self,
        table_name: str,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.database = database or settings.snowflake_database
        self.schema = schema or settings.snowflake_schema
        self.warehouse = warehouse or settings.snowflake_warehouse

        self.conn = connect(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )

    def load(self, data: list[dict[str, Any]]) -> None:
        """Load data to Snowflake table."""
        if not data:
            return

        df = pd.DataFrame(data)

        success, nchunks, nrows, _ = write_pandas(
            self.conn,
            df,
            table_name=self.table_name.upper(),
            database=self.database,
            schema=self.schema,
        )

        logger.info(
            "snowflake_loaded",
            table=self.table_name,
            success=success,
            chunks=nchunks,
            rows=nrows,
        )

    def close(self) -> None:
        """Close Snowflake connection."""
        self.conn.close()
{%- endif %}


{%- if values.sinkType == "bigquery" %}
"""BigQuery loader."""

from typing import Any

from google.cloud import bigquery

from src.core.config import settings
from src.core.logging import get_logger
from src.loaders.base import BaseLoader

logger = get_logger(__name__)


class BigQueryLoader(BaseLoader[dict[str, Any]]):
    """Load data to BigQuery."""

    def __init__(
        self,
        table_name: str,
        project: str | None = None,
        dataset: str | None = None,
    ) -> None:
        super().__init__()
        self.project = project or settings.bigquery_project
        self.dataset = dataset or settings.bigquery_dataset
        self.table_name = table_name

        self.client = bigquery.Client(project=self.project)
        self.table_ref = f"{self.project}.{self.dataset}.{self.table_name}"

    def load(self, data: list[dict[str, Any]]) -> None:
        """Load data to BigQuery table."""
        if not data:
            return

        errors = self.client.insert_rows_json(self.table_ref, data)

        if errors:
            logger.error("bigquery_insert_errors", errors=errors)
            raise Exception(f"BigQuery insert errors: {errors}")

        logger.info("bigquery_loaded", table=self.table_ref, records=len(data))

    def close(self) -> None:
        """Close BigQuery client."""
        self.client.close()
{%- endif %}


{%- if values.sinkType == "elasticsearch" %}
"""Elasticsearch loader."""

from typing import Any

from elasticsearch import Elasticsearch, helpers

from src.core.config import settings
from src.core.logging import get_logger
from src.loaders.base import BaseLoader

logger = get_logger(__name__)


class ElasticsearchLoader(BaseLoader[dict[str, Any]]):
    """Load data to Elasticsearch/OpenSearch."""

    def __init__(
        self,
        index: str | None = None,
        host: str | None = None,
        id_field: str | None = None,
    ) -> None:
        super().__init__()
        self.index = index or settings.elasticsearch_index
        self.id_field = id_field

        self.client = Elasticsearch(host or settings.elasticsearch_host)

    def load(self, data: list[dict[str, Any]]) -> None:
        """Load data to Elasticsearch index."""
        if not data:
            return

        actions = []
        for record in data:
            action = {
                "_index": self.index,
                "_source": record,
            }
            if self.id_field and self.id_field in record:
                action["_id"] = record[self.id_field]
            actions.append(action)

        success, errors = helpers.bulk(self.client, actions, raise_on_error=False)

        if errors:
            logger.error("elasticsearch_errors", error_count=len(errors))

        logger.info(
            "elasticsearch_loaded",
            index=self.index,
            success=success,
            errors=len(errors) if errors else 0,
        )

    def close(self) -> None:
        """Close Elasticsearch client."""
        self.client.close()
{%- endif %}


{%- if values.sinkType == "dynamodb" %}
"""DynamoDB loader."""

from typing import Any
from decimal import Decimal

import boto3

from src.core.config import settings
from src.core.logging import get_logger
from src.loaders.base import BaseLoader

logger = get_logger(__name__)


class DynamoDBLoader(BaseLoader[dict[str, Any]]):
    """Load data to DynamoDB."""

    def __init__(
        self,
        table_name: str,
    ) -> None:
        super().__init__()
        self.table_name = table_name

        dynamodb = boto3.resource("dynamodb", region_name=settings.aws_region)
        self.table = dynamodb.Table(table_name)

    def _convert_floats(self, obj: Any) -> Any:
        """Convert floats to Decimals for DynamoDB."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self._convert_floats(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_floats(i) for i in obj]
        return obj

    def load(self, data: list[dict[str, Any]]) -> None:
        """Load data to DynamoDB table using batch writer."""
        if not data:
            return

        with self.table.batch_writer() as batch:
            for record in data:
                item = self._convert_floats(record)
                batch.put_item(Item=item)

        logger.info("dynamodb_loaded", table=self.table_name, records=len(data))
{%- endif %}
