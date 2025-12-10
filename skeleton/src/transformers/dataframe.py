"""DataFrame-based transformers."""

from typing import Any, Callable

{%- if values.computeEngine == "polars" %}
import polars as pl
{%- elif values.computeEngine == "pandas" %}
import pandas as pd
{%- elif values.computeEngine == "spark" %}
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
{%- elif values.computeEngine == "dask" %}
import dask.dataframe as dd
import pandas as pd
{%- endif %}

from src.core.logging import get_logger
from src.transformers.base import BaseTransformer

logger = get_logger(__name__)


{%- if values.computeEngine == "polars" %}


class PolarsTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Transform data using Polars."""

    def __init__(
        self,
        transformations: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
    ) -> None:
        super().__init__()
        self.transformations = transformations

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform data using Polars DataFrame operations."""
        df = pl.DataFrame(data)

        if self.transformations:
            df = self.transformations(df)

        return df.to_dicts()


class PolarsFilterTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Filter data using Polars expressions."""

    def __init__(self, filter_expr: pl.Expr) -> None:
        super().__init__()
        self.filter_expr = filter_expr

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter records matching the expression."""
        df = pl.DataFrame(data)
        filtered = df.filter(self.filter_expr)
        return filtered.to_dicts()


class PolarsSelectTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Select and rename columns using Polars."""

    def __init__(self, columns: list[str] | dict[str, str]) -> None:
        super().__init__()
        self.columns = columns

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Select or rename columns."""
        df = pl.DataFrame(data)

        if isinstance(self.columns, list):
            df = df.select(self.columns)
        else:
            df = df.select([pl.col(old).alias(new) for old, new in self.columns.items()])

        return df.to_dicts()
{%- endif %}


{%- if values.computeEngine == "pandas" %}


class PandasTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Transform data using Pandas."""

    def __init__(
        self,
        transformations: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    ) -> None:
        super().__init__()
        self.transformations = transformations

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform data using Pandas DataFrame operations."""
        df = pd.DataFrame(data)

        if self.transformations:
            df = self.transformations(df)

        return df.to_dict("records")


class PandasFilterTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Filter data using Pandas query."""

    def __init__(self, query: str) -> None:
        super().__init__()
        self.query = query

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter records matching the query."""
        df = pd.DataFrame(data)
        filtered = df.query(self.query)
        return filtered.to_dict("records")


class PandasSelectTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Select and rename columns using Pandas."""

    def __init__(self, columns: list[str] | dict[str, str]) -> None:
        super().__init__()
        self.columns = columns

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Select or rename columns."""
        df = pd.DataFrame(data)

        if isinstance(self.columns, list):
            df = df[self.columns]
        else:
            df = df[list(self.columns.keys())].rename(columns=self.columns)

        return df.to_dict("records")
{%- endif %}


{%- if values.computeEngine == "spark" %}


class SparkTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Transform data using PySpark."""

    def __init__(
        self,
        spark: SparkSession | None = None,
        transformations: Callable[[DataFrame], DataFrame] | None = None,
    ) -> None:
        super().__init__()
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.transformations = transformations

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform data using Spark DataFrame operations."""
        df = self.spark.createDataFrame(data)

        if self.transformations:
            df = self.transformations(df)

        return [row.asDict() for row in df.collect()]


class SparkSQLTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Transform data using Spark SQL."""

    def __init__(
        self,
        sql: str,
        table_name: str = "data",
        spark: SparkSession | None = None,
    ) -> None:
        super().__init__()
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.sql = sql
        self.table_name = table_name

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform data using SQL query."""
        df = self.spark.createDataFrame(data)
        df.createOrReplaceTempView(self.table_name)

        result = self.spark.sql(self.sql)
        return [row.asDict() for row in result.collect()]
{%- endif %}


{%- if values.computeEngine == "dask" %}


class DaskTransformer(BaseTransformer[dict[str, Any], dict[str, Any]]):
    """Transform data using Dask for parallel processing."""

    def __init__(
        self,
        transformations: Callable[[dd.DataFrame], dd.DataFrame] | None = None,
        npartitions: int = 4,
    ) -> None:
        super().__init__()
        self.transformations = transformations
        self.npartitions = npartitions

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform data using Dask DataFrame operations."""
        pdf = pd.DataFrame(data)
        ddf = dd.from_pandas(pdf, npartitions=self.npartitions)

        if self.transformations:
            ddf = self.transformations(ddf)

        return ddf.compute().to_dict("records")
{%- endif %}
