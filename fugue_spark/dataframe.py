from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
import pyspark.sql as ps
from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    IterableDataFrame,
    LocalBoundedDataFrame,
    LocalDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.utils import (
    get_dataframe_column_names,
    rename_dataframe_column_names,
)
from fugue.exceptions import FugueDataFrameOperationError
from pyspark.sql.functions import col
from triad import SerializableRLock
from triad.collections.schema import SchemaError
from triad.utils.assertion import assert_or_throw

from fugue_spark._utils.convert import to_cast_expression, to_schema, to_type_safe_input


@get_dataframe_column_names.candidate(lambda df: isinstance(df, ps.DataFrame))
def _get_spark_dataframe_columns(df: ps.DataFrame) -> List[Any]:
    return [f.name for f in df.schema]


@rename_dataframe_column_names.candidate(
    lambda df, *args, **kwargs: isinstance(df, ps.DataFrame)
)
def _rename_spark_dataframe(df: ps.DataFrame, names: Dict[str, Any]) -> ps.DataFrame:
    if len(names) == 0:
        return df
    cols: List[ps.Column] = []
    for f in df.schema:
        c = col(f.name)
        if f.name in names:
            c = c.alias(names[f.name])
        cols.append(c)
    return df.select(cols)


class SparkDataFrame(DataFrame):
    """DataFrame that wraps Spark DataFrame. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: :class:`spark:pyspark.sql.DataFrame`
    :param schema: |SchemaLikeObject| or :class:`spark:pyspark.sql.types.StructType`,
      defaults to None.

    .. note::

        * You should use :meth:`fugue_spark.execution_engine.SparkExecutionEngine.to_df`
          instead of construction it by yourself.
        * If ``schema`` is set, then there will be type cast on the Spark DataFrame if
          the schema is different.
    """

    def __init__(self, df: Any = None, schema: Any = None):  # noqa: C901
        self._lock = SerializableRLock()
        if isinstance(df, ps.DataFrame):
            if schema is not None:
                schema = to_schema(schema).assert_not_empty()
                has_cast, expr = to_cast_expression(df, schema, True)
                if has_cast:
                    df = df.selectExpr(*expr)
            else:
                schema = to_schema(df).assert_not_empty()
            self._native = df
            super().__init__(schema)
        else:  # pragma: no cover
            assert_or_throw(schema is not None, SchemaError("schema is None"))
            schema = to_schema(schema).assert_not_empty()
            raise ValueError(f"{df} is incompatible with SparkDataFrame")

    @property
    def native(self) -> ps.DataFrame:
        """The wrapped Spark DataFrame

        :rtype: :class:`spark:pyspark.sql.DataFrame`
        """
        return self._native

    @property
    def is_local(self) -> bool:
        return False

    @property
    def is_bounded(self) -> bool:
        return True

    def as_local(self) -> LocalDataFrame:
        if any(pa.types.is_nested(t) for t in self.schema.types):
            data = list(to_type_safe_input(self.native.collect(), self.schema))
            return ArrayDataFrame(data, self.schema)
        return PandasDataFrame(self.native.toPandas(), self.schema)

    @property
    def num_partitions(self) -> int:
        return self.native.rdd.getNumPartitions()

    @property
    def empty(self) -> bool:
        return self._first is None

    def peek_array(self) -> List[Any]:
        self.assert_not_empty()
        return self._first  # type: ignore

    def count(self) -> int:
        with self._lock:
            if "_df_count" not in self.__dict__:
                self._df_count = self.native.count()
            return self._df_count

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        cols = (self.schema - cols).names
        return self._select_cols(cols)

    def _select_cols(self, cols: List[Any]) -> DataFrame:
        schema = self.schema.extract(cols)
        return SparkDataFrame(self.native[schema.names])

    def as_pandas(self) -> pd.DataFrame:
        return self.native.toPandas()

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        return SparkDataFrame(_rename_spark_dataframe(self.native, columns))

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        return SparkDataFrame(self.native, new_schema)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        sdf = self._select_columns(columns)
        return sdf.as_local().as_array(type_safe=type_safe)

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        sdf = self._select_columns(columns)
        if not type_safe:
            for row in to_type_safe_input(sdf.native.rdd.toLocalIterator(), sdf.schema):
                yield row
        else:
            df = IterableDataFrame(sdf.as_array_iterable(type_safe=False), sdf.schema)
            for row in df.as_array_iterable(type_safe=True):
                yield row

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        sdf = self._select_columns(columns)
        return SparkDataFrame(
            sdf.native.limit(n), sdf.schema
        ).as_local()  # type: ignore

    @property
    def _first(self) -> Optional[List[Any]]:
        with self._lock:
            if "_first_row" not in self.__dict__:
                self._first_row = self.native.first()
                if self._first_row is not None:
                    self._first_row = list(self._first_row)  # type: ignore
            return self._first_row  # type: ignore

    def _select_columns(self, columns: Optional[List[str]]) -> "SparkDataFrame":
        if columns is None:
            return self
        return SparkDataFrame(self.native.select(*columns))
