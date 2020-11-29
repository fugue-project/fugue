from threading import RLock
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
import pyspark.sql as ps
from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    IterableDataFrame,
    LocalDataFrame,
    PandasDataFrame,
)
from fugue.exceptions import FugueDataFrameInitError, FugueDataFrameOperationError
from fugue_spark._utils.convert import to_cast_expression, to_schema, to_type_safe_input
from triad.collections.schema import SchemaError
from triad.utils.assertion import assert_or_throw


class SparkDataFrame(DataFrame):
    """DataFrame that wraps Spark DataFrame. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: :class:`spark:pyspark.sql.DataFrame`
    :param schema: |SchemaLikeObject| or :class:`spark:pyspark.sql.types.StructType`,
      defaults to None.
    :param metadata: |ParamsLikeObject|, defaults to None

    :raises FugueDataFrameInitError: if the input is not compatible

    :Notice:

    * You should use :meth:`fugue_spark.execution_engine.SparkExecutionEngine.to_df`
      instead of construction it by yourself.
    * If ``schema`` is set, then there will be type cast on the Spark DataFrame if the
      schema is different.
    """

    def __init__(  # noqa: C901
        self, df: Any = None, schema: Any = None, metadata: Any = None
    ):
        self._lock = RLock()
        try:
            if isinstance(df, ps.DataFrame):
                if schema is not None:
                    schema = to_schema(schema).assert_not_empty()
                    has_cast, expr = to_cast_expression(df, schema, True)
                    if has_cast:
                        df = df.selectExpr(*expr)
                else:
                    schema = to_schema(df).assert_not_empty()
                self._native = df
                super().__init__(schema, metadata)
            else:  # pragma: no cover
                assert_or_throw(schema is not None, SchemaError("schema is None"))
                schema = to_schema(schema).assert_not_empty()
                raise ValueError(f"{df} is incompatible with SparkDataFrame")
        except Exception as e:
            raise FugueDataFrameInitError(e)

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
        # TODO: does it make sense to also include the metadata?
        if any(pa.types.is_nested(t) for t in self.schema.types):
            data = list(to_type_safe_input(self.native.collect(), self.schema))
            return ArrayDataFrame(data, self.schema, self.metadata)
        return PandasDataFrame(self.native.toPandas(), self.schema, self.metadata)

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
            raise FugueDataFrameOperationError(e)
        df = self.native
        for o, n in columns.items():
            df = df.withColumnRenamed(o, n)
        return SparkDataFrame(df)

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        return SparkDataFrame(self.native, new_schema)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        sdf = self._withColumns(columns)
        return sdf.as_local().as_array(type_safe=type_safe)

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        sdf = self._withColumns(columns)
        if not type_safe:
            for row in to_type_safe_input(sdf.native.rdd.toLocalIterator(), sdf.schema):
                yield row
        else:
            df = IterableDataFrame(sdf.as_array_iterable(type_safe=False), sdf.schema)
            for row in df.as_array_iterable(type_safe=True):
                yield row

    def head(self, n: int, columns: Optional[List[str]] = None) -> List[Any]:
        sdf = self._withColumns(columns)
        df = SparkDataFrame(sdf.native.limit(n), sdf.schema)
        return df.as_array(type_safe=True)

    @property
    def _first(self) -> Optional[List[Any]]:
        with self._lock:
            if "_first_row" not in self.__dict__:
                self._first_row = self.native.first()
                if self._first_row is not None:
                    self._first_row = list(self._first_row)
            return self._first_row

    def _withColumns(self, columns: Optional[List[str]]) -> "SparkDataFrame":
        if columns is None:
            return self
        return SparkDataFrame(self.native.select(*columns))
