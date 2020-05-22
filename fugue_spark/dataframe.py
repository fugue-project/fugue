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
from fugue_spark.utils.convert import to_cast_expression, to_schema
from pyspark.sql import Row
from triad.collections import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw


class SparkDataFrame(DataFrame):
    def __init__(  # noqa: C901
        self, df: Any = None, schema: Any = None, metadata: Any = None
    ):
        self._lock = RLock()
        if isinstance(df, ps.DataFrame):
            if schema is not None:
                has_cast, expr = to_cast_expression(df, schema, True)
                if has_cast:
                    df = df.selectExpr(*expr)
                schema = to_schema(schema)
            else:
                schema = to_schema(df)
            self._native = df
            super().__init__(schema, metadata)
        else:
            raise ValueError(f"{df} is incompatible with PandasDataFrame")

    @property
    def native(self) -> ps.DataFrame:
        return self._native

    @property
    def is_local(self) -> bool:
        return False

    @property
    def is_bounded(self) -> bool:
        return True

    def as_local(self) -> LocalDataFrame:
        if any(pa.types.is_nested(t) for t in self.schema.types):
            data = list(self._convert(self.native.collect(), self.schema))
            return ArrayDataFrame(data, self.schema)
        return PandasDataFrame(self.native.toPandas(), self.schema)

    @property
    def num_partitions(self) -> int:
        return self.native.rdd.getNumPartitions()

    @property
    def empty(self) -> bool:
        return self._first is None

    def peek_array(self) -> List[Any]:
        if self._first is None:
            raise InvalidOperationError("Dataframe is empty, can't peek_array")
        return self._first

    def count(self) -> int:
        with self._lock:
            if "_df_count" not in self.__dict__:
                self._df_count = self.native.count()
            return self._df_count

    def as_pandas(self) -> pd.DataFrame:
        return self.native.toPandas()

    def drop(self, cols: List[str]) -> "SparkDataFrame":
        assert_or_throw(
            cols in self.schema,
            InvalidOperationError(f"{cols} not all in {self.schema}"),
        )
        assert_or_throw(
            len(cols) < len(self.schema),
            InvalidOperationError(f"can't drop all columns {self.schema}"),
        )
        return SparkDataFrame(self.native.drop(*cols))

    def rename(self, columns: Dict[str, str]) -> "SparkDataFrame":
        df = self.native
        for o, n in columns.items():
            df = df.withColumnRenamed(o, n)
        return SparkDataFrame(df)

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
            for row in self._convert(sdf.native.rdd.toLocalIterator(), sdf.schema):
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

    def _convert(self, rows: Iterable[Row], schema: Schema) -> Iterable[List[Any]]:
        idx = [p for p, t in enumerate(schema.types) if pa.types.is_struct(t)]
        if len(idx) == 0:
            for row in rows:
                yield list(row)
        else:
            for row in rows:
                r = list(row)
                for i in idx:
                    if r[i] is not None:
                        r[i] = r[i].asDict()
                yield r
