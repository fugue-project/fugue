from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
import pyspark.sql as ps
from pyspark.sql.functions import col
from triad import SerializableRLock
from triad.collections.schema import SchemaError
from triad.utils.assertion import assert_or_throw

from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    IterableDataFrame,
    LocalBoundedDataFrame,
    PandasDataFrame,
)
from fugue.exceptions import FugueDataFrameOperationError
from fugue.plugins import (
    as_local_bounded,
    count,
    drop_columns,
    get_column_names,
    get_num_partitions,
    head,
    is_bounded,
    is_df,
    is_empty,
    is_local,
    rename,
    select_columns,
)

from ._utils.convert import to_cast_expression, to_pandas, to_schema, to_type_safe_input
from ._utils.misc import is_spark_connect, is_spark_dataframe


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
        if is_spark_dataframe(df):
            if schema is not None:
                schema = to_schema(schema).assert_not_empty()
                has_cast, expr = to_cast_expression(df, schema, True)
                if has_cast:
                    df = df.selectExpr(*expr)  # type: ignore
            else:
                schema = to_schema(df).assert_not_empty()
            self._native = df
            super().__init__(schema)
        else:  # pragma: no cover
            assert_or_throw(schema is not None, SchemaError("schema is None"))
            schema = to_schema(schema).assert_not_empty()
            raise ValueError(f"{df} is incompatible with SparkDataFrame")

    @property
    def alias(self) -> str:
        return "_" + str(id(self.native))

    @property
    def native(self) -> ps.DataFrame:
        """The wrapped Spark DataFrame

        :rtype: :class:`spark:pyspark.sql.DataFrame`
        """
        return self._native

    def native_as_df(self) -> ps.DataFrame:
        return self._native

    @property
    def is_local(self) -> bool:
        return False

    @property
    def is_bounded(self) -> bool:
        return True

    def as_local_bounded(self) -> LocalBoundedDataFrame:
        if any(pa.types.is_nested(t) for t in self.schema.types):
            data = list(to_type_safe_input(self.native.collect(), self.schema))
            res: LocalBoundedDataFrame = ArrayDataFrame(data, self.schema)
        else:
            res = PandasDataFrame(self.as_pandas(), self.schema)
        if self.has_metadata:
            res.reset_metadata(self.metadata)
        return res

    @property
    def num_partitions(self) -> int:
        return _spark_num_partitions(self.native)

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
        return to_pandas(self.native)

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
        if is_spark_connect(self.native):  # pragma: no cover
            yield from self.as_array(columns, type_safe=type_safe)
            return
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


@is_df.candidate(lambda df: is_spark_dataframe(df))
def _spark_is_df(df: ps.DataFrame) -> bool:
    return True


@get_num_partitions.candidate(lambda df: is_spark_dataframe(df))
def _spark_num_partitions(df: ps.DataFrame) -> int:
    return df.rdd.getNumPartitions()


@count.candidate(lambda df: is_spark_dataframe(df))
def _spark_df_count(df: ps.DataFrame) -> int:
    return df.count()


@is_bounded.candidate(lambda df: is_spark_dataframe(df))
def _spark_df_is_bounded(df: ps.DataFrame) -> bool:
    return True


@is_empty.candidate(lambda df: is_spark_dataframe(df))
def _spark_df_is_empty(df: ps.DataFrame) -> bool:
    return df.first() is None


@is_local.candidate(lambda df: is_spark_dataframe(df))
def _spark_df_is_local(df: ps.DataFrame) -> bool:
    return False


@as_local_bounded.candidate(lambda df: is_spark_dataframe(df))
def _spark_df_as_local(df: ps.DataFrame) -> pd.DataFrame:
    return to_pandas(df)


@get_column_names.candidate(lambda df: is_spark_dataframe(df))
def _get_spark_df_columns(df: ps.DataFrame) -> List[Any]:
    return df.columns


@rename.candidate(lambda df, *args, **kwargs: is_spark_dataframe(df))
def _rename_spark_df(
    df: ps.DataFrame, columns: Dict[str, Any], as_fugue: bool = False
) -> ps.DataFrame:
    if len(columns) == 0:
        return df
    _assert_no_missing(df, columns.keys())
    return _adjust_df(_rename_spark_dataframe(df, columns), as_fugue=as_fugue)


@drop_columns.candidate(lambda df, *args, **kwargs: is_spark_dataframe(df))
def _drop_spark_df_columns(
    df: ps.DataFrame, columns: List[str], as_fugue: bool = False
) -> Any:
    cols = [x for x in df.columns if x not in columns]
    if len(cols) == 0:
        raise FugueDataFrameOperationError("cannot drop all columns")
    if len(cols) + len(columns) != len(df.columns):
        _assert_no_missing(df, columns)
    return _adjust_df(df[cols], as_fugue=as_fugue)


@select_columns.candidate(lambda df, *args, **kwargs: is_spark_dataframe(df))
def _select_spark_df_columns(
    df: ps.DataFrame, columns: List[Any], as_fugue: bool = False
) -> Any:
    if len(columns) == 0:
        raise FugueDataFrameOperationError("must select at least one column")
    _assert_no_missing(df, columns)
    return _adjust_df(df[columns], as_fugue=as_fugue)


@head.candidate(lambda df, *args, **kwargs: is_spark_dataframe(df))
def _spark_df_head(
    df: ps.DataFrame,
    n: int,
    columns: Optional[List[str]] = None,
    as_fugue: bool = False,
) -> pd.DataFrame:
    if columns is not None:
        df = df[columns]
    res = df.limit(n)
    return SparkDataFrame(res).as_local() if as_fugue else to_pandas(res)


def _rename_spark_dataframe(df: ps.DataFrame, names: Dict[str, Any]) -> ps.DataFrame:
    cols: List[ps.Column] = []
    for f in df.schema:
        c = col(f.name)
        if f.name in names:
            c = c.alias(names[f.name])
        cols.append(c)
    return df.select(cols)


def _assert_no_missing(df: ps.DataFrame, columns: Iterable[Any]) -> None:
    missing = set(columns) - set(df.columns)
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")


def _adjust_df(res: ps.DataFrame, as_fugue: bool):
    return res if not as_fugue else SparkDataFrame(res)
