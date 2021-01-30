from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import DataFrame, LocalDataFrame, LocalUnboundedDataFrame
from fugue.exceptions import FugueDataFrameError, FugueDataFrameInitError
from triad import Schema, assert_or_throw
from triad.utils.iter import EmptyAwareIterable, make_empty_aware


class LocalDataFrameIterableDataFrame(LocalUnboundedDataFrame):
    """DataFrame that wraps native python iterable of arrays. Please read
    |DataFrameTutorial| to understand the concept

    :param df: 2-dimensional array, iterable of arrays, or
      :class:`~fugue.dataframe.dataframe.DataFrame`
    :param schema: |SchemaLikeObject|
    :param metadata: dict-like object with string keys, default ``None``

    :raises FugueDataFrameInitError: if the input is not compatible

    :Examples:

    >>> a = IterableDataFrame([[0,'a'],[1,'b']],"a:int,b:str")
    >>> b = IterableDataFrame(a)

    :Notice:

    It's ok to peek the dataframe, it will not affect the iteration, but it's
    invalid operation to count
    """

    def __init__(  # noqa: C901
        self, df: Any = None, schema: Any = None, metadata: Any = None
    ):
        try:
            if isinstance(df, Iterable):
                self._native = make_empty_aware(self._dfs_wrapper(df))
                orig_schema: Optional[Schema] = None
                if not self._native.empty:
                    orig_schema = self._native.peek().schema
            else:
                raise ValueError(
                    f"{df} is incompatible with LocalDataFrameIterableDataFrame"
                )
            if orig_schema is None and schema is None:
                raise FugueDataFrameInitError(
                    "schema is not provided and the input is empty"
                )
            elif orig_schema is None and schema is not None:
                pass
            elif orig_schema is not None and schema is None:
                schema = orig_schema
            else:
                schema = Schema(schema) if not isinstance(schema, Schema) else schema
                assert_or_throw(
                    orig_schema == schema,
                    f"iterable schema {orig_schema} is different from {schema}",
                )
            super().__init__(schema, metadata)
        except FugueDataFrameError:
            raise
        except Exception as e:
            raise FugueDataFrameInitError(e)

    def _dfs_wrapper(self, dfs: Iterable[DataFrame]) -> Iterable[LocalDataFrame]:
        last_empty: Any = None
        last_schema: Any = None
        yielded = False
        for df in dfs:
            if df.empty:
                last_empty = df
            else:
                assert_or_throw(
                    last_schema is None or df.schema == last_schema,
                    FugueDataFrameInitError(
                        f"encountered schema {df.schema} doesn't match"
                        f" the original schema {df.schema}"
                    ),
                )
                if last_schema is None:
                    last_schema = df.schema
                yield df.as_local()
                yielded = True
        if not yielded and last_empty is not None:
            yield last_empty

    @property
    def native(self) -> EmptyAwareIterable[LocalDataFrame]:
        """Iterable of dataframes"""
        return self._native

    @property
    def empty(self) -> bool:
        return self.native.empty or self.native.peek().empty

    def peek_array(self) -> Any:
        self.assert_not_empty()
        return self.native.peek().peek_array()

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        if self.empty:
            return ArrayDataFrame([], self.schema)[keys]

        def _transform():
            for df in self.native:
                yield df[keys]

        return LocalDataFrameIterableDataFrame(_transform())

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        if self.empty:
            return ArrayDataFrame([], self.schema).rename(columns)

        def _transform() -> Iterable[DataFrame]:
            for df in self.native:
                yield df.rename(columns)

        return LocalDataFrameIterableDataFrame(_transform())

    def alter_columns(self, columns: Any) -> DataFrame:
        if self.empty:
            return ArrayDataFrame([], self.schema).alter_columns(columns)

        def _transform() -> Iterable[DataFrame]:
            for df in self.native:
                yield df.alter_columns(columns)

        return LocalDataFrameIterableDataFrame(_transform())

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return sum(
            (df.as_array(columns=columns, type_safe=type_safe) for df in self.native),
            [],
        )

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        for df in self.native:
            yield from df.as_array_iterable(columns=columns, type_safe=type_safe)

    def as_pandas(self) -> pd.DataFrame:
        if self.empty:
            return ArrayDataFrame([], self.schema).as_pandas()

        return pd.concat(df.as_pandas() for df in self.native)

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        if self.empty:
            return ArrayDataFrame([], self.schema).as_arrow()

        return pa.concat_tables(df.as_arrow() for df in self.native)

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        if self.empty:
            return ArrayDataFrame([], self.schema)._drop_cols(cols)

        def _transform() -> Iterable[DataFrame]:
            for df in self.native:
                yield df._drop_cols(cols)

        return LocalDataFrameIterableDataFrame(_transform())
