from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import DataFrame, LocalDataFrame, LocalUnboundedDataFrame
from fugue.exceptions import FugueDataFrameError, FugueDataFrameInitError
from triad import Schema, assert_or_throw
from triad.utils.iter import EmptyAwareIterable, make_empty_aware


class LocalDataFrameIterableDataFrame(LocalUnboundedDataFrame):
    """DataFrame that wraps an iterable of local dataframes

    :param df: an iterable of
      :class:`~fugue.dataframe.dataframe.DataFrame`. If any is not local,
      they will be converted to :class:`~fugue.dataframe.dataframe.LocalDataFrame`
      by :meth:`~fugue.dataframe.dataframe.DataFrame.as_local`
    :param schema: |SchemaLikeObject|, if it is provided, it must match the schema
      of the dataframes
    :param metadata: dict-like object with string keys, default ``None``

    :raises FugueDataFrameInitError: if the input is not compatible

    :Examples:

        .. code-block:: python

            def get_dfs(seq):
                yield IterableDataFrame([], "a:int,b:int")
                yield IterableDataFrame([[1, 10]], "a:int,b:int")
                yield ArrayDataFrame([], "a:int,b:str")

            df = LocalDataFrameIterableDataFrame(get_dfs())
            for subdf in df.native:
                subdf.show()

    :Notice:

    It's ok to peek the dataframe, it will not affect the iteration, but it's
    invalid to count.

    ``schema`` can be used when the iterable contains no dataframe. But if there
    is any dataframe, ``schema`` must match the schema of the dataframes.

    For the iterable of dataframes, if there is any empty dataframe, they will
    be skipped and their schema will not matter. However, if all dataframes
    in the interable are empty, then the last empty dataframe will be used to
    set the schema.
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
            raise FugueDataFrameInitError from e

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
