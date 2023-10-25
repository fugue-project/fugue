from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import polars as pl
import pyarrow as pa
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.pyarrow import (
    LARGE_TYPES_REPLACEMENT,
    replace_types_in_schema,
    replace_types_in_table,
)

from fugue import ArrowDataFrame
from fugue.api import (
    as_array,
    as_array_iterable,
    as_arrow,
    as_dict_iterable,
    as_dicts,
    drop_columns,
    get_column_names,
    get_schema,
    is_df,
    is_empty,
    rename,
    select_columns,
)
from fugue.dataframe.dataframe import DataFrame, LocalBoundedDataFrame, _input_schema
from fugue.dataframe.utils import (
    pa_table_as_array,
    pa_table_as_array_iterable,
    pa_table_as_dict_iterable,
    pa_table_as_dicts,
)
from fugue.dataset.api import (
    as_local,
    as_local_bounded,
    count,
    get_num_partitions,
    is_bounded,
    is_local,
)
from fugue.exceptions import FugueDataFrameOperationError

from ._utils import build_empty_pl


class PolarsDataFrame(LocalBoundedDataFrame):
    """DataFrame that wraps :func:`pyarrow.Table <pa:pyarrow.table>`. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: polars DataFrame or None, defaults to None
    :param schema: |SchemaLikeObject|
    """

    def __init__(
        self,
        df: Optional[pl.DataFrame] = None,
        schema: Any = None,
    ):
        if df is None:
            schema = _input_schema(schema).assert_not_empty()
            self._native: pl.DataFrame = build_empty_pl(schema)
            super().__init__(schema)
            return
        else:
            assert_or_throw(
                schema is None,
                InvalidOperationError("can't reset schema for pl.DataFrame"),
            )
            self._native = df
            super().__init__(_get_pl_schema(df))

    @property
    def native(self) -> pl.DataFrame:
        """:func:`pyarrow.Table <pa:pyarrow.table>`"""
        return self._native

    def native_as_df(self) -> pl.DataFrame:
        return self._native

    @property
    def empty(self) -> bool:
        return self._native.is_empty()

    def peek_array(self) -> List[Any]:
        self.assert_not_empty()
        return list(self._native.row(0))

    def peek_dict(self) -> Dict[str, Any]:
        self.assert_not_empty()
        return self._native.row(0, named=True)

    def count(self) -> int:
        return self.native.shape[0]

    def as_pandas(self) -> pd.DataFrame:
        return self.native.to_pandas()

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        adf = self.native if columns is None else self.native.select(columns)
        n = min(n, self.count())
        if n == 0:
            schema = self.schema if columns is None else self.schema.extract(columns)
            return PolarsDataFrame(None, schema=schema)
        return PolarsDataFrame(adf.head(n))

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        return PolarsDataFrame(self.native.drop(cols))

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        return PolarsDataFrame(self.native.select(keys))

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        return PolarsDataFrame(_rename_pl_dataframe(self.native, columns))

    def alter_columns(self, columns: Any) -> DataFrame:
        adf = ArrowDataFrame(self.as_arrow()).alter_columns(columns)
        return PolarsDataFrame(pl.from_arrow(adf.native))

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        return _pl_as_arrow(self.native)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return _pl_as_array(self.native, columns=columns)

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        yield from _pl_as_array_iterable(self.native, columns=columns)

    def as_dicts(self, columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        return _pl_as_dicts(self.native, columns=columns)

    def as_dict_iterable(
        self, columns: Optional[List[str]] = None
    ) -> Iterable[Dict[str, Any]]:
        yield from _pl_as_dict_iterable(self.native, columns=columns)


@as_local.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_as_local(df: pl.DataFrame) -> pl.DataFrame:
    return df


@as_local_bounded.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_as_local_bounded(df: pl.DataFrame) -> pl.DataFrame:
    return df


@as_arrow.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_as_arrow(df: pl.DataFrame) -> pa.Table:
    adf = df.to_arrow()
    adf = replace_types_in_table(adf, LARGE_TYPES_REPLACEMENT)
    return adf


@is_df.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_is_df(df: pl.DataFrame) -> bool:
    return True


@count.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_count(df: pl.DataFrame) -> int:
    return df.shape[0]


@is_bounded.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_is_bounded(df: pl.DataFrame) -> bool:
    return True


@is_empty.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_is_empty(df: pl.DataFrame) -> bool:
    return df.is_empty()


@is_local.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_is_local(df: pl.DataFrame) -> bool:
    return True


@get_num_partitions.candidate(lambda df: isinstance(df, pl.DataFrame))
def _pl_get_num_partitions(df: pl.DataFrame) -> int:
    return 1


@get_column_names.candidate(lambda df: isinstance(df, pl.DataFrame))
def _get_pl_columns(df: pl.DataFrame) -> List[Any]:
    return list(df.schema.keys())


@get_schema.candidate(lambda df: isinstance(df, pl.DataFrame))
def _get_pl_schema(df: pl.DataFrame) -> Schema:
    adf = df.to_arrow()
    schema = replace_types_in_schema(adf.schema, LARGE_TYPES_REPLACEMENT)
    return Schema(schema)


@rename.candidate(lambda df, *args, **kwargs: isinstance(df, pl.DataFrame))
def _rename_pl_dataframe(df: pl.DataFrame, columns: Dict[str, Any]) -> pl.DataFrame:
    if len(columns) == 0:
        return df
    assert_or_throw(
        set(columns.keys()).issubset(set(df.columns)),
        FugueDataFrameOperationError(f"invalid {columns}"),
    )
    return df.rename(columns)


@drop_columns.candidate(lambda df, *args, **kwargs: isinstance(df, pl.DataFrame))
def _drop_pa_columns(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
    cols = [x for x in df.schema.keys() if x not in columns]
    if len(cols) == 0:
        raise FugueDataFrameOperationError("cannot drop all columns")
    if len(cols) + len(columns) != len(df.columns):
        _assert_no_missing(df, columns)
    return df.select(cols)


@select_columns.candidate(lambda df, *args, **kwargs: isinstance(df, pl.DataFrame))
def _select_pa_columns(df: pl.DataFrame, columns: List[Any]) -> pl.DataFrame:
    if len(columns) == 0:
        raise FugueDataFrameOperationError("must select at least one column")
    _assert_no_missing(df, columns=columns)
    return df.select(columns)


@as_array.candidate(lambda df, *args, **kwargs: isinstance(df, pl.DataFrame))
def _pl_as_array(
    df: pl.DataFrame, columns: Optional[List[str]] = None, type_safe: bool = False
) -> List[List[Any]]:
    _df = df if columns is None else _select_pa_columns(df, columns)
    adf = _pl_as_arrow(_df)
    return pa_table_as_array(adf, columns=columns)


@as_array_iterable.candidate(lambda df, *args, **kwargs: isinstance(df, pl.DataFrame))
def _pl_as_array_iterable(
    df: pl.DataFrame, columns: Optional[List[str]] = None, type_safe: bool = False
) -> Iterable[List[Any]]:
    _df = df if columns is None else _select_pa_columns(df, columns)
    yield from pa_table_as_array_iterable(_df.to_arrow(), columns=columns)


@as_dicts.candidate(lambda df, *args, **kwargs: isinstance(df, pl.DataFrame))
def _pl_as_dicts(
    df: pl.DataFrame, columns: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    _df = df if columns is None else _select_pa_columns(df, columns)
    return pa_table_as_dicts(_df.to_arrow(), columns=columns)


@as_dict_iterable.candidate(lambda df, *args, **kwargs: isinstance(df, pl.DataFrame))
def _pl_as_dict_iterable(
    df: pl.DataFrame, columns: Optional[List[str]] = None
) -> Iterable[Dict[str, Any]]:
    _df = df if columns is None else _select_pa_columns(df, columns)
    yield from pa_table_as_dict_iterable(_df.to_arrow(), columns=columns)


def _assert_no_missing(df: pl.DataFrame, columns: Iterable[Any]) -> None:
    missing = [x for x in columns if x not in df.schema.keys()]
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")
