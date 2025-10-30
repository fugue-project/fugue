from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from duckdb import DuckDBPyRelation
from triad import Schema, assert_or_throw
from triad.utils.pyarrow import LARGE_TYPES_REPLACEMENT, replace_types_in_table

from fugue import ArrowDataFrame, DataFrame, LocalBoundedDataFrame
from fugue.dataframe.arrow_dataframe import _pa_table_as_pandas
from fugue.dataframe.utils import (
    pa_table_as_array,
    pa_table_as_array_iterable,
    pa_table_as_dict_iterable,
    pa_table_as_dicts,
)
from fugue.exceptions import FugueDataFrameOperationError, FugueDatasetEmptyError
from fugue.plugins import (
    as_array,
    as_array_iterable,
    as_arrow,
    as_dict_iterable,
    as_dicts,
    as_fugue_dataset,
    as_local_bounded,
    as_pandas,
    drop_columns,
    get_column_names,
    get_num_partitions,
    get_schema,
    is_df,
    select_columns,
)

from ._utils import encode_column_name, to_duck_type, to_pa_type


class DuckDataFrame(LocalBoundedDataFrame):
    """DataFrame that wraps DuckDB ``DuckDBPyRelation``.

    :param rel: ``DuckDBPyRelation`` object
    """

    def __init__(self, rel: DuckDBPyRelation):
        self._rel = rel
        super().__init__(schema=lambda: _duck_get_schema(self._rel))

    @property
    def alias(self) -> str:
        return "_" + str(id(self._rel))  # DuckDBPyRelation.alias is not always unique

    @property
    def native(self) -> DuckDBPyRelation:
        """DuckDB relation object"""
        return self._rel

    def native_as_df(self) -> DuckDBPyRelation:
        return self._rel

    @property
    def empty(self) -> bool:
        return self.count() == 0

    def peek_array(self) -> List[Any]:
        res = self._rel.limit(1).to_df()
        if res.shape[0] == 0:
            raise FugueDatasetEmptyError()
        return res.values.tolist()[0]

    def count(self) -> int:
        return len(self._rel)

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        return DuckDataFrame(_drop_duckdb_columns(self._rel, cols))

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        return DuckDataFrame(_select_duckdb_columns(self._rel, keys))

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        _assert_no_missing(self._rel, columns.keys())
        expr = ", ".join(
            f"{a} AS {b}"
            for a, b in [
                (encode_column_name(name), encode_column_name(columns.get(name, name)))
                for name in self._rel.columns
            ]
        )
        return DuckDataFrame(self._rel.project(expr))

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self.schema.alter(columns)
        if new_schema == self.schema:
            return self
        fields: List[str] = []
        for f1, f2 in zip(self.schema.fields, new_schema.fields):
            if f1.type == f2.type:
                fields.append(encode_column_name(f1.name))
            else:
                tp = to_duck_type(f2.type)
                fields.append(
                    f"CAST({encode_column_name(f1.name)} AS {tp}) "
                    f"AS {encode_column_name(f1.name)}"
                )
        return DuckDataFrame(self._rel.project(", ".join(fields)))

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        return _duck_as_arrow(self._rel)

    def as_pandas(self) -> pd.DataFrame:
        return _duck_as_pandas(self._rel)

    def as_local_bounded(self) -> LocalBoundedDataFrame:
        res = ArrowDataFrame(self.as_arrow())
        if self.has_metadata:
            res.reset_metadata(self.metadata)
        return res

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return _duck_as_array(self._rel, columns=columns, type_safe=type_safe)

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        yield from _duck_as_array_iterable(
            self._rel, columns=columns, type_safe=type_safe
        )

    def as_dicts(self, columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        return _duck_as_dicts(self._rel, columns=columns)

    def as_dict_iterable(
        self, columns: Optional[List[str]] = None
    ) -> Iterable[Dict[str, Any]]:
        yield from _duck_as_dict_iterable(self._rel, columns=columns)

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        if columns is not None:
            return self[columns].head(n)
        return ArrowDataFrame(_duck_as_arrow(self._rel.limit(n)))


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, DuckDBPyRelation))
def _duckdb_as_fugue_df(df: DuckDBPyRelation, **kwargs: Any) -> DuckDataFrame:
    return DuckDataFrame(df, **kwargs)


@is_df.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _duck_is_df(df: DuckDBPyRelation) -> bool:
    return True


@get_num_partitions.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _duckdb_num_partitions(df: DuckDBPyRelation) -> int:
    return 1


@as_local_bounded.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _duck_as_local(df: DuckDBPyRelation) -> DuckDBPyRelation:
    return df


@as_arrow.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _duck_as_arrow(df: DuckDBPyRelation) -> pa.Table:
    _df = df.fetch_arrow_table()
    _df = replace_types_in_table(_df, LARGE_TYPES_REPLACEMENT, recursive=True)
    return _df


@as_pandas.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _duck_as_pandas(df: DuckDBPyRelation) -> pd.DataFrame:
    adf = _duck_as_arrow(df)
    return _pa_table_as_pandas(adf)


@get_schema.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _duck_get_schema(df: DuckDBPyRelation) -> Schema:
    return Schema([pa.field(x, to_pa_type(y)) for x, y in zip(df.columns, df.types)])


@get_column_names.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _get_duckdb_columns(df: DuckDBPyRelation) -> List[Any]:
    return list(df.columns)


@select_columns.candidate(lambda df, *args, **kwargs: isinstance(df, DuckDBPyRelation))
def _select_duckdb_columns(
    df: DuckDBPyRelation, columns: List[Any]
) -> DuckDBPyRelation:
    if len(columns) == 0:
        raise FugueDataFrameOperationError("must select at least one column")
    _assert_no_missing(df, columns)
    return df.project(",".join(encode_column_name(n) for n in columns))


@drop_columns.candidate(lambda df, *args, **kwargs: isinstance(df, DuckDBPyRelation))
def _drop_duckdb_columns(df: DuckDBPyRelation, columns: List[str]) -> DuckDBPyRelation:
    # if len(columns) == 0:
    #   return df
    _columns = {c: 1 for c in columns}
    cols = [col for col in df.columns if _columns.pop(col, None) is None]
    assert_or_throw(
        len(cols) > 0, FugueDataFrameOperationError("must keep at least one column")
    )
    assert_or_throw(
        len(_columns) == 0,
        FugueDataFrameOperationError("found nonexistent columns {_columns}"),
    )
    return df.project(",".join(encode_column_name(n) for n in cols))


@as_array.candidate(lambda df, *args, **kwargs: isinstance(df, DuckDBPyRelation))
def _duck_as_array(
    df: DuckDBPyRelation, columns: Optional[List[str]] = None, type_safe: bool = False
) -> List[Any]:
    return pa_table_as_array(df.fetch_arrow_table(), columns=columns)


@as_array_iterable.candidate(
    lambda df, *args, **kwargs: isinstance(df, DuckDBPyRelation)
)
def _duck_as_array_iterable(
    df: DuckDBPyRelation, columns: Optional[List[str]] = None, type_safe: bool = False
) -> Iterable[Any]:
    yield from pa_table_as_array_iterable(df.fetch_arrow_table(), columns=columns)


@as_dicts.candidate(lambda df, *args, **kwargs: isinstance(df, DuckDBPyRelation))
def _duck_as_dicts(
    df: DuckDBPyRelation, columns: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    return pa_table_as_dicts(df.fetch_arrow_table(), columns=columns)


@as_dict_iterable.candidate(
    lambda df, *args, **kwargs: isinstance(df, DuckDBPyRelation)
)
def _duck_as_dict_iterable(
    df: DuckDBPyRelation, columns: Optional[List[str]] = None
) -> Iterable[Dict[str, Any]]:
    yield from pa_table_as_dict_iterable(df.fetch_arrow_table(), columns=columns)


def _assert_no_missing(df: DuckDBPyRelation, columns: Iterable[Any]) -> None:
    missing = set(columns) - set(df.columns)
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")
