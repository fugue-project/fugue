from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from duckdb import DuckDBPyRelation
from triad import Schema
from triad.utils.pyarrow import LARGE_TYPES_REPLACEMENT, replace_types_in_table

from fugue import ArrayDataFrame, ArrowDataFrame, DataFrame, LocalBoundedDataFrame
from fugue.exceptions import FugueDataFrameOperationError, FugueDatasetEmptyError
from fugue.plugins import (
    as_arrow,
    as_fugue_dataset,
    as_local_bounded,
    get_column_names,
    get_num_partitions,
    get_schema,
    is_df,
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
        cols = [col for col in self._rel.columns if col not in cols]
        rel = self._rel.project(",".join(encode_column_name(n) for n in cols))
        return DuckDataFrame(rel)

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        rel = self._rel.project(",".join(encode_column_name(n) for n in keys))
        return DuckDataFrame(rel)

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
        new_schema = self._get_altered_schema(columns)
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
        if any(pa.types.is_nested(f.type) for f in self.schema.fields):
            # Duckdb has issue to directly convert nested types to pandas
            return ArrowDataFrame(self.as_arrow()).as_pandas()
        return self._rel.to_df()

    def as_local_bounded(self) -> LocalBoundedDataFrame:
        res = ArrowDataFrame(self.as_arrow())
        if self.has_metadata:
            res.reset_metadata(self.metadata)
        return res

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        if columns is not None:
            return self[columns].as_array(type_safe=type_safe)
        return self._fetchall(self._rel)

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        if columns is not None:
            yield from self[columns].as_array_iterable(type_safe=type_safe)
        else:
            yield from self._fetchall(self._rel)

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        if columns is not None:
            return self[columns].head(n)
        return ArrayDataFrame(self._fetchall(self._rel.limit(n)), schema=self.schema)

    def _fetchall(self, rel: DuckDBPyRelation) -> List[List[Any]]:
        map_pos = [i for i, t in enumerate(self.schema.types) if pa.types.is_map(t)]
        if len(map_pos) == 0:
            return [list(x) for x in rel.fetchall()]
        else:

            def to_list(row: Any) -> List[Any]:
                res = list(row)
                for p in map_pos:
                    res[p] = list(zip(row[p]["key"], row[p]["value"]))
                return res

            return [to_list(x) for x in rel.fetchall()]


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
    _df = df.arrow()
    _df = replace_types_in_table(_df, LARGE_TYPES_REPLACEMENT, recursive=True)
    return _df


@get_schema.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _duck_get_schema(df: DuckDBPyRelation) -> Schema:
    return Schema([pa.field(x, to_pa_type(y)) for x, y in zip(df.columns, df.types)])


@get_column_names.candidate(lambda df: isinstance(df, DuckDBPyRelation))
def _get_duckdb_columns(df: DuckDBPyRelation) -> List[Any]:
    return list(df.columns)


def _assert_no_missing(df: DuckDBPyRelation, columns: Iterable[Any]) -> None:
    missing = set(columns) - set(df.columns)
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")
