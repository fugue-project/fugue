from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from triad import Schema

from fugue import (
    DataFrame,
    IterableDataFrame,
    LocalBoundedDataFrame,
    LocalDataFrame,
    to_local_bounded_df,
)
from fugue.dataframe.dataframe import _input_schema
from fugue.exceptions import FugueDataFrameOperationError, FugueDatasetEmptyError
from fugue.plugins import is_df, get_column_names, rename

from ._compat import IbisTable
from ._utils import _pa_to_ibis_type, to_schema


class IbisDataFrame(DataFrame):
    """DataFrame that wraps Ibis ``Table``.

    :param rel: ``DuckDBPyRelation`` object
    """

    def __init__(self, table: IbisTable, schema: Any = None):
        self._table = table
        _schema = to_schema(table.schema())
        if schema is not None:
            _to_schema = _input_schema(schema).assert_not_empty()
            if _to_schema != _schema:
                table = self._alter_table_columns(table, _schema, _to_schema)
                _schema = _to_schema
        self._table = table
        super().__init__(schema=_schema)

    @property
    def native(self) -> IbisTable:
        """Ibis Table object"""
        return self._table

    def native_as_df(self) -> IbisTable:
        return self._table

    def _to_local_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        raise NotImplementedError  # pragma: no cover

    def _to_iterable_df(
        self, table: IbisTable, sdhema: Any = None
    ) -> IterableDataFrame:
        raise NotImplementedError  # pragma: no cover

    def _to_new_df(self, table: IbisTable, schema: Any = None) -> DataFrame:
        raise NotImplementedError  # pragma: no cover

    def _compute_scalar(self, table: IbisTable) -> Any:
        return table.execute()

    @property
    def is_local(self) -> bool:
        return False

    @property
    def is_bounded(self) -> bool:
        return True

    @property
    def empty(self) -> bool:
        return self._to_local_df(self._table.limit(1)).count() == 0

    @property
    def num_partitions(self) -> int:
        return 1  # pragma: no cover

    def peek_array(self) -> List[Any]:
        res = self._to_local_df(self._table.head(1)).as_array()
        if len(res) == 0:
            raise FugueDatasetEmptyError()
        return res[0]

    def count(self) -> int:
        return self._compute_scalar(self._table.count())

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        schema = self.schema.exclude(cols)
        return self._to_new_df(self._table[schema.names], schema=schema)

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        schema = self.schema.extract(keys)
        return self._to_new_df(self._table[schema.names], schema=schema)

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            schema = self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        df = _rename(self._table, self.schema.names, schema.names)
        return self if df is self._table else self._to_new_df(df, schema=schema)

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        return self._to_new_df(
            self._alter_table_columns(self._table, self.schema, new_schema),
            schema=new_schema,
        )

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        return self.as_local().as_arrow(type_safe=type_safe)

    def as_pandas(self) -> pd.DataFrame:
        return self.as_local().as_pandas()

    def as_local(self) -> LocalDataFrame:
        res = self._to_local_df(self._table, schema=self.schema)
        if res is not self and self.has_metadata:
            res.reset_metadata(self.metadata)
        return res

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        if columns is not None:
            return self[columns].as_array(type_safe=type_safe)
        return self.as_local().as_array(type_safe=type_safe)

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        if columns is not None:
            yield from self[columns].as_array_iterable(type_safe=type_safe)
        else:
            yield from self._to_iterable_df(self._table).as_array_iterable(
                type_safe=type_safe
            )

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        if columns is not None:
            return self[columns].head(n)
        return to_local_bounded_df(self._to_local_df(self._table.head(n)))

    def _alter_table_columns(
        self, table: IbisTable, schema: Schema, new_schema: Schema
    ):
        fields: Dict[str, Any] = {}
        for f1, f2 in zip(schema.fields, new_schema.fields):
            if not self._type_equal(f1.type, f2.type):
                fields[f1.name] = self._table[f1.name].cast(_pa_to_ibis_type(f2.type))
        return table.mutate(**fields)

    def _type_equal(self, tp1: pa.DataType, tp2: pa.DataType) -> bool:
        return tp1 == tp2


@is_df.candidate(lambda df: isinstance(df, IbisTable))
def _ibis_is_df(df: IbisTable) -> bool:
    return True


@get_column_names.candidate(lambda df: isinstance(df, IbisTable))
def _get_ibis_columns(df: IbisTable) -> List[Any]:
    return df.columns


@rename.candidate(lambda df, *args, **kwargs: isinstance(df, IbisTable))
def _rename_dask_dataframe(df: IbisTable, columns: Dict[str, Any]) -> IbisTable:
    _assert_no_missing(df, columns.keys())
    old_names = df.columns
    new_names = [columns.get(name, name) for name in old_names]
    return _rename(df, old_names, new_names)


def _rename(df: IbisTable, old_names: List[str], new_names: List[str]) -> IbisTable:
    cols: List[Any] = []
    has_change = False
    for a, b in zip(old_names, new_names):
        if a == b:
            cols.append(df[a])
        else:
            cols.append(df[a].name(b))
            has_change = True
    return df.projection(cols) if has_change else df


def _assert_no_missing(df: IbisTable, columns: Iterable[Any]) -> None:
    missing = set(columns) - set(df.columns)
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")
