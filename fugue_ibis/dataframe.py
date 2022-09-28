from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from fugue import DataFrame, IterableDataFrame, LocalDataFrame
from fugue.dataframe.dataframe import _input_schema
from fugue.exceptions import FugueDataFrameEmptyError, FugueDataFrameOperationError
from triad import Schema

from ._compat import IbisTable
from ._utils import _pa_to_ibis_type, to_schema


class IbisDataFrame(DataFrame):
    """DataFrame that wraps Ibis ``Table``.

    :param rel: ``DuckDBPyRelation`` object
    :param metadata: dict-like object with string keys, default ``None``
    """

    def __init__(self, table: IbisTable, schema: Any = None, metadata: Any = None):
        self._table = table
        _schema = to_schema(table.schema())
        if schema is not None:
            _to_schema = _input_schema(schema).assert_not_empty()
            if _to_schema != schema:
                table = self._alter_table_columns(table, _schema, _to_schema)
                _schema = _to_schema
        self._table = table
        super().__init__(schema=_schema, metadata=metadata)

    @property
    def native(self) -> IbisTable:
        """Ibis Table object"""
        return self._table

    def _to_local_df(self, table: IbisTable, metadata: Any = None) -> LocalDataFrame:
        raise NotImplementedError  # pragma: no cover

    def _to_iterable_df(
        self, table: IbisTable, metadata: Any = None
    ) -> IterableDataFrame:
        raise NotImplementedError  # pragma: no cover

    def _to_new_df(self, table: IbisTable, metadata: Any = None) -> DataFrame:
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

    def peek_array(self) -> Any:
        res = self._to_local_df(self._table.head(1)).as_array()
        if len(res) == 0:
            raise FugueDataFrameEmptyError()
        return res[0]

    def count(self) -> int:
        return self._compute_scalar(self._table.count())

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        schema = self.schema.exclude(cols)
        return self._to_new_df(self._table[schema.names])

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        schema = self.schema.extract(keys)
        return self._to_new_df(self._table[schema.names])

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            schema = self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        cols: List[Any] = []
        for a, b in zip(self.schema.names, schema.names):
            if a == b:
                cols.append(self._table[a])
            else:
                cols.append(self._table[a].name(b))
        return self._to_new_df(self._table.projection(cols))

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        return self._to_new_df(
            self._alter_table_columns(self._table, self.schema, new_schema)
        )

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        return self._to_local_df(self._table).as_arrow(type_safe=type_safe)

    def as_pandas(self) -> pd.DataFrame:
        return self._to_local_df(self._table).as_pandas()

    def as_local(self) -> LocalDataFrame:
        return self._to_local_df(self._table, self.metadata)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        if columns is not None:
            return self[columns].as_array(type_safe=type_safe)
        return self._to_local_df(self._table).as_array(type_safe=type_safe)

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        if columns is not None:
            yield from self[columns].as_array_iterable(type_safe=type_safe)
        else:
            yield from self._to_iterable_df(self._table).as_array_iterable(
                type_safe=type_safe
            )

    def head(self, n: int, columns: Optional[List[str]] = None) -> List[Any]:
        if columns is not None:
            return self[columns].head(n)
        return self._to_local_df(self._table.head(n)).as_array(type_safe=True)

    def _alter_table_columns(
        self, table: IbisTable, schema: Schema, new_schema: Schema
    ):
        fields: Dict[str, Any] = {}
        for f1, f2 in zip(schema.fields, new_schema.fields):
            if f1.type != f2.type:
                fields[f1.name] = self._table[f1.name].cast(_pa_to_ibis_type(f2.type))
        return table.mutate(**fields)
