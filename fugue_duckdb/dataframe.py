from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from duckdb import DuckDBPyRelation
from fugue import ArrowDataFrame, DataFrame, LocalBoundedDataFrame
from fugue.exceptions import FugueDataFrameEmptyError, FugueDataFrameOperationError
from triad import Schema

from fugue_duckdb._utils import to_duck_type, to_pa_type


class DuckDataFrame(LocalBoundedDataFrame):
    """DataFrame that wraps DuckDB ``DuckDBPyRelation``.

    :param rel: ``DuckDBPyRelation`` object
    :param metadata: dict-like object with string keys, default ``None``
    """

    def __init__(self, rel: DuckDBPyRelation, metadata: Any = None):
        self._rel = rel
        schema = Schema(
            [pa.field(x, to_pa_type(y)) for x, y in zip(rel.columns, rel.types)]
        )
        super().__init__(schema=schema, metadata=metadata)

    @property
    def native(self) -> DuckDBPyRelation:
        """DuckDB relation object"""
        return self._rel

    @property
    def empty(self) -> bool:
        return self._rel.fetchone() is None

    def peek_array(self) -> Any:
        res = self._rel.fetchone()
        if res is None:
            raise FugueDataFrameEmptyError()
        return list(res)

    def count(self) -> int:
        return self._rel.aggregate("count(1) AS ct").fetchone()[0]

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        schema = self.schema.exclude(cols)
        rel = self._rel.project(",".join(n for n in schema.names))
        return DuckDataFrame(rel)

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        schema = self.schema.extract(keys)
        rel = self._rel.project(",".join(n for n in schema.names))
        return DuckDataFrame(rel)

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            schema = self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        expr = ", ".join(f"{a} AS {b}" for a, b in zip(self.schema.names, schema.names))
        return DuckDataFrame(self._rel.project(expr))

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        fields: List[str] = []
        for f1, f2 in zip(self.schema.fields, new_schema.fields):
            if f1.type == f2.type:
                fields.append(f1.name)
            else:
                tp = to_duck_type(f2.type)
                fields.append(f"CAST({f1.name} AS {tp}) AS {f1.name}")
        return DuckDataFrame(self._rel.project(", ".join(fields)))

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        return self._rel.arrow()

    def as_pandas(self) -> pd.DataFrame:
        if any(pa.types.is_nested(f.type) for f in self.schema.fields):
            # Duckdb has issue to directly convert nested types to pandas
            return ArrowDataFrame(self.as_arrow()).as_pandas()
        return self._rel.to_df()

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        if columns is not None:
            return self[columns].as_array(type_safe=type_safe)
        return [list(x) for x in self._rel.fetchall()]

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        if columns is not None:
            yield from self[columns].as_array_iterable(type_safe=type_safe)
        else:
            yield from [list(x) for x in self._rel.fetchall()]
