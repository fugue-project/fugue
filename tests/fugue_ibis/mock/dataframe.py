from typing import Any

from fugue import ArrowDataFrame, DataFrame, LocalDataFrame
from fugue_ibis import IbisDataFrame, IbisTable
from fugue_ibis._utils import to_schema


class MockDuckDataFrame(IbisDataFrame):
    def _to_new_df(self, table: IbisTable, schema: Any = None) -> DataFrame:
        return MockDuckDataFrame(table, schema=schema)

    def _to_local_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return ArrowDataFrame(table.execute(), schema=schema)

    def _to_iterable_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return self._to_local_df(table, schema=schema)
