from typing import Any

from fugue import ArrowDataFrame, DataFrame, LocalDataFrame
from fugue_ibis import IbisDataFrame, IbisTable
from fugue_ibis._utils import to_schema


class MockDuckDataFrame(IbisDataFrame):
    def _to_new_df(self, table: IbisTable, metadata: Any = None) -> DataFrame:
        return MockDuckDataFrame(table, metadata)

    def _to_local_df(self, table: IbisTable, metadata: Any = None) -> LocalDataFrame:
        return ArrowDataFrame(
            table.execute(), to_schema(table.schema()), metadata=metadata
        )

    def _to_iterable_df(self, table: IbisTable, metadata: Any = None) -> LocalDataFrame:
        return self._to_local_df(table, metadata=metadata)
