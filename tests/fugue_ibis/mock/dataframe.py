from typing import Any

from fugue import ArrowDataFrame, DataFrame, LocalDataFrame
from fugue.plugins import as_fugue_dataset, as_local_bounded
from fugue_ibis import IbisDataFrame, IbisTable


class MockDuckDataFrame(IbisDataFrame):
    def to_sql(self) -> str:
        return str(self.native.compile())

    def _to_new_df(self, table: IbisTable, schema: Any = None) -> DataFrame:
        return MockDuckDataFrame(table, schema=schema)

    def _to_local_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return ArrowDataFrame(table.execute(), schema=schema)

    def _to_iterable_df(self, table: IbisTable, schema: Any = None) -> LocalDataFrame:
        return self._to_local_df(table, schema=schema)


# should also check the df._findbackend is duckdb
@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, IbisTable))
def _ibis_as_fugue(df: IbisTable, **kwargs: Any) -> bool:
    return MockDuckDataFrame(df, **kwargs)


# should also check the df._findbackend is duckdb
@as_local_bounded.candidate(lambda df, **kwargs: isinstance(df, IbisTable))
def _ibis_as_local(df: IbisTable, **kwargs: Any) -> bool:
    return df.execute()
