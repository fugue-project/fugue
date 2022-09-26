import sys
from datetime import datetime
from typing import Any

import ibis
import ibis.expr.types as ir
import pandas as pd
import pytest
from fugue import (
    ArrowDataFrame,
    DataFrame,
    IterableDataFrame,
    LocalDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises

from fugue_ibis._utils import to_schema
from fugue_ibis.dataframe import IbisDataFrame

from .mock.dataframe import MockDuckDataFrame


@pytest.mark.skipif(sys.version_info < (3, 7), reason="3.6")
class TestPandasIbisDataFrame(IbisDataFrame):
    def _to_new_df(self, table: ir.Table, metadata: Any = None) -> DataFrame:
        return TestPandasIbisDataFrame(table, metadata)

    def _to_local_df(self, table: ir.Table, metadata: Any = None) -> LocalDataFrame:
        return PandasDataFrame(
            table.execute(), to_schema(table.schema()), metadata=metadata
        )

    def _to_iterable_df(
        self, table: ir.Table, metadata: Any = None
    ) -> IterableDataFrame:
        return PandasDataFrame(
            table.execute().values.tolist(),
            to_schema(table.schema()),
            metadata=metadata,
        )


class IbisDataFrameTests(DataFrameTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = ibis.duckdb.connect()

    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> DuckDataFrame:
        df = ArrowDataFrame(data, schema, metadata)
        name = f"_{id(df.native)}"
        self._con.con.execute("register", (name, df.native))
        return MockDuckDataFrame(
            self._con.table(name), schema=schema, metadata=metadata
        )

    def test_is_local(self):
        df = self.df([["x", 1]], "a:str,b:int")
        assert not df.is_local
        assert df.is_bounded

    def _test_as_arrow(self):
        # empty
        df = self.df([["a", 1]], "a:str,b:int")
        assert [["a", 1]] == list(ArrowDataFrame(df.as_arrow()).as_array())

    def test_map_type(self):
        pass

    def test_as_arrow(self):
        # empty
        df = self.df([], "a:int,b:int")
        assert [] == list(ArrowDataFrame(df.as_arrow()).as_dict_iterable())
        # pd.Nat
        df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
        assert [dict(a=None, b=1)] == list(
            ArrowDataFrame(df.as_arrow()).as_dict_iterable()
        )
        # pandas timestamps
        df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
        assert [dict(a=datetime(2020, 1, 1), b=1)] == list(
            ArrowDataFrame(df.as_arrow()).as_dict_iterable()
        )
