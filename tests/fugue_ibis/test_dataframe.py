import sys
from datetime import datetime
from typing import Any

import pandas as pd

import fugue.api as fe
import fugue.test as ft
from fugue import ArrowDataFrame
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_test.dataframe_suite import DataFrameTests

from .mock.dataframe import MockDuckDataFrame
from .mock.tester import mockibisduck_session  # noqa: F401  # pylint: disable-all


@ft.fugue_test_suite("mockibisduck", mark_test=True)
class IbisDataFrameTests(DataFrameTests.Tests):
    def df(self, data: Any = None, schema: Any = None) -> MockDuckDataFrame:
        df = ArrowDataFrame(data, schema)
        name = f"_{id(df.native)}"
        con = self.context.engine.sql_engine.backend
        con.register(df.native, name)
        return MockDuckDataFrame(con.table(name), schema=schema)

    def test_init_df(self):
        df = self.df([["x", 1]], "a:str,b:int")
        df = MockDuckDataFrame(df.native, "a:str,b:long")
        assert df.schema == "a:str,b:long"

    def test_is_local(self):
        df = self.df([["x", 1]], "a:str,b:int")
        assert not fe.is_local(df)
        assert fe.is_bounded(df)

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

    def test_deep_nested_types(self):
        pass

    def test_list_type(self):
        pass
