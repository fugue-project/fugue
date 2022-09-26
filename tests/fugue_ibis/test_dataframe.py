import sys
from datetime import datetime
from typing import Any

import ibis
import pandas as pd
import pytest
from fugue import ArrowDataFrame
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_test.dataframe_suite import DataFrameTests

from .mock.dataframe import MockDuckDataFrame


@pytest.mark.skipif(sys.version_info < (3, 8), reason="< 3.8")
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
