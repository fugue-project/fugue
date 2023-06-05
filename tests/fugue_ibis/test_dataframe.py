import sys
from datetime import datetime
from typing import Any

import ibis
import pandas as pd
import pyarrow as pa
import pytest

import fugue.api as fe
from fugue import ArrowDataFrame
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_test.dataframe_suite import DataFrameTests

from .mock.dataframe import MockDuckDataFrame


@pytest.mark.skipif(sys.version_info < (3, 8), reason="< 3.8")
class IbisDataFrameTests(DataFrameTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = ibis.duckdb.connect()

    def df(self, data: Any = None, schema: Any = None) -> DuckDataFrame:
        df = ArrowDataFrame(data, schema)
        name = f"_{id(df.native)}"
        # self._con.con.execute("register", (name, df.native))
        self._con.register(df.native, name)
        return MockDuckDataFrame(self._con.table(name), schema=schema)

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


@pytest.mark.skipif(sys.version_info < (3, 8), reason="< 3.8")
class NativeIbisDataFrameTests(DataFrameTests.NativeTests):
    @classmethod
    def setUpClass(cls):
        cls._con = ibis.duckdb.connect()

    def df(self, data: Any = None, schema: Any = None):
        df = ArrowDataFrame(data, schema)
        name = f"_{id(df.native)}"
        # self._con.con.execute("register", (name, df.native))
        self._con.register(df.native, name)
        return MockDuckDataFrame(self._con.table(name), schema=schema).native

    def to_native_df(self, pdf: pd.DataFrame) -> Any:
        name = f"_{id(pdf)}"
        # self._con.con.execute("register", (name, pa.Table.from_pandas(pdf)))
        self._con.register(pa.Table.from_pandas(pdf), name)
        return self._con.table(name)

    def test_is_local(self):
        df = self.df([["x", 1]], "a:str,b:int")
        assert not fe.is_local(df)
        assert fe.is_bounded(df)

    def test_map_type(self):
        pass

    def test_as_arrow(self):
        pass

    def test_deep_nested_types(self):
        pass

    def test_list_type(self):
        pass
