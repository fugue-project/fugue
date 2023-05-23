from datetime import datetime
from typing import Any

import duckdb
import pandas as pd

import fugue.api as fa
from fugue import ArrowDataFrame
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_test.dataframe_suite import DataFrameTests


class DuckDataFrameTests(DataFrameTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()

    def df(self, data: Any = None, schema: Any = None) -> DuckDataFrame:
        df = ArrowDataFrame(data, schema)
        return DuckDataFrame(duckdb.from_arrow(df.native, self._con))

    def test_as_array_special_values(self):
        for func in [
            lambda df, *args, **kwargs: df.as_array(*args, **kwargs, type_safe=True),
            lambda df, *args, **kwargs: list(
                df.as_array_iterable(*args, **kwargs, type_safe=True)
            ),
        ]:
            df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
            data = func(df)
            assert [[datetime(2020, 1, 1), 1]] == data
            assert isinstance(data[0][0], datetime)
            assert isinstance(data[0][1], int)

            df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
            assert [[None, 1]] == func(df)

            df = self.df([[float("nan"), 1]], "a:double,b:int")
            assert [[None, 1]] == func(df)

            # DuckDB disallows nan and inf
            # see https://github.com/duckdb/duckdb/pull/541

            # df = self.df([[float("inf"), 1]], "a:double,b:int")
            # assert [[float("inf"), 1]] == func(df)

    def test_as_pandas_duck(self):
        df = self.df([[2.1, 1]], "a:double,b:int")
        assert df.as_pandas().values.tolist() == [[2.1, 1]]
        df = self.df([[2.1, [1]]], "a:double,b:[int]")
        assert df.as_pandas().values.tolist() == [[2.1, [1]]]
        df = self.df([[2.1, ["a"]]], "a:double,b:[str]")
        assert df.as_pandas().values.tolist() == [[2.1, ["a"]]]
        df = self.df([[2.1, {"a": 1}]], "a:double,b:{a:int}")
        assert df.as_pandas().values.tolist() == [[2.1, {"a": 1}]]

    def test_init(self):
        df = self.df([], "a:int,b:str")
        assert df.schema == "a:int,b:str"
        assert df.empty
        assert isinstance(df.native, duckdb.DuckDBPyRelation)
        assert df.is_bounded
        assert df.is_local

    def test_duck_as_local(self):
        df = self.df([[2.1, 1]], "a:double,b:int")
        assert isinstance(df.as_local(), ArrowDataFrame)


class NativeDuckDataFrameTests(DataFrameTests.NativeTests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()

    def df(self, data: Any = None, schema: Any = None) -> DuckDataFrame:
        df = ArrowDataFrame(data, schema)
        return DuckDataFrame(duckdb.from_arrow(df.native, self._con)).native

    def to_native_df(self, pdf: pd.DataFrame) -> Any:
        return duckdb.from_df(pdf)

    def test_num_partitions(self):
        assert fa.get_num_partitions(self.df([[0, 1]], "a:int,b:int")) == 1
