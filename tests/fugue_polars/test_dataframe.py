from datetime import datetime
from typing import Any

import pandas as pd
import polars as pl

import fugue.api as fa
from fugue import ArrowDataFrame
from fugue_polars import PolarsDataFrame
from fugue_test.dataframe_suite import DataFrameTests


class PolarsDataFrameTests(DataFrameTests.Tests):
    def df(self, data: Any = None, schema: Any = None) -> PolarsDataFrame:
        adf = ArrowDataFrame(data, schema).native
        return PolarsDataFrame(pl.from_arrow(adf))

    def test_map_type(self):
        pass

    def test_as_arrow(self):
        # empty
        df = self.df([], "a:int,b:int")
        assert [] == list(ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable())
        assert fa.is_local(fa.as_arrow(df))
        # pd.Nat
        df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
        assert [dict(a=None, b=1)] == list(
            ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable()
        )
        # pandas timestamps
        df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
        assert [dict(a=datetime(2020, 1, 1), b=1)] == list(
            ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable()
        )
        # float nan, list
        data = [[[float("nan"), 2.0]]]
        df = self.df(data, "a:[float]")
        assert [[[None, 2.0]]] == ArrowDataFrame(fa.as_arrow(df)).as_array()
        # dict
        data = [[dict(b=True)]]
        df = self.df(data, "a:{b:bool}")
        assert data == ArrowDataFrame(fa.as_arrow(df)).as_array()


class NativePolarsDataFrameTests(DataFrameTests.NativeTests):
    def df(self, data: Any = None, schema: Any = None) -> pd.DataFrame:
        adf = ArrowDataFrame(data, schema).native
        return pl.from_arrow(adf)

    def to_native_df(self, pdf: pd.DataFrame) -> Any:  # pragma: no cover
        return pl.from_pandas(pdf)

    def test_num_partitions(self):
        assert fa.get_num_partitions(self.df([[0, 1]], "a:int,b:int")) == 1

    def test_map_type(self):
        pass

    def test_as_arrow(self):
        # empty
        df = self.df([], "a:int,b:int")
        assert [] == list(ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable())
        assert fa.is_local(fa.as_arrow(df))
        # pd.Nat
        df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
        assert [dict(a=None, b=1)] == list(
            ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable()
        )
        # pandas timestamps
        df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
        assert [dict(a=datetime(2020, 1, 1), b=1)] == list(
            ArrowDataFrame(fa.as_arrow(df)).as_dict_iterable()
        )
        # float nan, list
        data = [[[float("nan"), 2.0]]]
        df = self.df(data, "a:[float]")
        assert [[[None, 2.0]]] == ArrowDataFrame(fa.as_arrow(df)).as_array()
        # dict
        data = [[dict(b=True)]]
        df = self.df(data, "a:{b:bool}")
        assert data == ArrowDataFrame(fa.as_arrow(df)).as_array()
