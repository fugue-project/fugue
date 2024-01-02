from typing import Any

import numpy as np
import pandas as pd
from pytest import raises

import fugue.api as fa
import fugue.test as ft
from fugue.dataframe import (
    ArrayDataFrame,
    ArrowDataFrame,
    IterableArrowDataFrame,
    IterableDataFrame,
    IterablePandasDataFrame,
    LocalDataFrameIterableDataFrame,
    PandasDataFrame,
)
from fugue_test.dataframe_suite import DataFrameTests


@ft.fugue_test_suite("native", mark_test=True)
class LocalDataFrameIterableDataFrameTests(DataFrameTests.Tests):
    def df(self, data: Any = None, schema: Any = None) -> IterableDataFrame:
        def get_dfs():
            if isinstance(data, list):
                for row in data:
                    yield ArrayDataFrame([], schema)  # noise
                    yield ArrayDataFrame([row], schema)
                if schema is None:
                    yield ArrayDataFrame([], schema)  # noise
            elif data is not None:
                yield ArrayDataFrame(data, schema)

        return LocalDataFrameIterableDataFrame(get_dfs(), schema)

    def test_empty_dataframes(self):
        df = IterablePandasDataFrame([], schema="a:long,b:int")
        assert df.empty
        pdf = df.as_pandas()
        assert pdf.dtypes["a"] == pd.Int64Dtype()
        assert pdf.dtypes["b"] == pd.Int32Dtype()
        assert fa.get_schema(df.as_arrow()) == "a:long,b:int"

        dfs = [PandasDataFrame(schema="a:long,b:int")]
        df = IterablePandasDataFrame(dfs)
        assert df.empty
        pdf = df.as_pandas()
        assert pdf.dtypes["a"] == pd.Int64Dtype()
        assert pdf.dtypes["b"] == pd.Int32Dtype()
        assert fa.get_schema(df.as_arrow()) == "a:long,b:int"

        dfs = [ArrowDataFrame(schema="a:long,b:int")]
        df = IterableArrowDataFrame(dfs)
        assert df.empty
        pdf = df.as_pandas()
        assert pdf.dtypes["a"] == pd.Int64Dtype()
        assert pdf.dtypes["b"] == pd.Int32Dtype()
        assert fa.get_schema(df.as_arrow()) == "a:long,b:int"


def test_init():
    raises(Exception, lambda: LocalDataFrameIterableDataFrame(1))

    def get_dfs(seq):
        for x in seq:
            if x == "e":
                yield IterableDataFrame([], "a:int,b:int")
            if x == "v":
                yield IterableDataFrame([[1, 10]], "a:int,b:int")
            if x == "o":  # bad schema but empty dataframe doesn't matter
                yield ArrayDataFrame([], "a:int,b:str")

    with raises(Exception):
        LocalDataFrameIterableDataFrame(get_dfs(""))

    with raises(Exception):  # schema mismatch
        LocalDataFrameIterableDataFrame(get_dfs("v"), "a:int,b:str")

    df = LocalDataFrameIterableDataFrame(get_dfs(""), "a:str")
    assert df.empty
    assert df.schema == "a:str"

    df = LocalDataFrameIterableDataFrame(get_dfs("e"))
    assert df.empty
    assert df.schema == "a:int,b:int"

    df = LocalDataFrameIterableDataFrame(get_dfs("eeeo"))
    assert df.empty
    assert df.schema == "a:int,b:str"

    df = LocalDataFrameIterableDataFrame(get_dfs("eveo"))
    assert not df.empty
    assert df.schema == "a:int,b:int"
    assert [[1, 10]] == df.as_array()

    df = LocalDataFrameIterableDataFrame(get_dfs("oveo"), "a:int,b:int")
    assert not df.empty
    assert df.schema == "a:int,b:int"
    assert [[1, 10]] == df.as_array()
