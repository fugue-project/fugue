import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from fugue.dataframe import (
    IterableDataFrame,
    PandasDataFrame,
    ArrayDataFrame,
    LocalDataFrameIterableDataFrame,
)
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueDataFrameInitError
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
from triad.collections.schema import Schema, SchemaError
from triad.exceptions import InvalidOperationError


class LocalDataFrameIterableDataFrameTests(DataFrameTests.Tests):
    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> IterableDataFrame:
        def get_dfs():
            if isinstance(data, list):
                for row in data:
                    yield ArrayDataFrame([], schema, metadata)  # noise
                    yield ArrayDataFrame([row], schema, metadata)
                if schema is None:
                    yield ArrayDataFrame([], schema, metadata)  # noise
            elif data is not None:
                yield ArrayDataFrame(data, schema, metadata)

        return LocalDataFrameIterableDataFrame(get_dfs(), schema, metadata)


def test_init():
    raises(FugueDataFrameInitError, lambda: LocalDataFrameIterableDataFrame(1))

    def get_dfs(seq):
        for x in seq:
            if x == "e":
                yield IterableDataFrame([], "a:int,b:int")
            if x == "v":
                yield IterableDataFrame([[1, 10]], "a:int,b:int")
            if x == "o":  # bad schema but empty dataframe doesn't matter
                yield ArrayDataFrame([], "a:int,b:str")

    with raises(FugueDataFrameInitError):
        LocalDataFrameIterableDataFrame(get_dfs(""))

    with raises(FugueDataFrameInitError):  # schema mismatch
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