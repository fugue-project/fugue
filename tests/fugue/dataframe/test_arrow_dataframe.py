import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from fugue.dataframe import ArrowDataFrame, PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueDataFrameInitError
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
from triad.collections.schema import Schema, SchemaError
from triad.exceptions import InvalidOperationError


class ArrowDataFrameTests(DataFrameTests.Tests):
    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> ArrowDataFrame:
        return ArrowDataFrame(data, schema, metadata)


def test_init():
    df = ArrowDataFrame(schema="a:str,b:int")
    assert df.empty
    assert df.schema == "a:str,b:int"
    assert df.is_bounded

    df = ArrowDataFrame(pd.DataFrame([], columns=["a", "b"]), schema="a:str,b:int")
    assert df.empty
    assert df.schema == "a:str,b:int"
    assert df.is_bounded

    data = [["a", "1"], ["b", "2"]]
    df = ArrowDataFrame(data, "a:str,b:str")
    assert [["a", "1"], ["b", "2"]] == df.as_array(type_safe=True)
    data = [["a", 1], ["b", 2]]
    df = ArrowDataFrame(data, "a:str,b:int")
    assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)
    df = ArrowDataFrame(data, "a:str,b:double")
    assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)

    ddf = ArrowDataFrame(df.native)
    assert [["a", 1.0], ["b", 2.0]] == ddf.as_array(type_safe=True)

    df = ArrowDataFrame(df.as_pandas(), "a:str,b:double")
    assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)
    df = ArrowDataFrame(df.as_pandas()["b"])
    assert [[1.0], [2.0]] == df.as_array(type_safe=True)

    df = ArrowDataFrame([], "x:str,y:double")
    assert df.empty
    assert df.is_local
    assert df.is_bounded

    raises(FugueDataFrameInitError, lambda: ArrowDataFrame(123))
