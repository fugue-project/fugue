import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from fugue.dataframe import ArrayDataFrame, PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueDataFrameInitError
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
from triad.collections.schema import Schema, SchemaError
from triad.exceptions import InvalidOperationError


class ArrayDataFrameTests(DataFrameTests.Tests):
    def df(self, data: Any = None, schema: Any = None,
           metadata: Any = None) -> ArrayDataFrame:
        return ArrayDataFrame(data, schema, metadata)


def test_init():
    df = ArrayDataFrame(schema="a:str,b:int")
    assert df.empty
    assert df.schema == "a:str,b:int"
    assert df.is_bounded

    data = [["a", 1], ["b", 2]]
    df = ArrayDataFrame(data, "a:str,b:str")
    assert [["a", "1"], ["b", "2"]] == df.as_array(type_safe=True)
    df = ArrayDataFrame(data, "a:str,b:int")
    assert [["a", 1], ["b", 2]] == df.as_array(type_safe=True)
    df = ArrayDataFrame(data, "a:str,b:double")
    assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)

    df = ArrayDataFrame(data, "a:str,b:double")
    ddf = ArrayDataFrame(df)
    assert [["a", 1.0], ["b", 2.0]] == ddf.as_array(type_safe=True)
    df = ArrayDataFrame(data, "a:str,b:double")
    ddf = ArrayDataFrame(df, "a:str,b:float64")
    assert [["a", 1.0], ["b", 2.0]] == ddf.as_array(type_safe=True)
    df = ArrayDataFrame(data, "a:str,b:double")
    ddf = ArrayDataFrame(df, "b:str,a:str")
    assert [["1", "a"], ["2", "b"]] == ddf.as_array(type_safe=True)
    df = ArrayDataFrame(data, "a:str,b:double")
    ddf = ArrayDataFrame(df, ["b"])
    assert ddf.schema == "b:double"
    assert [[1.0], [2.0]] == ddf.as_array(type_safe=True)
    df = ArrayDataFrame(data, "a:str,b:double")
    ddf = ArrayDataFrame(df, ["a:str,b:str"])
    assert [["a", "1"], ["b", "2"]] == ddf.as_array(type_safe=True)
    df = ArrayDataFrame(data, "a:str,b:double")
    ddf = ArrayDataFrame(df, ["b:str"])
    assert [["1"], ["2"]] == ddf.as_array(type_safe=True)

    pdf = PandasDataFrame(data, "a:str,b:double")
    df = ArrayDataFrame(pdf, "a:str,b:double")
    assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)
    df = ArrayDataFrame(pdf, "b:str,a:str")
    assert [["1.0", "a"], ["2.0", "b"]] == df.as_array(type_safe=True)

    df = ArrayDataFrame([], "x:str,y:double")
    assert df.empty

    raises(FugueDataFrameInitError, lambda: ArrayDataFrame(123))


def test_simple_methods():
    df = ArrayDataFrame([["a", 1], ["b", "2"]], "x:str,y:double")
    assert 2 == df.count()
    assert not df.empty
    assert ["a", 1.0] == df.peek_array()
    assert dict(x="a", y=1.0) == df.peek_dict()
    assert [["a", 1], ["b", "2"]] == df.as_array()

    df = ArrayDataFrame([["a", 1], ["b", "2"]], "x:str,y:double")
    pdf = df.as_pandas()
    assert [["a", 1.0], ["b", 2.0]] == pdf.values.tolist()

    df = ArrayDataFrame([], "x:str,y:double")
    pdf = df.as_pandas()
    assert [] == pdf.values.tolist()


def test_nested():
    data = [[dict(a=1, b=[3, 4], d=1.0)], [json.dumps(dict(b=[30, "40"]))]]
    df = ArrayDataFrame(data, "a:{a:str,b:[int]}")
    a = df.as_array(type_safe=True)
    assert [[dict(a="1", b=[3, 4])], [dict(a=None, b=[30, 40])]] == a

    data = [[[json.dumps(dict(b=[30, "40"]))]]]
    df = ArrayDataFrame(data, "a:[{a:str,b:[int]}]")
    a = df.as_array(type_safe=True)
    assert [[[dict(a=None, b=[30, 40])]]] == a


def test_as_array():
    df = ArrayDataFrame([], "a:str,b:int")
    assert [] == df.as_array()
    assert [] == df.as_array(type_safe=True)
    assert [] == list(df.as_array_iterable())
    assert [] == list(df.as_array_iterable(type_safe=True))

    df = ArrayDataFrame([["a", 1]], "a:str,b:int")
    assert [["a", 1]] == df.as_array()
    df = ArrayDataFrame([["a", 1]], "a:str,b:int")
    assert [["a", 1]] == df.as_array(["a", "b"])
    df = ArrayDataFrame([["a", 1]], "a:str,b:int")
    assert [[1, "a"]] == df.as_array(["b", "a"])

    df = ArrayDataFrame([[1.0, 1.1]], "a:double,b:int")
    d = df.as_array(type_safe=False)
    assert [[1.0, 1]] != d

    df = ArrayDataFrame([[1.0, 1.1]], "a:double,b:int")
    d = df.as_array(type_safe=True)
    assert [[1.0, 1]] == d
    assert isinstance(d[0][0], float)
    assert isinstance(d[0][1], int)
    df = ArrayDataFrame([[1.0, 1.1]], "a:double,b:int")
    assert [[1.0, 1]] == df.as_array(["a", "b"], type_safe=True)
    df = ArrayDataFrame([[1.0, 1.1]], "a:double,b:int")
    assert [[1, 1.0]] == df.as_array(["b", "a"], type_safe=True)

    df = ArrayDataFrame([[np.float64(1.0), 1.1]], "a:double,b:int")
    d = df.as_array(type_safe=True)
    assert [[1.0, 1]] == d
    assert isinstance(d[0][0], float)
    assert isinstance(d[0][1], int)

    df = ArrayDataFrame([[pd.Timestamp("2020-01-01"), 1.1]], "a:datetime,b:int")
    assert [[datetime(2020, 1, 1), 1]] == df.as_array(type_safe=True)

    df = ArrayDataFrame([["2020-01-01", 1.1]], "a:datetime,b:int")
    assert [[datetime(2020, 1, 1), 1]] == df.as_array(type_safe=True)

    df = ArrayDataFrame([[pd.NaT, 1.1]], "a:datetime,b:int")
    assert [[None, 1]] == df.as_array(type_safe=True)


def test_as_dict_iterable():
    df = ArrayDataFrame([["2020-01-01", 1.1]], "a:datetime,b:int")
    assert [dict(a=datetime(2020, 1, 1), b=1)] == list(df.as_dict_iterable())


def _test_as_array_perf():
    s = Schema()
    arr = []
    for i in range(100):
        s.append(f"a{i}:int")
        arr.append(i)
    for i in range(100):
        s.append(f"b{i}:int")
        arr.append(float(i))
    for i in range(100):
        s.append(f"c{i}:str")
        arr.append(str(i))
    data = []
    for i in range(5000):
        data.append(list(arr))
    df = ArrayDataFrame(data, s)
    res = df.as_array()
    res = df.as_array(type_safe=True)
    nts, ts = 0.0, 0.0
    for i in range(10):
        t = datetime.now()
        res = df.as_array()
        nts += (datetime.now() - t).total_seconds()
        t = datetime.now()
        res = df.as_array(type_safe=True)
        ts += (datetime.now() - t).total_seconds()
    print(nts, ts)
