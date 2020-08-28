import json
import math
from datetime import datetime
from typing import Any

import dask.dataframe as pd
import numpy as np
import pandas
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueDataFrameInitError
from fugue_dask.dataframe import DaskDataFrame
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
from triad.collections.schema import Schema


class DaskDataFrameTests(DataFrameTests.Tests):
    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> DaskDataFrame:
        return DaskDataFrame(data, schema, metadata)


def test_init():
    df = DaskDataFrame(schema="a:str,b:int")
    assert df.is_bounded
    assert df.count() == 0
    assert df.schema == "a:str,b:int"

    pdf = pandas.DataFrame([["a", 1], ["b", 2]])
    raises(FugueDataFrameInitError, lambda: DaskDataFrame(pdf))
    df = DaskDataFrame(pdf, "a:str,b:str")
    assert [["a", "1"], ["b", "2"]] == df.as_pandas().values.tolist()
    df = DaskDataFrame(pdf, "a:str,b:int")
    assert [["a", 1], ["b", 2]] == df.as_pandas().values.tolist()
    df = DaskDataFrame(pdf, "a:str,b:double")
    assert [["a", 1.0], ["b", 2.0]] == df.as_pandas().values.tolist()

    pdf = DaskDataFrame([["a", 1], ["b", 2]], "a:str,b:int").native["b"]
    assert isinstance(pdf, pd.Series)
    df = DaskDataFrame(pdf, "b:str")
    assert [["1"], ["2"]] == df.as_pandas().values.tolist()
    df = DaskDataFrame(pdf, "b:double")
    assert [[1.0], [2.0]] == df.as_pandas().values.tolist()

    pdf = DaskDataFrame([["a", 1], ["b", 2]], "x:str,y:long").native
    df = DaskDataFrame(pdf)
    assert df.schema == "x:str,y:long"
    df = DaskDataFrame(pdf, "y:str,x:str")
    assert [["1", "a"], ["2", "b"]] == df.as_pandas().values.tolist()
    ddf = DaskDataFrame(df)
    assert [["1", "a"], ["2", "b"]] == ddf.as_pandas().values.tolist()
    assert df.native is ddf.native  # no real copy happened

    df = DaskDataFrame([["a", 1], ["b", "2"]], "x:str,y:double")
    assert [["a", 1.0], ["b", 2.0]] == df.as_pandas().values.tolist()

    df = DaskDataFrame([], "x:str,y:double")
    assert [] == df.as_pandas().values.tolist()

    raises(FugueDataFrameInitError, lambda: DaskDataFrame(123))


def test_simple_methods():
    df = DaskDataFrame([], "a:str,b:int")
    assert df.empty
    assert 0 == df.count()
    assert not df.is_local

    df = DaskDataFrame([["a", 1], ["b", "2"]], "x:str,y:double")
    assert not df.empty
    assert 2 == df.count()
    assert ["a", 1.0] == df.peek_array()
    assert dict(x="a", y=1.0) == df.peek_dict()

    df_eq(
        PandasDataFrame(df.as_pandas()),
        [["a", 1.0], ["b", 2.0]],
        "x:str,y:double",
        throw=True,
    )


def _test_nested():
    # TODO: nested type doesn't work in dask
    # data = [[dict(a=1, b=[3, 4], d=1.0)], [json.dumps(dict(b=[30, "40"]))]]
    # df = DaskDataFrame(data, "a:{a:str,b:[int]}")
    # a = df.as_array(type_safe=True)
    # assert [[dict(a="1", b=[3, 4])], [dict(a=None, b=[30, 40])]] == a

    data = [[[json.dumps(dict(b=[30, "40"]))]]]
    df = DaskDataFrame(data, "a:[{a:str,b:[int]}]")
    a = df.as_array(type_safe=True)
    assert [[[dict(a=None, b=[30, 40])]]] == a


def test_as_array():
    df = DaskDataFrame([], "a:str,b:int")
    assert [] == df.as_array()
    assert [] == df.as_array(type_safe=True)
    assert [] == list(df.as_array_iterable())
    assert [] == list(df.as_array_iterable(type_safe=True))

    df = DaskDataFrame([["a", 1]], "a:str,b:int")
    assert [["a", 1]] == df.as_array()
    assert [["a", 1]] == df.as_array(["a", "b"])
    assert [[1, "a"]] == df.as_array(["b", "a"])

    # prevent pandas auto type casting
    df = DaskDataFrame([[1.0, 1.1]], "a:double,b:int")
    assert [[1.0, 1]] == df.as_array()
    assert isinstance(df.as_array()[0][0], float)
    assert isinstance(df.as_array()[0][1], int)
    assert [[1.0, 1]] == df.as_array(["a", "b"])
    assert [[1, 1.0]] == df.as_array(["b", "a"])

    df = DaskDataFrame([[np.float64(1.0), 1.1]], "a:double,b:int")
    assert [[1.0, 1]] == df.as_array()
    assert isinstance(df.as_array()[0][0], float)
    assert isinstance(df.as_array()[0][1], int)

    df = DaskDataFrame([[pandas.Timestamp("2020-01-01"), 1.1]], "a:datetime,b:int")
    df.native["a"] = pd.to_datetime(df.native["a"])
    assert [[datetime(2020, 1, 1), 1]] == df.as_array()
    assert isinstance(df.as_array()[0][0], datetime)
    assert isinstance(df.as_array()[0][1], int)

    df = DaskDataFrame([[pandas.NaT, 1.1]], "a:datetime,b:int")
    df.native["a"] = pd.to_datetime(df.native["a"])
    assert isinstance(df.as_array()[0][0], datetime)
    assert isinstance(df.as_array()[0][1], int)

    df = DaskDataFrame([[1.0, 1.1]], "a:double,b:int")
    assert [[1.0, 1]] == df.as_array(type_safe=True)
    assert isinstance(df.as_array()[0][0], float)
    assert isinstance(df.as_array()[0][1], int)


def test_as_dict_iterable():
    df = DaskDataFrame([["2020-01-01", 1.1]], "a:datetime,b:int")
    assert [dict(a=datetime(2020, 1, 1), b=1)] == list(df.as_dict_iterable())


def test_nan_none():
    # TODO: on dask, these tests can't pass
    # df = ArrayDataFrame([[None, None]], "b:str,c:double")
    # assert df.as_pandas().iloc[0, 0] is None
    # arr = DaskDataFrame(df.as_pandas(), df.schema).as_array()[0]
    # assert arr[0] is None
    # assert math.isnan(arr[1])

    # df = ArrayDataFrame([[None, None]], "b:int,c:bool")
    # arr = DaskDataFrame(df.as_pandas(), df.schema).as_array(type_safe=True)[0]
    # assert np.isnan(arr[0])  # TODO: this will cause inconsistent behavior cross engine
    # assert np.isnan(arr[1])  # TODO: this will cause inconsistent behavior cross engine

    df = ArrayDataFrame([["a", 1.1], [None, None]], "b:str,c:double")
    arr = DaskDataFrame(df.as_pandas(), df.schema).as_array()[1]
    assert arr[0] is None
    assert math.isnan(arr[1])

    arr = DaskDataFrame(df.as_array(), df.schema).as_array()[1]
    assert arr[0] is None
    assert math.isnan(arr[1])

    arr = DaskDataFrame(df.as_pandas()["b"], "b:str").as_array()[1]
    assert arr[0] is None


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
    df = DaskDataFrame(data, s)
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
