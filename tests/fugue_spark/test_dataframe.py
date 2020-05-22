import json
import math
from datetime import datetime

import numpy as np
import pandas as pd
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.dataframe.utils import to_local_bounded_df
from fugue_spark.dataframe import SparkDataFrame
from fugue_spark.utils.convert import to_schema, to_spark_schema
from pyspark.sql import SparkSession
from pytest import raises
from triad.collections.schema import Schema, SchemaError
from triad.exceptions import InvalidOperationError
from fugue.dataframe.pandas_dataframes import PandasDataFrame


def test_init(spark_session):
    # spark_session = SparkSession.builder.config(
    #    "spark.master", "local[*]").getOrCreate()
    raises(ValueError, lambda: SparkDataFrame())

    sdf = spark_session.createDataFrame([["a", 1]])
    df = SparkDataFrame(sdf, "a:str,b:double")
    assert [["a", 1.0]] == df.as_array()
    assert [["a", 1.0]] == df.as_pandas().values.tolist()

    df = _df([["a", 1], ["b", 2]])
    assert [["a", 1], ["b", 2]] == df.as_array()
    df = _df([], "a:str,b:str")
    assert [] == df.as_array()
    assert df.schema == "a:str,b:str"
    df = _df([["a", 1], ["b", 2]], "a:str,b:str")
    assert [["a", "1"], ["b", "2"]] == df.as_array()
    assert df.schema == "a:str,b:str"


def test_simple_methods(spark_session):
    df = _df([], "a:int")

    assert df.empty
    assert 0 == df.count()
    raises(InvalidOperationError, lambda: df.peek_array())
    raises(InvalidOperationError, lambda: df.peek_dict())
    assert not df.is_local
    assert df.is_bounded

    df = _df([["a", 1], ["b", 2]], "x:str,y:int")
    assert not df.empty
    assert df.is_bounded
    assert df.num_partitions > 0
    assert 2 == df.count()
    assert ["a", 1.0] == df.peek_array()
    assert dict(x="a", y=1.0) == df.peek_dict()


def test_nested(spark_session):
    # data = [[dict(a=1, b=[3, 4], d=1.0)], [json.dumps(dict(b=[30, "40"]))]]
    # df = SparkDataFrame(data, "a:{a:str,b:[int]}")
    # a = df.as_array(type_safe=True)
    # assert [[dict(a="1", b=[3, 4])], [dict(a=None, b=[30, 40])]] == a

    data = [[[10, 20]]]
    sdf = spark_session.createDataFrame(data, to_spark_schema("a:[int]"))
    df = SparkDataFrame(sdf)
    assert data == df.as_array(type_safe=False)
    assert data == df.as_array(type_safe=True)
    assert data == list(df.as_array_iterable(type_safe=False))
    assert data == list(df.as_array_iterable(type_safe=True))

    data = [[dict(b=[30, 40])]]
    sdf = spark_session.createDataFrame(data, to_spark_schema("a:{a:str,b:[int]}"))
    df = SparkDataFrame(sdf)
    a = df.as_array(type_safe=False)
    assert [[dict(a=None, b=[30, 40])]] == a
    a = df.as_array(type_safe=True)
    assert [[dict(a=None, b=[30, 40])]] == a
    a = list(df.as_array_iterable(type_safe=False))
    assert [[dict(a=None, b=[30, 40])]] == a
    a = list(df.as_array_iterable(type_safe=True))
    assert [[dict(a=None, b=[30, 40])]] == a


def test_drop(spark_session):
    df = _df([], "a:str,b:int").drop(["a"])
    assert df.empty
    assert df.schema == "b:int"
    raises(InvalidOperationError, lambda: df.drop(["b"]))  # can't be empty
    raises(InvalidOperationError, lambda: df.drop(["x"]))  # cols must exist

    df = _df([["a", 1]], "a:str,b:int").drop(["a"])
    assert df.schema == "b:int"
    raises(InvalidOperationError, lambda: df.drop(["b"]))  # can't be empty
    raises(InvalidOperationError, lambda: df.drop(["x"]))  # cols must exist
    assert [[1]] == df.as_pandas().values.tolist()


def test_rename(spark_session):
    df = _df([["a", 1]], "a:str,b:int")
    df2 = df.rename(columns=dict(a="aa"))
    df_eq(df2, [["a", 1]], "aa:str,b:int", throw=True)
    df_eq(df, [["a", 1]], "a:str,b:int", throw=True)


def test_as_array(spark_session):
    for func in [lambda df, *args, **kwargs: df.as_array(*args, **kwargs),
                 lambda df, *args, **kwargs: list(df.as_array_iterable(*args, **kwargs))]:
        df = _df([], "a:str,b:int")

        assert [] == func(df)
        assert [] == func(df, type_safe=True)

        df = _df([["a", 1]], "a:str,b:int")
        assert [["a", 1]] == func(df)
        assert [["a", 1]] == func(df, ["a", "b"])
        assert [[1, "a"]] == func(df, ["b", "a"])

        # prevent pandas auto type casting
        df = _df([[1.0, 1]], "a:double,b:int")
        assert [[1.0, 1]] == func(df)
        assert isinstance(func(df)[0][0], float)
        assert isinstance(func(df)[0][1], int)
        assert [[1.0, 1]] == func(df, ["a", "b"])
        assert [[1, 1.0]] == func(df, ["b", "a"])

        df = _df([[np.float64(1.1), 1]], "a:double,b:int")
        assert [[1.1, 1]] == func(df)
        assert isinstance(func(df)[0][0], float)
        assert isinstance(func(df)[0][1], int)

        df = _df([[pd.Timestamp("2020-01-01"), 1.1]], "a:datetime,b:int")
        assert [[datetime(2020, 1, 1), 1]] == func(df)
        assert isinstance(func(df)[0][0], datetime)
        assert isinstance(func(df)[0][1], int)

        df = _df([[pd.NaT, 1.1]], "a:datetime,b:int")
        assert isinstance(func(df, type_safe=True)[0][0], datetime)
        assert isinstance(func(df, type_safe=True)[0][1], int)

        df = _df([[1.0, 1]], "a:double,b:int")
        assert [[1.0, 1]] == func(df, type_safe=True)
        assert isinstance(func(df)[0][0], float)
        assert isinstance(func(df)[0][1], int)


def test_as_dict_iterable():
    df = _df([[pd.NaT, 1.1]], "a:datetime,b:int")
    assert [dict(a=pd.NaT, b=1)] == list(df.as_dict_iterable())
    df = _df([["2020-01-01", 1.1]], "a:datetime,b:int")
    assert [dict(a=datetime(2020, 1, 1), b=1)] == list(df.as_dict_iterable())


def test_head():
    df = _df([], "a:str,b:int")
    assert [] == df.head(1)
    df = _df([["a", 1]], "a:str,b:int")
    assert [["a", 1]] == df.head(1)
    assert [["a", 1]] == df.head(1)
    assert [[1, "a"]] == df.head(1, ["b", "a"])


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
    df = SparkDataFrame(data, s)
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


def _df(data, schema=None, metadata=None):
    session = SparkSession.builder.getOrCreate()
    if schema is not None:
        pdf = PandasDataFrame(data, to_schema(schema), metadata)
        df = session.createDataFrame(pdf.native, to_spark_schema(schema))
    else:
        df = session.createDataFrame(data)
    return SparkDataFrame(df, schema, metadata)
