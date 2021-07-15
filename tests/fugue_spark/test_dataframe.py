import json
import math
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.dataframe.utils import to_local_bounded_df
from fugue.exceptions import FugueDataFrameInitError
from fugue_test.dataframe_suite import DataFrameTests
from pyspark.sql import SparkSession
from pytest import raises
from triad.collections.schema import Schema, SchemaError
from triad.exceptions import InvalidOperationError

from fugue_spark._utils.convert import to_schema, to_spark_schema
from fugue_spark.dataframe import SparkDataFrame
from fugue_spark import SparkExecutionEngine


class SparkDataFrameTests(DataFrameTests.Tests):
    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> SparkDataFrame:
        session = SparkSession.builder.getOrCreate()
        engine = SparkExecutionEngine(session)
        return engine.to_df(data, schema=schema, metadata=metadata)

    def test_alter_columns_invalid(self):
        # TODO: Spark will silently cast invalid data to nulls without exceptions
        pass


def test_init(spark_session):
    sdf = spark_session.createDataFrame([["a", 1]])
    df = SparkDataFrame(sdf, "a:str,b:double")
    assert [["a", 1.0]] == df.as_array()
    assert [["a", 1.0]] == df.as_pandas().values.tolist()
    assert not df.is_local
    assert df.is_bounded
    assert df.num_partitions > 0

    df = _df([["a", 1], ["b", 2]])
    assert [["a", 1], ["b", 2]] == df.as_array()
    df = _df([], "a:str,b:str")
    assert [] == df.as_array()
    assert df.schema == "a:str,b:str"
    df = _df([["a", 1], ["b", 2]], "a:str,b:str")
    assert [["a", "1"], ["b", "2"]] == df.as_array()
    assert df.schema == "a:str,b:str"


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
