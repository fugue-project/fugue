import os
import pickle

import numpy as np
import pandas as pd
import pyarrow as pa
from pytest import raises
from triad import Schema
from triad.collections.schema import SchemaError
from triad.exceptions import InvalidOperationError, NoneArgumentError

from fugue import ArrayDataFrame, IterableDataFrame, PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.dataframe.utils import (
    deserialize_df,
    get_column_names,
    get_join_schemas,
    normalize_dataframe_column_names,
    rename,
    serialize_df,
)


def test_df_eq():
    df1 = ArrayDataFrame([[0, 100.0, "a"]], "a:int,b:double,c:str")
    df2 = ArrayDataFrame([[0, 100.001, "a"]], "a:int,b:double,c:str")
    assert df_eq(df1, df1)
    assert df_eq(df1, df2, digits=2)
    # precision
    assert not df_eq(df1, df2, digits=6)
    # no content
    assert df_eq(df1, df2, digits=6, check_content=False)
    raises(AssertionError, lambda: df_eq(df1, df2, throw=True))

    df1 = ArrayDataFrame([[100.0, "a"]], "a:double,b:str")
    assert df_eq(df1, df1.as_pandas(), df1.schema)

    df1 = ArrayDataFrame([[None, "a"]], "a:double,b:str")
    assert df_eq(df1, df1)

    df1 = ArrayDataFrame([[None, "a"]], "a:double,b:str")
    df2 = ArrayDataFrame([[np.nan, "a"]], "a:double,b:str")
    assert df_eq(df1, df2)

    df1 = ArrayDataFrame([[100.0, None]], "a:double,b:str")
    df2 = ArrayDataFrame([[100.0, None]], "a:double,b:str")
    assert df_eq(df1, df2)

    df1 = ArrayDataFrame([[0], [1]], "a:int")
    df2 = ArrayDataFrame([[1], [0]], "a:int")
    assert df_eq(df1, df2)
    assert not df_eq(df1, df2, check_order=True)


def test_get_join_schemas():
    a = ArrayDataFrame([], "a:int,b:int")
    b = ArrayDataFrame([], "c:int")
    c = ArrayDataFrame([], "d:str,a:int")
    i, u = get_join_schemas(a, b, how="cross", on=[])
    assert i == ""
    assert u == "a:int,b:int,c:int"
    raises(NoneArgumentError, lambda: get_join_schemas(a, b, how=None, on=[]))
    raises(ValueError, lambda: get_join_schemas(a, b, how="x", on=[]))
    raises(SchemaError, lambda: get_join_schemas(a, b, how="CROSS", on=["a"]))
    raises(SchemaError, lambda: get_join_schemas(a, c, how="CROSS", on=["a"]))
    raises(SchemaError, lambda: get_join_schemas(a, c, how="CROSS", on=[]))
    raises(SchemaError, lambda: get_join_schemas(a, b, how="inner", on=["a"]))
    raises(ValueError, lambda: get_join_schemas(a, c, how="outer", on=["a"]))
    i, u = get_join_schemas(a, c, how="inner", on=["a"])
    assert i == "a:int"
    assert u == "a:int,b:int,d:str"
    i, u = get_join_schemas(a, c, how="inner", on=[])  # infer
    assert i == "a:int"
    assert u == "a:int,b:int,d:str"
    a = ArrayDataFrame([], "a:int,b:int,c:int")
    b = ArrayDataFrame([], "c:int,b:int,x:int")
    raises(SchemaError, lambda: get_join_schemas(a, b, how="inner", on=["a"]))
    i, u = get_join_schemas(a, b, how="inner", on=["c", "b"])
    assert i == "b:int,c:int"
    assert u == "a:int,b:int,c:int,x:int"
    for how in ["SEMI", "LEFT_Semi", "Anti", "left_Anti"]:
        i, u = get_join_schemas(c, a, how=how, on=["a"])
        assert i == "a:int"
        assert u == "d:str,a:int"


def test_serialize_df(tmpdir):
    def assert_eq(df, df_expected=None, raw=False):
        if df_expected is None:
            df_expected = df
        df_actual = deserialize_df(serialize_df(df))
        if raw:
            assert df_expected.native == df_actual.native
        else:
            df_eq(df_expected, df_actual, throw=True)

    assert deserialize_df(serialize_df(None)) is None
    assert_eq(ArrayDataFrame([], "a:int,b:int"))
    assert_eq(ArrayDataFrame([[None, None]], "a:int,b:int"))
    assert_eq(ArrayDataFrame([[None, "abc"]], "a:int,b:str"))
    assert_eq(
        ArrayDataFrame([[None, [1, 2], dict(x=1)]], "a:int,b:[int],c:{x:int}"), raw=True
    )
    assert_eq(
        IterableDataFrame([[None, [1, 2], dict(x=1)]], "a:int,b:[int],c:{x:int}"),
        ArrayDataFrame([[None, [1, 2], dict(x=1)]], "a:int,b:[int],c:{x:int}"),
        raw=True,
    )
    assert_eq(PandasDataFrame([[None, None]], "a:int,b:int"))
    assert_eq(PandasDataFrame([[None, "abc"]], "a:int,b:str"))

    raises(
        InvalidOperationError,
        lambda: serialize_df(ArrayDataFrame([], "a:int,b:int"), 0),
    )

    path = os.path.join(tmpdir, "1.pkl")

    df = ArrayDataFrame([[None, None]], "a:int,b:int")
    s = serialize_df(df, 0, path)
    df_eq(df, deserialize_df(s), throw=True)

    s = serialize_df(df, 0, path)
    df_eq(df, deserialize_df(s), throw=True)

    raises(ValueError, lambda: deserialize_df(pickle.dumps(1)))


def _test_get_column_names():
    df = pd.DataFrame([[0, 1, 2]])
    assert get_column_names(df) == [0, 1, 2]

    adf = pa.Table.from_pandas(df)
    assert get_column_names(adf) == ["0", "1", "2"]

    pdf = PandasDataFrame(pd.DataFrame([[0, 1]], columns=["a", "b"]))
    assert get_column_names(pdf) == ["a", "b"]


def _test_rename():
    assert rename("dummy", {}) == "dummy"
    pdf = pd.DataFrame([[0, 1, 2]], columns=["a", "b", "c"])
    df = rename(pdf, {})
    assert get_column_names(df) == ["a", "b", "c"]
    df = rename(pdf, {"b": "bb"})
    assert get_column_names(df) == ["a", "bb", "c"]

    adf = pa.Table.from_pandas(pdf)
    adf = rename(adf, {})
    assert get_column_names(adf) == ["a", "b", "c"]
    adf = rename(adf, {"b": "bb"})
    assert get_column_names(adf) == ["a", "bb", "c"]

    fdf = PandasDataFrame(pdf)
    fdf = rename(fdf, {})
    assert get_column_names(fdf) == ["a", "b", "c"]
    fdf = rename(fdf, {"b": "bb"})
    assert get_column_names(fdf) == ["a", "bb", "c"]


def test_normalize_dataframe_column_names():
    df = pd.DataFrame([[0, 1, 2]], columns=["a", "b", "c"])
    df, names = normalize_dataframe_column_names(df)
    assert get_column_names(df) == ["a", "b", "c"]
    assert names == {}

    df = pd.DataFrame([[0, 1, 2]])
    df, names = normalize_dataframe_column_names(df)
    assert get_column_names(df) == ["_0", "_1", "_2"]
    assert names == {"_0": 0, "_1": 1, "_2": 2}

    df = pd.DataFrame([[0, 1, 2, 3]], columns=["1", "2", "_2", "大"])
    df, names = normalize_dataframe_column_names(df)
    assert get_column_names(df) == ["_1", "_2_1", "_2", "_1_1"]
    assert names == {"_1": "1", "_2_1": "2", "_1_1": "大"}
