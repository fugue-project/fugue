import concurrent.futures
import os
from threading import RLock

import numpy as np
from fugue.dataframe import to_local_bounded_df, to_local_df
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.iterable_dataframe import IterableDataFrame
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.dataframe.utils import (
    deserialize_df,
    get_join_schemas,
    pickle_df,
    serialize_df,
    unpickle_df,
)
from pytest import raises
from triad import FileSystem, assert_or_throw
from triad.collections.schema import SchemaError
from triad.exceptions import InvalidOperationError, NoneArgumentError


def test_to_local_df():
    df = ArrayDataFrame([[0, 1]], "a:int,b:int")
    pdf = PandasDataFrame(df.as_pandas(), "a:int,b:int")
    idf = IterableDataFrame([[0, 1]], "a:int,b:int")
    assert to_local_df(df) is df
    assert to_local_df(pdf) is pdf
    assert to_local_df(idf) is idf
    assert isinstance(to_local_df(df.native, "a:int,b:int"), ArrayDataFrame)
    assert isinstance(to_local_df(pdf.native, "a:int,b:int"), PandasDataFrame)
    assert isinstance(to_local_df(idf.native, "a:int,b:int"), IterableDataFrame)
    raises(TypeError, lambda: to_local_df(123))

    metadata = dict(a=1)
    assert to_local_df(df.native, df.schema, metadata).metadata == metadata

    raises(NoneArgumentError, lambda: to_local_df(None))
    raises(ValueError, lambda: to_local_df(df, "a:int,b:int", None))


def test_to_local_bounded_df():
    df = ArrayDataFrame([[0, 1]], "a:int,b:int")
    idf = IterableDataFrame([[0, 1]], "a:int,b:int", dict(a=1))
    assert to_local_bounded_df(df) is df
    r = to_local_bounded_df(idf)
    assert r is not idf
    assert r.as_array() == [[0, 1]]
    assert r.schema == "a:int,b:int"
    assert r.metadata == dict(a=1)


def test_df_eq():
    df1 = ArrayDataFrame([[0, 100.0, "a"]], "a:int,b:double,c:str", dict(a=1))
    df2 = ArrayDataFrame([[0, 100.001, "a"]], "a:int,b:double,c:str", dict(a=2))
    assert df_eq(df1, df1)
    assert df_eq(df1, df2, digits=4, check_metadata=False)
    # metadata
    assert not df_eq(df1, df2, digits=4, check_metadata=True)
    # precision
    assert not df_eq(df1, df2, digits=6, check_metadata=False)
    # no content
    assert df_eq(df1, df2, digits=6, check_metadata=False, check_content=False)
    raises(AssertionError, lambda: df_eq(df1, df2, throw=True))

    df1 = ArrayDataFrame([[100.0, "a"]], "a:double,b:str", dict(a=1))
    assert df_eq(df1, df1.as_pandas(), df1.schema, df1.metadata)

    df1 = ArrayDataFrame([[None, "a"]], "a:double,b:str", dict(a=1))
    assert df_eq(df1, df1)

    df1 = ArrayDataFrame([[None, "a"]], "a:double,b:str", dict(a=1))
    df2 = ArrayDataFrame([[np.nan, "a"]], "a:double,b:str", dict(a=1))
    assert df_eq(df1, df2)

    df1 = ArrayDataFrame([[100.0, None]], "a:double,b:str", dict(a=1))
    df2 = ArrayDataFrame([[100.0, None]], "a:double,b:str", dict(a=1))
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


def test_pickle_df():
    def assert_eq(df, df_expected=None, raw=False):
        if df_expected is None:
            df_expected = df
        df_actual = unpickle_df(pickle_df(df))
        if raw:
            assert df_expected.native == df_actual.native
        else:
            df_eq(df_expected, df_actual, throw=True)

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


def test_serialize_df(tmpdir):
    def assert_eq(df, df_expected=None, raw=False):
        if df_expected is None:
            df_expected = df
        df_actual = deserialize_df(serialize_df(df))
        if raw:
            assert df_expected.native == df_actual.native
        else:
            df_eq(df_expected, df_actual, throw=True)

    fs = FileSystem()
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
    s = serialize_df(df, 0, path, fs)
    df_eq(df, deserialize_df(s, fs), throw=True)
    df_eq(df, deserialize_df(s), throw=True)

    s = serialize_df(df, 0, path)
    df_eq(df, deserialize_df(s), throw=True)

    raises(ValueError, lambda: deserialize_df('{"x":1}'))
