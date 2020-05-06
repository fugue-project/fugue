from fugue.dataframe import to_local_bounded_df, to_local_df, df_eq
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.iterable_dataframe import IterableDataFrame
from fugue.dataframe.pandas_dataframes import PandasDataFrame
from pytest import raises
from triad.exceptions import NoneArgumentError


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
