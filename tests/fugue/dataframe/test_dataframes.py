from fugue.dataframe import DataFrames
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from pytest import raises
from triad.exceptions import InvalidOperationError


def test_dataframes():
    df1 = ArrayDataFrame([[0]], "a:int")
    df2 = ArrayDataFrame([[1]], "a:int")
    dfs = DataFrames(a=df1, b=df2)
    assert dfs[0] is df1
    assert dfs[1] is df2

    dfs = DataFrames([df1, df2], df1)
    assert not dfs.has_key
    assert dfs[0] is df1
    assert dfs[1] is df2
    assert dfs[2] is df1

    dfs2 = DataFrames(dfs, dfs, df2)
    assert not dfs2.has_key
    assert dfs2[0] is df1
    assert dfs2[1] is df2
    assert dfs2[2] is df1
    assert dfs2[3] is df1
    assert dfs2[4] is df2
    assert dfs2[5] is df1
    assert dfs2[6] is df2

    dfs = DataFrames([("a", df1), ("b", df2)])
    assert dfs.has_key
    assert dfs[0] is df1
    assert dfs[1] is df2
    assert dfs["a"] is df1
    assert dfs["b"] is df2

    with raises(ValueError):
        dfs["c"] = 1

    with raises(ValueError):
        dfs2 = DataFrames(1)

    with raises(ValueError):
        dfs2 = DataFrames(a=df1, b=2)

    with raises(InvalidOperationError):
        dfs2 = DataFrames(dict(a=df1), df2)

    with raises(InvalidOperationError):
        dfs2 = DataFrames(df2, dict(a=df1))

    with raises(InvalidOperationError):
        dfs2 = DataFrames(df1, a=df2)

    with raises(InvalidOperationError):
        dfs2 = DataFrames(DataFrames(df1, df2), x=df2)

    dfs2 = DataFrames(dfs)
    assert dfs2.has_key
    assert dfs2[0] is df1
    assert dfs2[1] is df2

    dfs1 = DataFrames(a=df1, b=df2)
    dfs2 = dfs1.convert(lambda x: PandasDataFrame(x.as_array(), x.schema))
    assert len(dfs1) == len(dfs2)
    assert dfs2.has_key
    assert isinstance(dfs2["a"], PandasDataFrame)
    assert isinstance(dfs2["b"], PandasDataFrame)

    dfs1 = DataFrames(df1, df2)
    dfs2 = dfs1.convert(lambda x: PandasDataFrame(x.as_array(), x.schema))
    assert len(dfs1) == len(dfs2)
    assert not dfs2.has_key
    assert isinstance(dfs2[0], PandasDataFrame)
    assert isinstance(dfs2[1], PandasDataFrame)