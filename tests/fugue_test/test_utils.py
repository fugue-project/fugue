from fugue.dataframe.array_dataframe import ArrayDataFrame

from fugue_test.utils import df_eq
from pytest import raises


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
