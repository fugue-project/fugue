import fugue.api as fa
import pandas as pd
import polars as pl


def test_to_df():
    df = pl.from_pandas(pd.DataFrame({"a": [0, 1]}))
    res = fa.fugue_sql("SELECT * FROM df", df=df, engine="duckdb")
    assert fa.as_array(res) == [[0], [1]]

    df2 = pl.from_pandas(pd.DataFrame({"a": [0]}))
    res = fa.inner_join(df, df2, engine="duckdb")
    assert fa.as_array(res) == [[0]]
