from typing import Any

import pandas as pd
from fugue.dataframe import DataFrame, to_local_bounded_df


def df_eq(
    df: DataFrame,
    data: Any,
    schema: Any = None,
    metadata: Any = None,
    digits=8,
    check_schema: bool = True,
    check_content: bool = True,
    check_metadata: bool = True,
    throw=False,
) -> bool:
    df1 = to_local_bounded_df(df)
    df2 = to_local_bounded_df(data, schema, metadata)
    try:
        assert (
            df1.count() == df2.count()
        ), f"count mismatch {df1.count()}, {df2.count()}"
        assert (
            not check_schema or df1.schema == df2.schema
        ), f"schema mismatch {df1.schema}, {df2.schema}"
        assert (
            not check_metadata or df1.metadata == df2.metadata
        ), f"metadata mismatch {df1.metadata}, {df2.metadata}"
        if not check_content:
            return True
        d1 = df1.as_pandas()
        d2 = df2.as_pandas()
        pd.testing.assert_frame_equal(d1, d2, check_less_precise=digits)
        return True
    except AssertionError:
        if throw:
            raise
        return False
