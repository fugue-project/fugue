from typing import Any, Iterable, List

import pandas as pd
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import DataFrame, LocalBoundedDataFrame, LocalDataFrame
from fugue.dataframe.iterable_dataframe import IterableDataFrame
from fugue.dataframe.pandas_dataframes import PandasDataFrame
from triad.utils.assertion import assert_arg_not_none
from triad.utils.assertion import assert_or_throw as aot


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


def to_local_df(df: Any, schema: Any = None, metadata: Any = None) -> LocalDataFrame:
    assert_arg_not_none(df, "df")
    if isinstance(df, DataFrame):
        aot(
            schema is None and metadata is None,
            ValueError("schema and metadata must be None when df is a DataFrame"),
        )
        return df.as_local()
    if isinstance(df, pd.DataFrame):
        return PandasDataFrame(df, schema, metadata)
    if isinstance(df, List):
        return ArrayDataFrame(df, schema, metadata)
    if isinstance(df, Iterable):
        return IterableDataFrame(df, schema, metadata)
    raise TypeError(f"{df} cannot convert to a LocalDataFrame")


def to_local_bounded_df(
    df: Any, schema: Any = None, metadata: Any = None
) -> LocalBoundedDataFrame:
    df = to_local_df(df, schema, metadata)
    if isinstance(df, LocalBoundedDataFrame):
        return df
    return ArrayDataFrame(df.as_array(), df.schema, df.metadata)
