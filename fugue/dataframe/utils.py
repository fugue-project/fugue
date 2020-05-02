from typing import Any, Iterable, List

import pandas as pd
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import DataFrame, LocalBoundedDataFrame, LocalDataFrame
from fugue.dataframe.iterable_dataframe import IterableDataFrame
from fugue.dataframe.pandas_dataframes import PandasDataFrame
from triad.utils.assertion import assert_arg_not_none
from triad.utils.assertion import assert_or_throw as aot


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
