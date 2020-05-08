from typing import Any, Iterable, List, Tuple

import pandas as pd
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import DataFrame, LocalBoundedDataFrame, LocalDataFrame
from fugue.dataframe.iterable_dataframe import IterableDataFrame
from fugue.dataframe.pandas_dataframes import PandasDataFrame
from triad.collections import Schema
from triad.utils.assertion import assert_arg_not_none, assert_or_throw as aot


def _df_eq(
    df: DataFrame,
    data: Any,
    schema: Any = None,
    metadata: Any = None,
    digits=8,
    check_order: bool = False,
    check_schema: bool = True,
    check_content: bool = True,
    check_metadata: bool = True,
    throw=False,
) -> bool:
    """_df_eq is for internal, local test purpose only. DO NOT use
    it on critical or expensive tasks.
    """
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
        if not check_order:
            d1 = d1.sort_values(df1.schema.names)
            d2 = d2.sort_values(df1.schema.names)
        d1 = d1.reset_index(drop=True)
        d2 = d2.reset_index(drop=True)
        pd.testing.assert_frame_equal(
            d1, d2, check_less_precise=digits, check_dtype=False
        )
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


def get_join_schemas(
    df1: DataFrame, df2: DataFrame, how: str, keys: Iterable[str]
) -> Tuple[Schema, Schema]:
    assert_arg_not_none(how, "how")
    how = how.lower()
    aot(
        how
        in [
            "semi",
            "left_semi",
            "anti",
            "left_anti",
            "inner",
            "left_outer",
            "right_outer",
            "full_outer",
            "cross",
        ],
        ValueError(f"{how} is not a valid join type"),
    )
    keys = list(keys)
    aot(len(keys) == len(set(keys)), f"{keys} has duplication")
    schema2 = df2.schema
    aot(
        how != "outer",
        ValueError(
            "'how' must use left_outer, right_outer, full_outer for outer joins"
        ),
    )
    if how in ["semi", "left_semi", "anti", "left_anti"]:
        schema2 = schema2.extract(keys)
    aot(
        keys in df1.schema and keys in schema2,
        KeyError(f"{keys} is not the intersection of {df1.schema} & {df2.schema}"),
    )
    cm = df1.schema.intersect(keys)
    if how == "cross":
        aot(
            len(df1.schema.intersect(schema2)) == 0,
            KeyError("can't specify keys for cross join"),
        )
    else:
        aot(len(keys) > 0, KeyError("keys must be specified"))
    return cm, (df1.schema.union(schema2))
