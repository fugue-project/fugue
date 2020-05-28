import base64
import json
import os
import pickle
from typing import Any, Iterable, List, Optional, Tuple

import pandas as pd
from fs import open_fs
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import DataFrame, LocalBoundedDataFrame, LocalDataFrame
from fugue.dataframe.iterable_dataframe import IterableDataFrame
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from triad.collections import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_arg_not_none
from triad.utils.assertion import assert_or_throw as aot

from triad.collections.fs import FileSystem


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
    no_pandas: bool = False,
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
            not check_schema or df.schema == df2.schema
        ), f"schema mismatch {df.schema.pa_schema}, {df2.schema.pa_schema}"
        assert (
            not check_metadata or df.metadata == df2.metadata
        ), f"metadata mismatch {df.metadata}, {df2.metadata}"
        if not check_content:
            return True
        if no_pandas:
            dd1 = [[x.__repr__()] for x in df1.as_array_iterable(type_safe=True)]
            dd2 = [[x.__repr__()] for x in df2.as_array_iterable(type_safe=True)]
            d1 = pd.DataFrame(dd1, columns=["data"])
            d2 = pd.DataFrame(dd2, columns=["data"])
        else:
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


def pickle_df(df: DataFrame) -> bytes:
    df = to_local_bounded_df(df)
    o: List[Any] = [df.schema]
    if isinstance(df, PandasDataFrame):
        o.append("p")
        o.append(df.native)
    else:
        o.append("a")
        o.append(df.as_array())
    return pickle.dumps(o)


def serialize_df(
    df: Optional[DataFrame],
    threshold: int = -1,
    file_path: Optional[str] = None,
    fs: Optional[FileSystem] = None,
) -> str:
    if df is None:
        return json.dumps(dict())
    data = pickle_df(df)
    size = len(data)
    if threshold < 0 or size <= threshold:
        res = dict(data=base64.b64encode(data).decode())
    else:
        if file_path is None:
            raise InvalidOperationError("file_path is not provided")
        if fs is None:
            with open_fs(
                os.path.dirname(file_path), writeable=True, create=False
            ) as fs:
                fs.writebytes(os.path.basename(file_path), data)
        else:
            fs.writebytes(file_path, data)
        res = dict(path=file_path)
    return json.dumps(res)


def unpickle_df(stream: bytes) -> LocalBoundedDataFrame:
    o = pickle.loads(stream)
    schema = o[0]
    if o[1] == "p":
        return PandasDataFrame(o[2], schema)
    if o[1] == "a":
        return ArrayDataFrame(o[2], schema)
    raise NotImplementedError(  # pragma: no cover
        f"{o[1]} is not supported for unpickle"
    )


def deserialize_df(
    json_str: str, fs: Optional[FileSystem] = None
) -> Optional[LocalBoundedDataFrame]:
    d = json.loads(json_str)
    if len(d) == 0:
        return None
    if "data" in d:
        return unpickle_df(base64.b64decode(d["data"].encode()))
    elif "path" in d:
        if fs is None:
            with open_fs(os.path.dirname(d["path"]), create=False) as fs:
                return unpickle_df(fs.readbytes(os.path.basename(d["path"])))
        return unpickle_df(fs.readbytes(d["path"]))
    raise ValueError(f"{json_str} is invalid")


def get_join_schemas(
    df1: DataFrame, df2: DataFrame, how: str, on: Iterable[str]
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
    on = list(on)
    aot(len(on) == len(set(on)), f"{on} has duplication")
    schema2 = df2.schema
    aot(
        how != "outer",
        ValueError(
            "'how' must use left_outer, right_outer, full_outer for outer joins"
        ),
    )
    if how in ["semi", "left_semi", "anti", "left_anti"]:
        schema2 = schema2.extract(on)
    aot(
        on in df1.schema and on in schema2,
        KeyError(f"{on} is not the intersection of {df1.schema} & {df2.schema}"),
    )
    cm = df1.schema.intersect(on)
    if how == "cross":
        aot(
            len(df1.schema.intersect(schema2)) == 0,
            KeyError("can't specify on for cross join"),
        )
    else:
        aot(len(on) > 0, KeyError("on must be specified"))
    return cm, (df1.schema.union(schema2))
