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
from triad.collections.fs import FileSystem
from triad.collections.schema import SchemaError
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_arg_not_none
from triad.utils.assertion import assert_or_throw as aot


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
    """Compare if two dataframes are equal. Is for internal, unit test
    purpose only. It will convert both dataframes to
    :class:`~fugue.dataframe.dataframe.LocalBoundedDataFrame`, so it assumes
    both dataframes are small and fast enough to convert. DO NOT use it
    on critical or expensive tasks.

    :param df: first data frame
    :param data: :ref:`DataFrame like
      <tutorial:/tutorials/x-like.ipynb#dataframe>` object
    :param schema: :ref:`Schema like
      <tutorial:/tutorials/x-like.ipynb#schema>` object, defaults to None
    :param metadata: :ref:`Parameteres like
      <tutorial:/tutorials/x-like.ipynb#parameters>` object, defaults to None
    :param digits: precision on float number comparison, defaults to 8
    :param check_order: if to compare the row orders, defaults to False
    :param check_schema: if compare schemas, defaults to True
    :param check_content: if to compare the row values, defaults to True
    :param check_metadata: if to compare the dataframe metadatas, defaults to True
    :param no_pandas: if true, it will compare the string representations of the
      dataframes, otherwise, it will convert both to pandas dataframe to compare,
      defaults to False
    :param throw: if to throw error if not equal, defaults to False
    :return: if they equal
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
    """Convert a data structure to :class:`~fugue.dataframe.dataframe.LocalDataFrame`

    :param df: :class:`~fugue.dataframe.dataframe.DataFrame`, pandas DataFramme and
      list or iterable of arrays
    :param schema: |SchemaLikeObject|, defaults to None, it should not be set for
      :class:`~fugue.dataframe.dataframe.DataFrame` type
    :param metadata: dict-like object with string keys, defaults to  None
    :raises ValueError: if ``df`` is :class:`~fugue.dataframe.dataframe.DataFrame`
      but you set ``schema`` or ``metadata``
    :raises TypeError: if ``df`` is not compatible
    :return: the dataframe itself if it's
      :class:`~fugue.dataframe.dataframe.LocalDataFrame` else a converted one

    :Examples:

    >>> a = to_local_df([[0,'a'],[1,'b']],"a:int,b:str")
    >>> assert to_local_df(a) is a
    >>> to_local_df(SparkDataFrame([[0,'a'],[1,'b']],"a:int,b:str"))
    """
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
    """Convert a data structure to :class:`~fugue.dataframe.dataframe.LocalBoundedDataFrame`

    :param df: :class:`~fugue.dataframe.dataframe.DataFrame`, pandas DataFramme and
      list or iterable of arrays
    :param schema: |SchemaLikeObject|, defaults to None, it should not be set for
      :class:`~fugue.dataframe.dataframe.DataFrame` type
    :param metadata: dict-like object with string keys, defaults to  None
    :raises ValueError: if ``df`` is :class:`~fugue.dataframe.dataframe.DataFrame`
      but you set ``schema`` or ``metadata``
    :raises TypeError: if ``df`` is not compatible
    :return: the dataframe itself if it's
      :class:`~fugue.dataframe.dataframe.LocalBoundedDataFrame` else a converted one

    :Examples:

    >>> a = IterableDataFrame([[0,'a'],[1,'b']],"a:int,b:str")
    >>> assert isinstance(to_local_bounded_df(a), LocalBoundedDataFrame)
    >>> to_local_bounded_df(SparkDataFrame([[0,'a'],[1,'b']],"a:int,b:str"))

    :Notice:

    Compared to :func:`.to_local_df`, this function makes sure the dataframe is also
    bounded, so :class:`~fugue.dataframe.iterable_dataframe.IterableDataFrame` will be
    converted although it's local.
    """
    df = to_local_df(df, schema, metadata)
    if isinstance(df, LocalBoundedDataFrame):
        return df
    return ArrayDataFrame(df.as_array(), df.schema, df.metadata)


def pickle_df(df: DataFrame) -> bytes:
    """Pickles a dataframe to bytes array. It firstly converts the dataframe
    using :func:`.to_local_bounded_df`, and then serialize the underlying data.

    :param df: input DataFrame
    :return: pickled binary data

    :Notice:

    Be careful to use on large dataframes or non-local, un-materialized dataframes,
    it can be slow. You should always use :func:`.unpickle_df` to deserialize.
    """
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
    """Serialize input dataframe to base64 string or to file
    if it's larger than threshold

    :param df: input DataFrame
    :param threshold: file byte size threshold, defaults to -1
    :param file_path: file path to store the data (used only if the serialized data
      is larger than ``threshold``), defaults to None
    :param fs: :class:`~triad:triad.collections.fs.FileSystem`, defaults to None
    :raises InvalidOperationError: if file is large but ``file_path`` is not provided
    :return: a json string either containing the base64 data or the file path

    :Notice:

    If fs is not provided but it needs to write to disk, then it will use
    :meth:`~fs:fs.opener.registry.Registry.open_fs` to try to open the file to write.
    """
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
    """Unpickles a dataframe from bytes array.

    :param stream: binary data
    :return: unpickled dataframe

    :Notice:

    The data must be serialized by :func:`.pickle_df` to deserialize.
    """
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
    """Deserialize json string to
    :class:`~fugue.dataframe.dataframe.LocalBoundedDataFrame`

    :param json_str: json string containing the base64 data or a file path
    :param fs: :class:`~triad:triad.collections.fs.FileSystem`, defaults to None
    :raises ValueError: if the json string is invalid, not generated from
      :func:`~.serialize_df`
    :return: :class:`~fugue.dataframe.dataframe.LocalBoundedDataFrame` if ``json_str``
      contains a dataframe or None if its valid but contains no data
    """
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
    """Get :class:`~triad:triad.collections.schema.Schema` object after
    joining ``df1`` and ``df2``. If ``on`` is not empty, it's mainly for
    validation purpose.

    :param df1: first dataframe
    :param df2: second dataframe
    :param how: can accept ``semi``, ``left_semi``, ``anti``, ``left_anti``,
      ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
    :param on: it can always be inferred, but if you provide, it will be
      validated agained the inferred keys.
    :return: the pair key schema and schema after join

    :Notice:

    In Fugue, joined schema can always be inferred because it always uses the
    input dataframes' common keys as the join keys. So you must make sure to
    :meth:`~fugue.dataframe.dataframe.DataFrame.rename` to input dataframes so
    they follow this rule.
    """
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
    if how != "cross" and len(on) == 0:
        on = list(df1.schema.intersect(df2.schema.names).names)
        aot(
            len(on) > 0,
            SchemaError(f"no common columns between {df1.schema} and {df2.schema}"),
        )
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
        SchemaError(f"{on} is not the intersection of {df1.schema} & {df2.schema}"),
    )
    cm = df1.schema.intersect(on)
    if how == "cross":
        aot(
            len(df1.schema.intersect(schema2)) == 0,
            SchemaError("can't specify on for cross join"),
        )
    else:
        aot(len(on) > 0, SchemaError("on must be specified"))
    return cm, (df1.schema.union(schema2))
