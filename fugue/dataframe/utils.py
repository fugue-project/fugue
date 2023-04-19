import base64
import json
import os
import pickle
from typing import Any, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
from fs import open_fs
from triad import FileSystem, Schema
from triad.collections.schema import SchemaError
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_arg_not_none
from triad.utils.assertion import assert_or_throw as aot

from .api import get_column_names, normalize_column_names, rename, as_fugue_df
from .array_dataframe import ArrayDataFrame
from .dataframe import DataFrame, LocalBoundedDataFrame
from .pandas_dataframe import PandasDataFrame

# For backward compatibility, TODO: remove!
get_dataframe_column_names = get_column_names
normalize_dataframe_column_names = normalize_column_names
rename_dataframe_column_names = rename


def _pa_type_eq(t1: pa.DataType, t2: pa.DataType) -> bool:
    # should ignore the name difference of list
    # e.g. list<item: string> == list<l: string>
    if pa.types.is_list(t1) and pa.types.is_list(t2):  # pragma: no cover
        return _pa_type_eq(t1.value_type, t2.value_type)
    return t1 == t2


def _schema_eq(s1: Schema, s2: Schema) -> bool:
    if s1 == s2:
        return True
    return s1.names == s2.names and all(
        _pa_type_eq(f1.type, f2.type) for f1, f2 in zip(s1.fields, s2.fields)
    )


def _df_eq(
    df: DataFrame,
    data: Any,
    schema: Any = None,
    digits=8,
    check_order: bool = False,
    check_schema: bool = True,
    check_content: bool = True,
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
      <tutorial:tutorials/advanced/x-like:dataframe>` object
    :param schema: :ref:`Schema like
      <tutorial:tutorials/advanced/x-like:schema>` object, defaults to None
    :param digits: precision on float number comparison, defaults to 8
    :param check_order: if to compare the row orders, defaults to False
    :param check_schema: if compare schemas, defaults to True
    :param check_content: if to compare the row values, defaults to True
    :param no_pandas: if true, it will compare the string representations of the
      dataframes, otherwise, it will convert both to pandas dataframe to compare,
      defaults to False
    :param throw: if to throw error if not equal, defaults to False
    :return: if they equal
    """
    df1 = as_fugue_df(df).as_local_bounded()
    if schema is not None:
        df2 = as_fugue_df(data, schema=schema).as_local_bounded()
    else:
        df2 = as_fugue_df(data).as_local_bounded()
    try:
        assert (
            df1.count() == df2.count()
        ), f"count mismatch {df1.count()}, {df2.count()}"
        assert not check_schema or _schema_eq(
            df.schema, df2.schema
        ), f"schema mismatch {df.schema.pa_schema}, {df2.schema.pa_schema}"
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
            d1 = d1.sort_values(df1.columns)
            d2 = d2.sort_values(df1.columns)
        d1 = d1.reset_index(drop=True)
        d2 = d2.reset_index(drop=True)
        pd.testing.assert_frame_equal(
            d1, d2, rtol=0, atol=10 ** (-digits), check_dtype=False, check_exact=False
        )
        return True
    except AssertionError:
        if throw:
            raise
        return False


def pickle_df(df: DataFrame) -> bytes:
    """Pickles a dataframe to bytes array. It firstly converts the dataframe
    local bounded, and then serialize the underlying data.

    :param df: input DataFrame
    :return: pickled binary data

    .. note::

        Be careful to use on large dataframes or non-local, un-materialized dataframes,
        it can be slow. You should always use :func:`.unpickle_df` to deserialize.
    """
    df = df.as_local_bounded()
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

    .. note::

        If fs is not provided but it needs to write to disk, then it will use
        :meth:`~fs:fs.opener.registry.Registry.open_fs` to try to open the file to
        write.
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
            ) as _fs:
                _fs.writebytes(os.path.basename(file_path), data)
        else:
            fs.writebytes(file_path, data)
        res = dict(path=file_path)
    return json.dumps(res)


def unpickle_df(stream: bytes) -> LocalBoundedDataFrame:
    """Unpickles a dataframe from bytes array.

    :param stream: binary data
    :return: unpickled dataframe

    .. note::

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
            with open_fs(os.path.dirname(d["path"]), create=False) as _fs:
                return unpickle_df(_fs.readbytes(os.path.basename(d["path"])))
        return unpickle_df(fs.readbytes(d["path"]))
    raise ValueError(f"{json_str} is invalid")


def get_join_schemas(
    df1: DataFrame, df2: DataFrame, how: str, on: Optional[Iterable[str]]
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

    .. note::

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
    on = list(on) if on is not None else []
    aot(len(on) == len(set(on)), f"{on} has duplication")
    if how != "cross" and len(on) == 0:
        other = set(df2.columns)
        on = [c for c in df1.columns if c in other]
        aot(
            len(on) > 0,
            lambda: SchemaError(
                f"no common columns between {df1.columns} and {df2.columns}"
            ),
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
        lambda: SchemaError(
            f"{on} is not the intersection of {df1.schema} & {df2.schema}"
        ),
    )
    cm = df1.schema.intersect(on)
    if how == "cross":
        cs = df1.schema.intersect(schema2)
        aot(
            len(cs) == 0,
            SchemaError(f"invalid cross join, two dataframes have common columns {cs}"),
        )
    else:
        aot(len(on) > 0, SchemaError("join on columns must be specified"))
    return cm, (df1.schema.union(schema2))
