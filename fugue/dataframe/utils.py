import pickle
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
from fsspec import AbstractFileSystem
from triad import Schema, assert_or_throw
from triad.collections.schema import SchemaError
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_arg_not_none
from triad.utils.assertion import assert_or_throw as aot
from triad.utils.io import url_to_fs
from triad.utils.pyarrow import pa_batch_to_dicts

from .api import as_fugue_df, get_column_names, normalize_column_names, rename
from .dataframe import DataFrame, LocalBoundedDataFrame

# For backward compatibility, TODO: remove!
get_dataframe_column_names = get_column_names
normalize_dataframe_column_names = normalize_column_names
rename_dataframe_column_names = rename


def _df_eq(
    df: DataFrame,
    data: Any,
    schema: Any = None,
    digits=8,
    check_order: bool = False,
    check_schema: bool = True,
    check_content: bool = True,
    no_pandas: bool = False,
    equal_type_groups: Optional[List[List[Any]]] = None,
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
    :param equal_type_groups: the groups to treat as equal types, defaults to None.
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
        assert not check_schema or df.schema.is_like(
            df2.schema, equal_groups=equal_type_groups
        ), f"schema mismatch {df.schema.pa_schema}, {df2.schema.pa_schema}"
        if not check_content:
            return True
        cols: Any = df1.columns
        if no_pandas:
            dd1 = [[x.__repr__()] for x in df1.as_array_iterable(type_safe=True)]
            dd2 = [[x.__repr__()] for x in df2.as_array_iterable(type_safe=True)]
            d1 = pd.DataFrame(dd1, columns=["data"])
            d2 = pd.DataFrame(dd2, columns=["data"])
            cols = ["data"]
        else:
            d1 = df1.as_pandas()
            d2 = df2.as_pandas()
        if not check_order:
            d1 = d1.sort_values(cols)
            d2 = d2.sort_values(cols)
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


def serialize_df(
    df: Optional[DataFrame],
    threshold: int = -1,
    file_path: Optional[str] = None,
) -> Optional[bytes]:
    """Serialize input dataframe to base64 string or to file
    if it's larger than threshold

    :param df: input DataFrame
    :param threshold: file byte size threshold, defaults to -1
    :param file_path: file path to store the data (used only if the serialized data
      is larger than ``threshold``), defaults to None
    :raises InvalidOperationError: if file is large but ``file_path`` is not provided
    :return: a pickled blob either containing the data or the file path
    """
    if df is None:
        return None
    data = pickle.dumps(df.as_local_bounded())
    size = len(data)
    if threshold < 0 or size <= threshold:
        return data
    else:
        if file_path is None:
            raise InvalidOperationError("file_path is not provided")
        fs, path = url_to_fs(file_path)
        with fs.open(path, "wb") as f:
            f.write(data)
        return pickle.dumps(file_path)


def deserialize_df(
    data: Optional[bytes], fs: Optional[AbstractFileSystem] = None
) -> Optional[LocalBoundedDataFrame]:
    """Deserialize json string to
    :class:`~fugue.dataframe.dataframe.LocalBoundedDataFrame`

    :param json_str: json string containing the base64 data or a file path
    :param fs: the file system to use, defaults to None
    :raises ValueError: if the json string is invalid, not generated from
      :func:`~.serialize_df`
    :return: :class:`~fugue.dataframe.dataframe.LocalBoundedDataFrame` if ``json_str``
      contains a dataframe or None if its valid but contains no data
    """
    if data is None:
        return None
    obj = pickle.loads(data)
    if isinstance(obj, LocalBoundedDataFrame):
        return obj
    elif isinstance(obj, str):
        fs, path = url_to_fs(obj)
        with fs.open(path, "rb") as f:
            return pickle.load(f)
    raise ValueError("data is invalid")


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


def pa_table_as_array_iterable(
    df: pa.Table, columns: Optional[List[str]] = None
) -> Iterable[List[List[Any]]]:
    """Convert a pyarrow table to an iterable of list

    :param df: pyarrow table
    :param columns: if not None, only these columns will be returned, defaults to None
    :return: an iterable of list
    """
    assert_or_throw(columns is None or len(columns) > 0, ValueError("empty columns"))
    _df = df if columns is None or len(columns) == 0 else df.select(columns)
    for batch in _df.to_batches():
        for x in zip(*batch.to_pydict().values()):
            yield list(x)


def pa_table_as_array(
    df: pa.Table, columns: Optional[List[str]] = None
) -> List[List[List[Any]]]:
    """Convert a pyarrow table to a list of list

    :param df: pyarrow table
    :param columns: if not None, only these columns will be returned, defaults to None
    :return: a list of list
    """
    return list(pa_table_as_array_iterable(df, columns=columns))


def pa_table_as_dict_iterable(
    df: pa.Table, columns: Optional[List[str]] = None
) -> Iterable[Dict[str, Any]]:
    """Convert a pyarrow table to an iterable of dict

    :param df: pyarrow table
    :param columns: if not None, only these columns will be returned, defaults to None
    :return: an iterable of dict
    """
    for ck in _pa_table_as_dicts_chunks(df, columns=columns):
        yield from ck


def pa_table_as_dicts(
    df: pa.Table, columns: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Convert a pyarrow table to a list of dict

    :param df: pyarrow table
    :param columns: if not None, only these columns will be returned, defaults to None
    :return: a list of dict
    """
    res: List[Dict[str, Any]] = []
    for ck in _pa_table_as_dicts_chunks(df, columns=columns):
        res += ck
    return res


def _pa_table_as_dicts_chunks(
    df: pa.Table, columns: Optional[List[str]] = None
) -> Iterable[List[Dict[str, Any]]]:
    assert_or_throw(columns is None or len(columns) > 0, ValueError("empty columns"))
    _df = df if columns is None or len(columns) == 0 else df.select(columns)
    for batch in _df.to_batches():
        yield pa_batch_to_dicts(batch)
