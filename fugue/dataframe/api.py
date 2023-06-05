from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
from triad.collections.schema import Schema
from triad.utils.rename import normalize_names

from .._utils.registry import fugue_plugin
from .dataframe import AnyDataFrame, DataFrame, as_fugue_df


@fugue_plugin
def is_df(df: Any) -> bool:
    """Whether ``df`` is a DataFrame like object"""
    return isinstance(df, DataFrame)


def get_native_as_df(df: AnyDataFrame) -> AnyDataFrame:
    """Return the dataframe form of the input ``df``.
    If ``df`` is a :class:`~.DataFrame`, then call the
    :meth:`~.DataFrame.native_as_df`, otherwise, it depends on whether there is
    a correspondent function handling it.
    """
    if isinstance(df, DataFrame):
        return df.native_as_df()
    if is_df(df):
        return df
    raise NotImplementedError(f"cannot get a dataframe like object from {type(df)}")


@fugue_plugin
def get_schema(df: AnyDataFrame) -> Schema:
    """Get the schema of the ``df``

    :param df: the object that can be recognized as a dataframe by Fugue
    :return: the Schema object
    """
    return as_fugue_df(df).schema


@fugue_plugin
def as_pandas(df: AnyDataFrame) -> pd.DataFrame:
    """Convert ``df`` to a Pandas DataFrame

    :param df: the object that can be recognized as a dataframe by Fugue
    :return: the Pandas DataFrame
    """
    return as_fugue_df(df).as_pandas()


@fugue_plugin
def as_arrow(df: AnyDataFrame) -> pa.Table:
    """Convert ``df`` to a PyArrow Table

    :param df: the object that can be recognized as a dataframe by Fugue
    :return: the PyArrow Table
    """
    return as_fugue_df(df).as_arrow()


@fugue_plugin
def as_array(
    df: AnyDataFrame, columns: Optional[List[str]] = None, type_safe: bool = False
) -> List[Any]:  # pragma: no cover
    """Convert df to 2-dimensional native python array

    :param df: the object that can be recognized as a dataframe by Fugue
    :param columns: columns to extract, defaults to None
    :param type_safe: whether to ensure output conforms with its schema,
        defaults to False
    :return: 2-dimensional native python array

    .. note::

        If ``type_safe`` is False, then the returned values are 'raw' values.
    """
    return as_fugue_df(df).as_array(columns=columns, type_safe=type_safe)


@fugue_plugin
def as_array_iterable(
    df: AnyDataFrame, columns: Optional[List[str]] = None, type_safe: bool = False
) -> Iterable[Any]:  # pragma: no cover
    """Convert df to iterable of native python arrays

    :param df: the object that can be recognized as a dataframe by Fugue
    :param columns: columns to extract, defaults to None
    :param type_safe: whether to ensure output conforms with its schema,
        defaults to False
    :return: iterable of native python arrays

    .. note::

        If ``type_safe`` is False, then the returned values are 'raw' values.
    """

    return as_fugue_df(df).as_array_iterable(columns=columns, type_safe=type_safe)


@fugue_plugin
def as_dict_iterable(
    df: AnyDataFrame, columns: Optional[List[str]] = None
) -> Iterable[Dict[str, Any]]:
    """Convert df to iterable of native python dicts

    :param df: the object that can be recognized as a dataframe by Fugue
    :param columns: columns to extract, defaults to None
    :return: iterable of native python dicts

    .. note::

        The default implementation enforces ``type_safe`` True
    """
    return as_fugue_df(df).as_dict_iterable(columns=columns)


@fugue_plugin
def peek_array(df: AnyDataFrame) -> List[Any]:
    """Peek the first row of the dataframe as an array

    :param df: the object that can be recognized as a dataframe by Fugue
    :return: the first row as an array
    """
    return as_fugue_df(df).peek_array()


@fugue_plugin
def peek_dict(df: AnyDataFrame) -> Dict[str, Any]:
    """Peek the first row of the dataframe as a array

    :param df: the object that can be recognized as a dataframe by Fugue
    :return: the first row as a dict
    """
    return as_fugue_df(df).peek_dict()


@fugue_plugin
def head(
    df: AnyDataFrame,
    n: int,
    columns: Optional[List[str]] = None,
    as_fugue: bool = False,
) -> AnyDataFrame:
    """Get first n rows of the dataframe as a new local bounded dataframe

    :param n: number of rows
    :param columns: selected columns, defaults to None (all columns)
    :param as_fugue: whether return a Fugue :class:`~.DataFrame`, default to
        False. If False, then if the input ``df`` is not a Fugue DataFrame
        then it will return the underlying DataFrame object.
    :return: a local bounded dataframe
    """
    res = as_fugue_df(df).head(n=n, columns=columns)
    if as_fugue or isinstance(df, DataFrame):
        return res
    return res.native_as_df()


@fugue_plugin
def alter_columns(
    df: AnyDataFrame, columns: Any, as_fugue: bool = False
) -> AnyDataFrame:
    """Change column types

    :param df: the object that can be recognized as a dataframe by Fugue
    :param columns: |SchemaLikeObject|,
        all columns should be contained by the dataframe schema
    :param as_fugue: whether return a Fugue :class:`~.DataFrame`, default to
        False. If False, then if the input ``df`` is not a Fugue DataFrame
        then it will return the underlying DataFrame object.
    :return: a new dataframe with altered columns, the order of the
        original schema will not change
    """
    return _convert_df(df, as_fugue_df(df).alter_columns(columns), as_fugue=as_fugue)


@fugue_plugin
def drop_columns(
    df: AnyDataFrame, columns: List[str], as_fugue: bool = False
) -> AnyDataFrame:
    """Drop certain columns and return a new dataframe

    :param df: the object that can be recognized as a dataframe by Fugue
    :param columns: columns to drop
    :param as_fugue: whether return a Fugue :class:`~.DataFrame`, default to
        False. If False, then if the input ``df`` is not a Fugue DataFrame
        then it will return the underlying DataFrame object.
    :return: a new dataframe removing the columns
    """
    return _convert_df(df, as_fugue_df(df).drop(columns), as_fugue=as_fugue)


@fugue_plugin
def select_columns(
    df: AnyDataFrame, columns: List[Any], as_fugue: bool = False
) -> AnyDataFrame:
    """Select certain columns and return a new dataframe

    :param df: the object that can be recognized as a dataframe by Fugue
    :param columns: columns to return
    :param as_fugue: whether return a Fugue :class:`~.DataFrame`, default to
        False. If False, then if the input ``df`` is not a Fugue DataFrame
        then it will return the underlying DataFrame object.
    :return: a new dataframe with the selected the columns
    """
    return _convert_df(df, as_fugue_df(df)[columns], as_fugue=as_fugue)


@fugue_plugin
def get_column_names(df: AnyDataFrame) -> List[Any]:  # pragma: no cover
    """A generic function to get column names of any dataframe

    :param df: the dataframe object
    :return: the column names

    .. note::

        In order to support a new type of dataframe, an implementation must
        be registered, for example

        .. code-block::python

            @get_column_names.candidate(lambda df: isinstance(df, pa.Table))
            def _get_pyarrow_dataframe_columns(df: pa.Table) -> List[Any]:
                return [f.name for f in df.schema]
    """
    return get_schema(df).names


@fugue_plugin
def rename(
    df: AnyDataFrame, columns: Dict[str, Any], as_fugue: bool = False
) -> AnyDataFrame:
    """A generic function to rename column names of any dataframe

    :param df: the dataframe object
    :param columns: the rename operations as a dict: ``old name => new name``
    :param as_fugue: whether return a Fugue :class:`~.DataFrame`, default to
        False. If False, then if the input ``df`` is not a Fugue DataFrame
        then it will return the underlying DataFrame object.
    :return: the renamed dataframe

    .. note::

        In order to support a new type of dataframe, an implementation must
        be registered, for example

        .. code-block::python

            @rename.candidate(
                lambda df, *args, **kwargs: isinstance(df, pd.DataFrame)
            )
            def _rename_pandas_dataframe(
                df: pd.DataFrame, columns: Dict[str, Any]
            ) -> pd.DataFrame:
                if len(columns) == 0:
                    return df
                return df.rename(columns=columns)
    """
    if len(columns) == 0:
        return df
    return _convert_df(df, as_fugue_df(df).rename(columns), as_fugue=as_fugue)


def normalize_column_names(df: AnyDataFrame) -> Tuple[AnyDataFrame, Dict[str, Any]]:
    """A generic function to normalize any dataframe's column names to follow
    Fugue naming rules

    .. note::

        This is a temporary solution before
        :class:`~triad:triad.collections.schema.Schema`
        can take arbitrary names

    .. admonition:: Examples

        * ``[0,1]`` => ``{"_0":0, "_1":1}``
        * ``["1a","2b"]`` => ``{"_1a":"1a", "_2b":"2b"}``
        * ``["*a","-a"]`` => ``{"_a":"*a", "_a_1":"-a"}``

    :param df: a dataframe object
    :return: the renamed dataframe and the rename operations as a dict that
        can **undo** the change

    .. seealso::

        * :func:`~.get_column_names`
        * :func:`~.rename`
        * :func:`~triad:triad.utils.rename.normalize_names`
    """
    cols = get_column_names(df)
    names = normalize_names(cols)
    if len(names) == 0:
        return df, {}
    undo = {v: k for k, v in names.items()}
    return (rename(df, names), undo)


def _convert_df(
    input_df: AnyDataFrame, output_df: DataFrame, as_fugue: bool
) -> AnyDataFrame:
    if as_fugue or isinstance(input_df, DataFrame):
        return output_df
    return output_df.native_as_df()
