from ..collections.partition import PartitionSpec
from ..dataframe.dataframe import DataFrame
from .factory import make_execution_engine
from typing import Any, Optional, List


def repartition(
    df: Any,
    partition_spec: PartitionSpec,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Partition the input dataframe using ``partition_spec``.

    :param df: an input dataframe that can be recognized by Fugue
    :param partition_spec: how you want to partition the dataframe
    :return: the repartitioned dataframe
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.to_df(df)
    return _adjust_df(
        [df], e.repartition(edf, partition_spec=partition_spec), as_fugue=as_fugue
    )


def broadcast(
    df: Any,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Broadcast the dataframe to all workers for a distributed computing framework

    :param df: an input dataframe that can be recognized by Fugue
    :return: the broadcasted dataframe
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.to_df(df)
    return _adjust_df([df], e.broadcast(edf), as_fugue=as_fugue)


def persist(
    df: Any,
    lazy: bool = False,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    **kwargs: Any,
) -> Any:
    """Force materializing and caching the dataframe

    :param df: an input dataframe that can be recognized by Fugue
    :param lazy: ``True``: first usage of the output will trigger persisting
        to happen; ``False`` (eager): persist is forced to happend immediately.
        Default to ``False``
    :param kwargs: parameter to pass to the underlying persist implementation
    :return: the persisted dataframe
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.to_df(df)
    return _adjust_df([df], e.persist(edf, lazy=lazy, **kwargs), as_fugue=as_fugue)


def join(
    df1: Any,
    df2: Any,
    how: str,
    on: Optional[List[str]] = None,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Join two dataframes

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param how: can accept ``semi``, ``left_semi``, ``anti``, ``left_anti``,
        ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
    :param on: it can always be inferred, but if you provide, it will be
        validated against the inferred keys.
    :return: the joined dataframe

    .. note::

        Please read :func:`~.fugue.dataframe.utils.get_join_schemas`
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df1, df2])
    edf1 = e.to_df(df1)
    edf2 = e.to_df(df2)
    return _adjust_df([df1, df2], e.join(edf1, edf2, how=how, on=on), as_fugue=as_fugue)


def union(
    df1: Any,
    df2: Any,
    distinct: bool = True,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Join two dataframes

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param distinct: ``true`` for ``UNION`` (== ``UNION DISTINCT``),
        ``false`` for ``UNION ALL``
    :return: the unioned dataframe

    .. note::

        Currently, the schema of ``df1`` and ``df2`` must be identical, or
        an exception will be thrown.
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df1, df2])
    edf1 = e.to_df(df1)
    edf2 = e.to_df(df2)
    return _adjust_df(
        [df1, df2], e.union(edf1, edf2, distinct=distinct), as_fugue=as_fugue
    )


def subtract(
    df1: Any,
    df2: Any,
    distinct: bool = True,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """``df1 - df2``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param distinct: ``true`` for ``EXCEPT`` (== ``EXCEPT DISTINCT``),
        ``false`` for ``EXCEPT ALL``
    :return: the unioned dataframe

    .. note::

        Currently, the schema of ``df1`` and ``df2`` must be identical, or
        an exception will be thrown.
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df1, df2])
    edf1 = e.to_df(df1)
    edf2 = e.to_df(df2)
    return _adjust_df(
        [df1, df2], e.subtract(edf1, edf2, distinct=distinct), as_fugue=as_fugue
    )


def intersect(
    df1: Any,
    df2: Any,
    distinct: bool = True,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Intersect ``df1`` and ``df2``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param distinct: ``true`` for ``INTERSECT`` (== ``INTERSECT DISTINCT``),
        ``false`` for ``INTERSECT ALL``
    :return: the unioned dataframe

    .. note::

        Currently, the schema of ``df1`` and ``df2`` must be identical, or
        an exception will be thrown.
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df1, df2])
    edf1 = e.to_df(df1)
    edf2 = e.to_df(df2)
    return _adjust_df(
        [df1, df2], e.intersect(edf1, edf2, distinct=distinct), as_fugue=as_fugue
    )


def _adjust_df(input_dfs: Any, output_df: DataFrame, as_fugue: bool) -> Any:
    if as_fugue or any(isinstance(x, DataFrame) for x in input_dfs):
        return output_df
    return output_df.native  # type: ignore
