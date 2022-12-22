from typing import Any, List, Optional, Union

from ..collections.partition import PartitionSpec
from ..dataframe.dataframe import DataFrame
from .factory import make_execution_engine


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


def distinct(
    df: Any, engine: Any = None, engine_conf: Any = None, as_fugue: bool = False
) -> Any:
    """Equivalent to ``SELECT DISTINCT * FROM df``

    :param df: an input dataframe that can be recognized by Fugue
    :return: [description]
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.distinct(e.to_df(df))
    return _adjust_df([df], edf, as_fugue=as_fugue)


def dropna(
    df: Any,
    how: str = "any",
    thresh: int = None,
    subset: List[str] = None,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Drop NA recods from dataframe

    :param df: an input dataframe that can be recognized by Fugue
    :param how: 'any' or 'all'. 'any' drops rows that contain any nulls.
        'all' drops rows that contain all nulls.
    :param thresh: int, drops rows that have less than thresh non-null values
    :param subset: list of columns to operate on

    :return: DataFrame with NA records dropped
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.dropna(e.to_df(df), how=how, thresh=thresh, subset=subset)
    return _adjust_df([df], edf, as_fugue=as_fugue)


def fillna(
    df: Any,
    value: Any,
    subset: List[str] = None,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """
    Fill ``NULL``, ``NAN``, ``NAT`` values in a dataframe

    :param df: an input dataframe that can be recognized by Fugue
    :param value: if scalar, fills all columns with same value.
        if dictionary, fills NA using the keys as column names and the
        values as the replacement values.
    :param subset: list of columns to operate on. ignored if value is
        a dictionary

    :return: DataFrame with NA records filled
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.fillna(e.to_df(df), value=value, subset=subset)
    return _adjust_df([df], edf, as_fugue=as_fugue)


def sample(
    df: Any,
    n: Optional[int] = None,
    frac: Optional[float] = None,
    replace: bool = False,
    seed: Optional[int] = None,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """
    Sample dataframe by number of rows or by fraction

    :param df: an input dataframe that can be recognized by Fugue
    :param n: number of rows to sample, one and only one of ``n`` and ``frac``
        must be set
    :param frac: fraction [0,1] to sample, one and only one of ``n`` and ``frac``
        must be set
    :param replace: whether replacement is allowed. With replacement,
        there may be duplicated rows in the result, defaults to False
    :param seed: seed for randomness, defaults to None

    :return: the sampled dataframe
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.sample(e.to_df(df), n=n, frac=frac, replace=replace, seed=seed)
    return _adjust_df([df], edf, as_fugue=as_fugue)


def take(
    df: Any,
    n: int,
    presort: str,
    na_position: str = "last",
    partition_spec: Optional[PartitionSpec] = None,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """
    Get the first n rows of a DataFrame per partition. If a presort is defined,
    use the presort before applying take. presort overrides partition_spec.presort.
    The Fugue implementation of the presort follows Pandas convention of specifying
    NULLs first or NULLs last. This is different from the Spark and SQL convention
    of NULLs as the smallest value.

    :param df: an input dataframe that can be recognized by Fugue
    :param n: number of rows to return
    :param presort: presort expression similar to partition presort
    :param na_position: position of null values during the presort.
        can accept ``first`` or ``last``
    :param partition_spec: PartitionSpec to apply the take operation,
        defaults to None

    :return: n rows of DataFrame per partition
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.take(
        e.to_df(df),
        n=n,
        presort=presort,
        na_position=na_position,
        partition_spec=partition_spec,
    )
    return _adjust_df([df], edf, as_fugue=as_fugue)


def load(
    path: Union[str, List[str]],
    format_hint: Any = None,
    columns: Any = None,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    **kwargs: Any,
) -> Any:
    """Load dataframe from persistent storage

    :param path: the path to the dataframe
    :param format_hint: can accept ``parquet``, ``csv``, ``json``,
        defaults to None, meaning to infer
    :param columns: list of columns or a |SchemaLikeObject|, defaults to None
    :param kwargs: parameters to pass to the underlying framework
    :return: an engine compatible dataframe

    For more details and examples, read |ZipComap|.
    """
    e = make_execution_engine(engine, engine_conf)
    res = e.load_df(path=path, format_hint=format_hint, columns=columns, **kwargs)
    return _adjust_df([], res, as_fugue=as_fugue)


def save(
    df: Any,
    path: str,
    format_hint: Any = None,
    mode: str = "overwrite",
    partition_spec: Optional[PartitionSpec] = None,
    force_single: bool = False,
    engine: Any = None,
    engine_conf: Any = None,
    **kwargs: Any,
) -> None:
    """Save dataframe to a persistent storage

    :param df: an input dataframe that can be recognized by Fugue
    :param path: output path
    :param format_hint: can accept ``parquet``, ``csv``, ``json``,
        defaults to None, meaning to infer
    :param mode: can accept ``overwrite``, ``append``, ``error``,
        defaults to "overwrite"
    :param partition_spec: how to partition the dataframe before saving,
        defaults to empty
    :param force_single: force the output as a single file, defaults to False
    :param kwargs: parameters to pass to the underlying framework

    For more details and examples, read |LoadSave|.
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df])
    edf = e.to_df(df)
    e.save_df(
        edf,
        path=path,
        format_hint=format_hint,
        mode=mode,
        partition_spec=partition_spec,
        force_single=force_single,
        **kwargs,
    )


def join(
    df1: Any,
    df2: Any,
    *dfs: Any,
    how: str,
    on: Optional[List[str]] = None,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Join two dataframes

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
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
    res = e.join(edf1, edf2, how=how, on=on)
    for odf in dfs:
        res = e.join(res, e.to_df(odf), how=how, on=on)
    return _adjust_df([df1, df2, *dfs], res, as_fugue=as_fugue)


def union(
    df1: Any,
    df2: Any,
    *dfs: Any,
    distinct: bool = True,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Join two dataframes

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to union
    :param distinct: ``true`` for ``UNION`` (== ``UNION DISTINCT``),
        ``false`` for ``UNION ALL``
    :return: the unioned dataframe

    .. note::

        Currently, the schema of all dataframes must be identical, or
        an exception will be thrown.
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df1, df2])
    edf1 = e.to_df(df1)
    edf2 = e.to_df(df2)
    res = e.union(edf1, edf2, distinct=distinct)
    for odf in dfs:
        res = e.union(res, e.to_df(odf), distinct=distinct)
    return _adjust_df([df1, df2, *dfs], res, as_fugue=as_fugue)


def subtract(
    df1: Any,
    df2: Any,
    *dfs: Any,
    distinct: bool = True,
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """``df1 - df2``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to subtract
    :param distinct: ``true`` for ``EXCEPT`` (== ``EXCEPT DISTINCT``),
        ``false`` for ``EXCEPT ALL``
    :return: the unioned dataframe

    .. note::

        Currently, the schema of all datafrmes must be identical, or
        an exception will be thrown.
    """
    e = make_execution_engine(engine, engine_conf, infer_by=[df1, df2])
    edf1 = e.to_df(df1)
    edf2 = e.to_df(df2)
    res = e.subtract(edf1, edf2, distinct=distinct)
    for odf in dfs:
        res = e.subtract(edf1, e.to_df(odf), distinct=distinct)
    return _adjust_df([df1, df2, *dfs], res, as_fugue=as_fugue)


def intersect(
    df1: Any,
    df2: Any,
    *dfs: Any,
    distinct: bool = True,  # pylint: disable-all
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
) -> Any:
    """Intersect ``df1`` and ``df2``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to intersect with
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
    res = e.intersect(edf1, edf2, distinct=distinct)
    for odf in dfs:
        res = e.intersect(res, e.to_df(odf), distinct=distinct)
    return _adjust_df([df1, df2, *dfs], res, as_fugue=as_fugue)


def _adjust_df(input_dfs: Any, output_df: DataFrame, as_fugue: bool) -> Any:
    if as_fugue or any(isinstance(x, DataFrame) for x in input_dfs):
        return output_df
    return output_df.native  # type: ignore
