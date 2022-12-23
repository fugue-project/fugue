from contextlib import contextmanager
from typing import Any, Callable, Iterator, List, Optional, Union
from triad import assert_or_throw
from ..collections.partition import PartitionSpec
from ..dataframe.dataframe import DataFrame
from .execution_engine import ExecutionEngine, _FUGUE_GLOBAL_EXECUTION_ENGINE_CONTEXT
from .factory import make_execution_engine


@contextmanager
def engine_context(
    engine: Any = None, engine_conf: Any = None, infer_by: Optional[List[Any]] = None
) -> Iterator[ExecutionEngine]:
    """Make an execution engine and set it as the context engine. This function
    is thread safe and async safe.

    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param infer_by: a list of objects to infer the engine, defaults to None

    .. note::

        For more details, please read
        :func:`~.fugue.execution.factory.make_execution_engine`

    .. admonition:: Examples

        .. code-block:: python

            import fugue.api as fa

            with fa.engine_context(spark_session):
                transform(df, func)  # will use spark in this transformation

    """
    e = make_execution_engine(engine, engine_conf, infer_by=infer_by)
    return e._as_context()


def set_global_engine(engine: Any, engine_conf: Any = None) -> ExecutionEngine:
    """Make an execution engine and set it as the global execution engine

    :param engine: an engine like object, must not be None
    :param engine_conf: the configs for the engine, defaults to None

    .. caution::

        In general, it is not a good practice to set a global engine. You should
        consider :func:`~.engine_context` instead. The exception
        is when you iterate in a notebook and cross cells, this could simplify
        the code.

    .. note::

        For more details, please read
        :func:`~.fugue.execution.factory.make_execution_engine` and
        :meth:`~fugue.execution.execution_engine.ExecutionEngine.set_global`

    .. admonition:: Examples

        .. code-block:: python

            import fugue.api as fa

            fa.set_global_engine(spark_session)
            transform(df, func)  # will use spark in this transformation
            fa.clear_global_engine()  # remove the global setting
    """
    assert_or_throw(engine is not None, ValueError("engine must be specified"))
    return make_execution_engine(engine, engine_conf).set_global()


def clear_global_engine() -> None:
    """Remove the global exeuction engine (if set)"""
    _FUGUE_GLOBAL_EXECUTION_ENGINE_CONTEXT.set(None)


def get_current_engine() -> ExecutionEngine:
    """Get the current execution engine. Regarding the order of the logic
    please read :func:`~.fugue.execution.factory.make_execution_engine`
    """
    return make_execution_engine()


def run_engine_function(
    func: Callable[[ExecutionEngine], Any],
    engine: Any = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    infer_by: Optional[List[Any]] = None,
) -> Any:
    with engine_context(engine, engine_conf, infer_by=infer_by) as e:
        res = func(e)

        if isinstance(res, DataFrame):
            if as_fugue or any(isinstance(x, DataFrame) for x in (infer_by or [])):
                return res
            return res.native_as_df()
        return res


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
    return run_engine_function(
        lambda e: e.repartition(e.to_df(df), partition_spec=partition_spec),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
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
    return run_engine_function(
        lambda e: e.broadcast(e.to_df(df)),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
    )


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
    return run_engine_function(
        lambda e: e.persist(e.to_df(df), lazy=lazy, **kwargs),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
    )


def distinct(
    df: Any, engine: Any = None, engine_conf: Any = None, as_fugue: bool = False
) -> Any:
    """Equivalent to ``SELECT DISTINCT * FROM df``

    :param df: an input dataframe that can be recognized by Fugue
    :return: [description]
    """
    return run_engine_function(
        lambda e: e.distinct(e.to_df(df)),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
    )


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
    return run_engine_function(
        lambda e: e.dropna(e.to_df(df), how=how, thresh=thresh, subset=subset),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
    )


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
    return run_engine_function(
        lambda e: e.fillna(e.to_df(df), value=value, subset=subset),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
    )


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
    return run_engine_function(
        lambda e: e.sample(e.to_df(df), n=n, frac=frac, replace=replace, seed=seed),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
    )


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

    return run_engine_function(
        lambda e: e.take(
            e.to_df(df),
            n=n,
            presort=presort,
            na_position=na_position,
            partition_spec=partition_spec,
        ),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
    )


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
    return run_engine_function(
        lambda e: e.load_df(
            path=path, format_hint=format_hint, columns=columns, **kwargs
        ),
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
    )


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
    run_engine_function(
        lambda e: e.save_df(
            e.to_df(df),
            path=path,
            format_hint=format_hint,
            mode=mode,
            partition_spec=partition_spec,
            force_single=force_single,
            **kwargs,
        ),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
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

    def _join(e: ExecutionEngine):
        edf1 = e.to_df(df1)
        edf2 = e.to_df(df2)
        res = e.join(edf1, edf2, how=how, on=on)
        for odf in dfs:
            res = e.join(res, e.to_df(odf), how=how, on=on)
        return res

    return run_engine_function(
        _join,
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        infer_by=[df1, df2, *dfs],
    )


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

    def _union(e: ExecutionEngine):
        edf1 = e.to_df(df1)
        edf2 = e.to_df(df2)
        res = e.union(edf1, edf2, distinct=distinct)
        for odf in dfs:
            res = e.union(res, e.to_df(odf), distinct=distinct)
        return res

    return run_engine_function(
        _union,
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        infer_by=[df1, df2, *dfs],
    )


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

    def _subtract(e: ExecutionEngine):
        edf1 = e.to_df(df1)
        edf2 = e.to_df(df2)
        res = e.subtract(edf1, edf2, distinct=distinct)
        for odf in dfs:
            res = e.subtract(res, e.to_df(odf), distinct=distinct)
        return res

    return run_engine_function(
        _subtract,
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        infer_by=[df1, df2, *dfs],
    )


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

    def _intersect(e: ExecutionEngine):
        edf1 = e.to_df(df1)
        edf2 = e.to_df(df2)
        res = e.intersect(edf1, edf2, distinct=distinct)
        for odf in dfs:
            res = e.intersect(res, e.to_df(odf), distinct=distinct)
        return res

    return run_engine_function(
        _intersect,
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        infer_by=[df1, df2, *dfs],
    )


def _adjust_df(input_dfs: Any, output_df: DataFrame, as_fugue: bool) -> Any:
    if as_fugue or any(isinstance(x, DataFrame) for x in input_dfs):
        return output_df
    return output_df.native  # type: ignore
