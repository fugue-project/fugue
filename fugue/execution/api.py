from contextlib import contextmanager
from typing import Any, Callable, Iterator, List, Optional, Union

from triad import ParamDict, assert_or_throw

from fugue.column import ColumnExpr, SelectColumns, col, lit
from fugue.constants import _FUGUE_GLOBAL_CONF
from fugue.exceptions import FugueInvalidOperation

from ..collections.partition import PartitionSpec
from ..dataframe.dataframe import AnyDataFrame, DataFrame, as_fugue_df
from .execution_engine import (
    _FUGUE_GLOBAL_EXECUTION_ENGINE_CONTEXT,
    AnyExecutionEngine,
    ExecutionEngine,
)
from .factory import make_execution_engine, try_get_context_execution_engine
from .._utils.registry import fugue_plugin


@contextmanager
def engine_context(
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    infer_by: Optional[List[Any]] = None,
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


def set_global_engine(
    engine: AnyExecutionEngine, engine_conf: Any = None
) -> ExecutionEngine:
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


def get_context_engine() -> ExecutionEngine:
    """Get the execution engine in the current context. Regarding the order of the logic
    please read :func:`~.fugue.execution.factory.make_execution_engine`
    """
    engine = try_get_context_execution_engine()
    if engine is None:
        raise FugueInvalidOperation("No global/context engine is set")
    return engine


def get_current_conf() -> ParamDict:
    """Get the current configs either in the defined engine context or by
    the global configs (see :func:`~.fugue.constants.register_global_conf`)
    """
    engine = try_get_context_execution_engine()
    if engine is not None:
        return engine.conf
    return _FUGUE_GLOBAL_CONF


def get_current_parallelism() -> int:
    """Get the current parallelism of the current global/context engine.
    If there is no global/context engine, it creates a temporary engine
    using :func:`~.fugue.execution.factory.make_execution_engine` to get
    its parallelism

    :return: the size of the parallelism
    """
    return make_execution_engine().get_current_parallelism()


@fugue_plugin
def as_fugue_engine_df(
    engine: ExecutionEngine, df: AnyDataFrame, schema: Any = None
) -> DataFrame:
    """Convert a dataframe to a Fugue engine dependent DataFrame.
    This function is used internally by Fugue. It is not recommended
    to use

    :param engine: the ExecutionEngine to use, must not be None
    :param df: a dataframe like object
    :param schema: the schema of the dataframe, defaults to None

    :return: the engine dependent DataFrame
    """
    if schema is None:
        fdf = as_fugue_df(df)
    else:
        fdf = as_fugue_df(df, schema=schema)
    return engine.to_df(fdf)


def run_engine_function(
    func: Callable[[ExecutionEngine], Any],
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
    infer_by: Optional[List[Any]] = None,
) -> Any:
    """Run a lambda function based on the engine provided

    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False
    :param infer_by: a list of objects to infer the engine, defaults to None

    :return: None or a Fugue :class:`~.fugue.dataframe.dataframe.DataFrame` if
        ``as_fugue`` is True, otherwise if ``infer_by`` contains any
        Fugue DataFrame, then return the Fugue DataFrame, otherwise
        it returns the underlying dataframe using
        :meth:`~.fugue.dataframe.dataframe.DataFrame.native_as_df`

    .. note::

        This function is for deveopment use. Users should not need it.
    """
    with engine_context(engine, engine_conf=engine_conf, infer_by=infer_by) as e:
        res = func(e)

        if isinstance(res, DataFrame):
            res = e.convert_yield_dataframe(res, as_local=as_local)
            if as_fugue or any(isinstance(x, DataFrame) for x in (infer_by or [])):
                return res
            return res.native_as_df()
        return res


def repartition(
    df: AnyDataFrame,
    partition: PartitionSpec,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Partition the input dataframe using ``partition``.

    :param df: an input dataframe that can be recognized by Fugue
    :param partition: how you want to partition the dataframe
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the repartitioned dataframe

    .. caution::

        This function is experimental, and may be removed in the future.
    """
    return run_engine_function(
        lambda e: e.repartition(
            as_fugue_df(df), partition_spec=PartitionSpec(partition)
        ),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def broadcast(
    df: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Broadcast the dataframe to all workers of a distributed computing backend

    :param df: an input dataframe that can be recognized by Fugue
    :param engine: an engine-like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the broadcasted dataframe
    """
    return run_engine_function(
        lambda e: e.broadcast(as_fugue_df(df)),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def persist(
    df: AnyDataFrame,
    lazy: bool = False,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
    **kwargs: Any,
) -> AnyDataFrame:
    """Force materializing and caching the dataframe

    :param df: an input dataframe that can be recognized by Fugue
    :param lazy: ``True``: first usage of the output will trigger persisting
        to happen; ``False`` (eager): persist is forced to happend immediately.
        Default to ``False``
    :param kwargs: parameter to pass to the underlying persist implementation
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the persisted dataframe
    """
    return run_engine_function(
        lambda e: e.persist(as_fugue_df(df), lazy=lazy, **kwargs),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def distinct(
    df: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Equivalent to ``SELECT DISTINCT * FROM df``

    :param df: an input dataframe that can be recognized by Fugue
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the result with distinct rows
    """
    return run_engine_function(
        lambda e: e.distinct(as_fugue_df(df)),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def dropna(
    df: AnyDataFrame,
    how: str = "any",
    thresh: int = None,
    subset: List[str] = None,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Drop NA recods from dataframe

    :param df: an input dataframe that can be recognized by Fugue
    :param how: 'any' or 'all'. 'any' drops rows that contain any nulls.
        'all' drops rows that contain all nulls.
    :param thresh: int, drops rows that have less than thresh non-null values
    :param subset: list of columns to operate on
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: DataFrame with NA records dropped
    """
    return run_engine_function(
        lambda e: e.dropna(as_fugue_df(df), how=how, thresh=thresh, subset=subset),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def fillna(
    df: AnyDataFrame,
    value: Any,
    subset: List[str] = None,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """
    Fill ``NULL``, ``NAN``, ``NAT`` values in a dataframe

    :param df: an input dataframe that can be recognized by Fugue
    :param value: if scalar, fills all columns with same value.
        if dictionary, fills NA using the keys as column names and the
        values as the replacement values.
    :param subset: list of columns to operate on. ignored if value is
        a dictionary
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: DataFrame with NA records filled
    """
    return run_engine_function(
        lambda e: e.fillna(as_fugue_df(df), value=value, subset=subset),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def sample(
    df: AnyDataFrame,
    n: Optional[int] = None,
    frac: Optional[float] = None,
    replace: bool = False,
    seed: Optional[int] = None,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
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
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the sampled dataframe
    """
    return run_engine_function(
        lambda e: e.sample(as_fugue_df(df), n=n, frac=frac, replace=replace, seed=seed),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def take(
    df: AnyDataFrame,
    n: int,
    presort: str,
    na_position: str = "last",
    partition: Any = None,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
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
    :param partition: PartitionSpec to apply the take operation,
        defaults to None
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: n rows of DataFrame per partition
    """

    return run_engine_function(
        lambda e: e.take(
            as_fugue_df(df),
            n=n,
            presort=presort,
            na_position=na_position,
            partition_spec=None if partition is None else PartitionSpec(partition),
        ),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def load(
    path: Union[str, List[str]],
    format_hint: Any = None,
    columns: Any = None,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
    **kwargs: Any,
) -> AnyDataFrame:
    """Load dataframe from persistent storage

    :param path: the path to the dataframe
    :param format_hint: can accept ``parquet``, ``csv``, ``json``,
        defaults to None, meaning to infer
    :param columns: list of columns or a |SchemaLikeObject|, defaults to None
    :param kwargs: parameters to pass to the underlying framework
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False
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
        as_local=as_local,
    )


def save(
    df: AnyDataFrame,
    path: str,
    format_hint: Any = None,
    mode: str = "overwrite",
    partition: Any = None,
    force_single: bool = False,
    engine: AnyExecutionEngine = None,
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
    :param partition: how to partition the dataframe before saving,
        defaults to None
    :param force_single: force the output as a single file, defaults to False
    :param kwargs: parameters to pass to the underlying framework
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None

    For more details and examples, read |LoadSave|.
    """
    run_engine_function(
        lambda e: e.save_df(
            as_fugue_df(df),
            path=path,
            format_hint=format_hint,
            mode=mode,
            partition_spec=None if partition is None else PartitionSpec(partition),
            force_single=force_single,
            **kwargs,
        ),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
    )


def join(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    how: str,
    on: Optional[List[str]] = None,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Join two dataframes

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
    :param how: can accept ``semi``, ``left_semi``, ``anti``, ``left_anti``,
        ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
    :param on: it can always be inferred, but if you provide, it will be
        validated against the inferred keys.
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the joined dataframe

    .. note::

        Please read :func:`~.fugue.dataframe.utils.get_join_schemas`
    """

    def _join(e: ExecutionEngine):
        edf1 = as_fugue_engine_df(e, df1)
        edf2 = as_fugue_engine_df(e, df2)
        res = e.join(edf1, edf2, how=how, on=on)
        for odf in dfs:
            res = e.join(res, as_fugue_engine_df(e, odf), how=how, on=on)
        return res

    return run_engine_function(
        _join,
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
        infer_by=[df1, df2, *dfs],
    )


def inner_join(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Inner join two dataframes.
    This is a wrapper of :func:`~.join` with ``how="inner"``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
    :param how: can accept ``semi``, ``left_semi``, ``anti``, ``left_anti``,
        ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the joined dataframe
    """
    return join(
        df1,
        df2,
        *dfs,
        how="inner",
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
    )


def semi_join(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Left semi-join two dataframes.
    This is a wrapper of :func:`~.join` with ``how="semi"``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the joined dataframe
    """
    return join(
        df1,
        df2,
        *dfs,
        how="semi",
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
    )


def anti_join(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Left anti-join two dataframes.
    This is a wrapper of :func:`~.join` with ``how="anti"``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the joined dataframe
    """
    return join(
        df1,
        df2,
        *dfs,
        how="anti",
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
    )


def left_outer_join(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Left outer join two dataframes.
    This is a wrapper of :func:`~.join` with ``how="left_outer"``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the joined dataframe
    """
    return join(
        df1,
        df2,
        *dfs,
        how="left_outer",
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
    )


def right_outer_join(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Right outer join two dataframes.
    This is a wrapper of :func:`~.join` with ``how="right_outer"``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the joined dataframe
    """
    return join(
        df1,
        df2,
        *dfs,
        how="right_outer",
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
    )


def full_outer_join(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Full outer join two dataframes.
    This is a wrapper of :func:`~.join` with ``how="full_outer"``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the joined dataframe
    """
    return join(
        df1,
        df2,
        *dfs,
        how="full_outer",
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
    )


def cross_join(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Cross join two dataframes.
    This is a wrapper of :func:`~.join` with ``how="cross"``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to join
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the joined dataframe
    """
    return join(
        df1,
        df2,
        *dfs,
        how="cross",
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
    )


def union(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    distinct: bool = True,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Join two dataframes

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to union
    :param distinct: ``true`` for ``UNION`` (== ``UNION DISTINCT``),
        ``false`` for ``UNION ALL``
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the unioned dataframe

    .. note::

        Currently, the schema of all dataframes must be identical, or
        an exception will be thrown.
    """

    def _union(e: ExecutionEngine):
        edf1 = as_fugue_engine_df(e, df1)
        edf2 = as_fugue_engine_df(e, df2)
        res = e.union(edf1, edf2, distinct=distinct)
        for odf in dfs:
            res = e.union(res, as_fugue_engine_df(e, odf), distinct=distinct)
        return res

    return run_engine_function(
        _union,
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
        infer_by=[df1, df2, *dfs],
    )


def subtract(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    distinct: bool = True,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """``df1 - df2``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to subtract
    :param distinct: ``true`` for ``EXCEPT`` (== ``EXCEPT DISTINCT``),
        ``false`` for ``EXCEPT ALL``
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the unioned dataframe

    .. note::

        Currently, the schema of all datafrmes must be identical, or
        an exception will be thrown.
    """

    def _subtract(e: ExecutionEngine):
        edf1 = as_fugue_engine_df(e, df1)
        edf2 = as_fugue_engine_df(e, df2)
        res = e.subtract(edf1, edf2, distinct=distinct)
        for odf in dfs:
            res = e.subtract(res, as_fugue_engine_df(e, odf), distinct=distinct)
        return res

    return run_engine_function(
        _subtract,
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
        infer_by=[df1, df2, *dfs],
    )


def intersect(
    df1: AnyDataFrame,
    df2: AnyDataFrame,
    *dfs: AnyDataFrame,
    distinct: bool = True,  # pylint: disable-all
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Intersect ``df1`` and ``df2``

    :param df1: the first dataframe
    :param df2: the second dataframe
    :param dfs: more dataframes to intersect with
    :param distinct: ``true`` for ``INTERSECT`` (== ``INTERSECT DISTINCT``),
        ``false`` for ``INTERSECT ALL``
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the unioned dataframe

    .. note::

        Currently, the schema of ``df1`` and ``df2`` must be identical, or
        an exception will be thrown.
    """

    def _intersect(e: ExecutionEngine):
        edf1 = as_fugue_engine_df(e, df1)
        edf2 = as_fugue_engine_df(e, df2)
        res = e.intersect(edf1, edf2, distinct=distinct)
        for odf in dfs:
            res = e.intersect(res, as_fugue_engine_df(e, odf), distinct=distinct)
        return res

    return run_engine_function(
        _intersect,
        engine=engine,
        engine_conf=engine_conf,
        as_fugue=as_fugue,
        as_local=as_local,
        infer_by=[df1, df2, *dfs],
    )


def select(
    df: AnyDataFrame,
    *columns: Union[str, ColumnExpr],
    where: Optional[ColumnExpr] = None,
    having: Optional[ColumnExpr] = None,
    distinct: bool = False,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """The functional interface for SQL select statement

    :param df: the dataframe to be operated on
    :param columns: column expressions, for strings they will represent
        the column names
    :param where: ``WHERE`` condition expression, defaults to None
    :param having: ``having`` condition expression, defaults to None. It
        is used when ``cols`` contains aggregation columns, defaults to None
    :param distinct: whether to return distinct result, defaults to False
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the select result as a dataframe

    .. attention::

        This interface is experimental, it's subjected to change in new versions.

    .. seealso::

        Please find more expression examples in :mod:`fugue.column.sql` and
        :mod:`fugue.column.functions`

    .. admonition:: Examples

        .. code-block:: python

            from fugue.column import col, lit, functions as f
            import fugue.api as fa

            with fa.engine_context("duckdb"):
                # select existed and new columns
                fa.select(df, col("a"),col("b"),lit(1,"another"))
                fa.select(df, col("a"),(col("b")+lit(1)).alias("x"))

                # aggregation
                # SELECT COUNT(DISTINCT *) AS x FROM df
                fa.select(
                    df,
                    f.count_distinct(all_cols()).alias("x"))

                # SELECT a, MAX(b+1) AS x FROM df GROUP BY a
                fa.select(
                    df,
                    col("a"),f.max(col("b")+lit(1)).alias("x"))

                # SELECT a, MAX(b+1) AS x FROM df
                #   WHERE b<2 AND a>1
                #   GROUP BY a
                #   HAVING MAX(b+1)>0
                fa.select(
                    df,
                    col("a"),f.max(col("b")+lit(1)).alias("x"),
                    where=(col("b")<2) & (col("a")>1),
                    having=f.max(col("b")+lit(1))>0
                )
    """
    cols = SelectColumns(
        *[col(x) if isinstance(x, str) else x for x in columns],
        arg_distinct=distinct,
    )

    return run_engine_function(
        lambda e: e.select(as_fugue_df(df), cols=cols, where=where, having=having),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def filter(
    df: AnyDataFrame,
    condition: ColumnExpr,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
) -> AnyDataFrame:
    """Filter rows by the given condition

    :param df: the dataframe to be filtered
    :param condition: (boolean) column expression
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the filtered dataframe

    .. seealso::

        Please find more expression examples in :mod:`fugue.column.sql` and
        :mod:`fugue.column.functions`

    .. admonition:: Examples

        .. code-block:: python

            from fugue.column import col, functions as f
            import fugue.api as fa

            with fa.engine_context("duckdb"):
                fa.filter(df, (col("a")>1) & (col("b")=="x"))
                fa.filter(df, f.coalesce(col("a"),col("b"))>1)
    """
    return run_engine_function(
        lambda e: e.filter(as_fugue_df(df), condition=condition),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def assign(
    df: AnyDataFrame,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
    **columns: Any,
) -> AnyDataFrame:
    """Update existing columns with new values and add new columns

    :param df: the dataframe to set columns
    :param columns: column expressions
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the updated dataframe

    .. tip::

        This can be used to cast data types, alter column values or add new
        columns. But you can't use aggregation in columns.

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. seealso::

        Please find more expression examples in :mod:`fugue.column.sql` and
        :mod:`fugue.column.functions`

    .. admonition:: Examples

        .. code-block:: python

            from fugue.column import col, functions as f
            import fugue.api as fa

            # assume df has schema: a:int,b:str

            with fa.engine_context("duckdb"):
                # add constant column x
                fa.assign(df, x=1)

                # change column b to be a constant integer
                fa.assign(df, b=1)

                # add new x to be a+b
                fa.assign(df, x=col("a")+col("b"))

                # cast column a data type to double
                fa.assign(df, a=col("a").cast(float))
    """
    cols = [
        v.alias(k) if isinstance(v, ColumnExpr) else lit(v).alias(k)
        for k, v in columns.items()
    ]
    return run_engine_function(
        lambda e: e.assign(as_fugue_df(df), columns=cols),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )


def aggregate(
    df: AnyDataFrame,
    partition_by: Union[None, str, List[str]] = None,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
    **agg_kwcols: ColumnExpr,
) -> AnyDataFrame:
    """Aggregate on dataframe

    :param df: the dataframe to aggregate on
    :param partition_by: partition key(s), defaults to None
    :param agg_kwcols: aggregation expressions
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether to force return a local DataFrame, defaults to False

    :return: the aggregated result as a dataframe

    .. seealso::

        Please find more expression examples in :mod:`fugue.column.sql` and
        :mod:`fugue.column.functions`

    .. admonition:: Examples

        .. code-block:: python

            from fugue.column import col, functions as f
            import fugue.api as fa

            with fa.engine_context("duckdb"):
                # SELECT MAX(b) AS b FROM df
                fa.aggregate(df, b=f.max(col("b")))

                # SELECT a, MAX(b) AS x FROM df GROUP BY a
                fa.aggregate(df, "a", x=f.max(col("b")))
    """
    cols = [
        v.alias(k) if isinstance(v, ColumnExpr) else lit(v).alias(k)
        for k, v in agg_kwcols.items()
    ]
    return run_engine_function(
        lambda e: e.aggregate(
            as_fugue_df(df),
            partition_spec=None
            if partition_by is None
            else PartitionSpec(by=partition_by),
            agg_cols=cols,
        ),
        engine=engine,
        engine_conf=engine_conf,
        infer_by=[df],
        as_fugue=as_fugue,
        as_local=as_local,
    )
