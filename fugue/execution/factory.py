from typing import Any, Callable, List, Optional, Type, Union

import pandas as pd
from triad import ParamDict, assert_or_throw
from triad.utils.convert import to_instance

from .._utils.registry import fugue_plugin
from ..exceptions import FuguePluginsRegistrationError
from .execution_engine import (
    _FUGUE_EXECUTION_ENGINE_CONTEXT,
    _FUGUE_GLOBAL_EXECUTION_ENGINE_CONTEXT,
    ExecutionEngine,
    SQLEngine,
)
from .native_execution_engine import NativeExecutionEngine


def register_execution_engine(
    name_or_type: Union[str, Type], func: Callable, on_dup="overwrite"
) -> None:
    """Register :class:`~fugue.execution.execution_engine.ExecutionEngine` with
    a given name.

    :param name_or_type: alias of the execution engine, or type of an object that
      can be converted to an execution engine
    :param func: a callable taking |ParamsLikeObject| and ``**kwargs`` and returning an
      :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
    :param on_dup: action on duplicated ``name``. It can be "overwrite", "ignore"
      (not overwriting), defaults to "overwrite".

    .. admonition:: Examples

        Alias registration examples:

        .. code-block:: python

            # create a new engine with name my (overwrites if existed)
            register_execution_engine("my", lambda conf: MyExecutionEngine(conf))

            # 0
            make_execution_engine("my")
            make_execution_engine("my", {"myconfig":"value})

            # 1
            dag = FugueWorkflow()
            dag.create([[0]],"a:int").show()
            dag.run("my", {"myconfig":"value})

            # 2
            fsql('''
            CREATE [[0]] SCHEMA a:int
            PRINT
            ''').run("my")

        Type registration examples:

        .. code-block:: python

            from pyspark.sql import SparkSession
            from fugue_spark import SparkExecutionEngine
            from fugue import fsql

            register_execution_engine(
                SparkSession,
                lambda session, conf: SparkExecutionEngine(session, conf))

            spark_session = SparkSession.builder.getOrCreate()

            fsql('''
            CREATE [[0]] SCHEMA a:int
            PRINT
            ''').run(spark_session)
    """
    if isinstance(name_or_type, str):
        nm = name_or_type
        parse_execution_engine.register(  # type: ignore
            func=lambda engine, conf, **kwargs: func(conf, **kwargs),
            matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
            and engine == nm,
            priority=_get_priority(on_dup),
        )
    else:  # type
        tp = name_or_type
        parse_execution_engine.register(  # type: ignore
            func=lambda engine, conf, **kwargs: func(engine, conf, **kwargs),
            matcher=lambda engine, conf, **kwargs: isinstance(engine, tp),
            priority=_get_priority(on_dup),
        )


def register_default_execution_engine(func: Callable, on_dup="overwrite") -> None:
    """Register :class:`~fugue.execution.execution_engine.ExecutionEngine` as the
    default engine.

    :param func: a callable taking |ParamsLikeObject| and ``**kwargs`` and returning an
      :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
    :param on_dup: action on duplicated ``name``. It can be "overwrite", "ignore"
      (not overwriting), defaults to "overwrite".

    .. admonition:: Examples

        .. code-block:: python

            # create a new engine with name my (overwrites if existed)
            register_default_execution_engine(lambda conf: MyExecutionEngine(conf))

            # the following examples will use MyExecutionEngine

            # 0
            make_execution_engine()
            make_execution_engine(None, {"myconfig":"value})

            # 1
            dag = FugueWorkflow()
            dag.create([[0]],"a:int").show()
            dag.run(None, {"myconfig":"value})

            # 2
            fsql('''
            CREATE [[0]] SCHEMA a:int
            PRINT
            ''').run("", {"myconfig":"value})
    """
    parse_execution_engine.register(  # type: ignore
        func=lambda engine, conf, **kwargs: func(conf, **kwargs),
        matcher=lambda engine, conf, **kwargs: engine is None
        or (isinstance(engine, str) and engine == ""),
        priority=_get_priority(on_dup),
    )


def register_sql_engine(name: str, func: Callable, on_dup="overwrite") -> None:
    """Register :class:`~fugue.execution.execution_engine.SQLEngine` with
    a given name.

    :param name: name of the SQL engine
    :param func: a callable taking
      :class:`~fugue.execution.execution_engine.ExecutionEngine`
      and ``**kwargs`` and returning a
      :class:`~fugue.execution.execution_engine.SQLEngine` instance
    :param on_dup: action on duplicated ``name``. It can be "overwrite", "ignore"
      (not overwriting), defaults to "overwrite".

    .. admonition:: Examples

        .. code-block:: python

            # create a new engine with name my (overwrites if existed)
            register_sql_engine("mysql", lambda engine: MySQLEngine(engine))

            # create execution engine with MySQLEngine as the default
            make_execution_engine(("", "mysql"))

            # create DaskExecutionEngine with MySQLEngine as the default
            make_execution_engine(("dask", "mysql"))

            # default execution engine + MySQLEngine
            with FugueWorkflow() as dag:
                dag.create([[0]],"a:int").show()
            dag.run(("","mysql"))
    """
    nm = name
    parse_sql_engine.register(  # type: ignore
        func=lambda engine, execution_engine, **kwargs: func(
            execution_engine, **kwargs
        ),
        matcher=lambda engine, execution_engine, **kwargs: isinstance(engine, str)
        and engine == nm,
        priority=_get_priority(on_dup),
    )


def register_default_sql_engine(func: Callable, on_dup="overwrite") -> None:
    """Register :class:`~fugue.execution.execution_engine.SQLEngine` as the
    default engine

    :param func: a callable taking
      :class:`~fugue.execution.execution_engine.ExecutionEngine`
      and ``**kwargs`` and returning a
      :class:`~fugue.execution.execution_engine.SQLEngine` instance
    :param on_dup: action on duplicated ``name``. It can be "overwrite", "ignore"
      (not overwriting) or "throw" (throw exception), defaults to "overwrite".

    :raises KeyError: if ``on_dup`` is ``throw`` and the ``name`` already exists

    .. note::

        You should be careful to use this function, because when you set a custom
        SQL engine as default, all execution engines you create will use this SQL
        engine unless you are explicit. For example if you set the default SQL engine
        to be a Spark specific one, then if you start a NativeExecutionEngine, it will
        try to use it and will throw exceptions.

        So it's always a better idea to use ``register_sql_engine`` instead

    .. admonition:: Examples

        .. code-block:: python

            # create a new engine with name my (overwrites if existed)
            register_default_sql_engine(lambda engine: MySQLEngine(engine))

            # create NativeExecutionEngine with MySQLEngine as the default
            make_execution_engine()

            # create SparkExecutionEngine with MySQLEngine instead of SparkSQLEngine
            make_execution_engine("spark")

            # NativeExecutionEngine with MySQLEngine
            with FugueWorkflow() as dag:
                dag.create([[0]],"a:int").show()
            dag.run()
    """
    parse_sql_engine.register(  # type: ignore
        func=lambda engine, execution_engine, **kwargs: func(
            execution_engine, **kwargs
        ),
        matcher=lambda engine, execution_engine, **kwargs: engine is None
        or (isinstance(engine, str) and engine == ""),
        priority=_get_priority(on_dup),
    )


def try_get_context_execution_engine() -> Optional[ExecutionEngine]:
    """If the global execution engine is set (see
    :meth:`~fugue.execution.execution_engine.ExecutionEngine.set_global`)
    or the context is set (see
    :meth:`~fugue.execution.execution_engine.ExecutionEngine.as_context`),
    then return the engine, else return None
    """
    engine = _FUGUE_EXECUTION_ENGINE_CONTEXT.get()
    if engine is None:
        engine = _FUGUE_GLOBAL_EXECUTION_ENGINE_CONTEXT.get()
    return engine


def make_execution_engine(
    engine: Any = None,
    conf: Any = None,
    infer_by: Optional[List[Any]] = None,
    **kwargs: Any,
) -> ExecutionEngine:
    """Create :class:`~fugue.execution.execution_engine.ExecutionEngine`
    with specified ``engine``

    :param engine: it can be empty string or null (use the default execution
      engine), a string (use the registered execution engine), an
      :class:`~fugue.execution.execution_engine.ExecutionEngine` type, or
      the :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
      , or a tuple of two values where the first value represents execution
      engine and the second value represents the sql engine (you can use ``None``
      for either of them to use the default one), defaults to None
    :param conf: |ParamsLikeObject|, defaults to None
    :param infer_by: List of objects that can be used to infer the execution
      engine using :func:`~.infer_execution_engine`
    :param kwargs: additional parameters to initialize the execution engine

    :return: the :class:`~fugue.execution.execution_engine.ExecutionEngine`
      instance

    .. note::

        This function finds/constructs the engine in the following order:

        * If ``engine`` is None, it first try to see if there is any defined
          context engine to use (=> engine)
        * If ``engine`` is still empty, then it will try to get the global execution
          engine. See
          :meth:`~fugue.execution.execution_engine.ExecutionEngine.set_global`
        * If ``engine`` is still empty, then if ``infer_by``
          is given, it will try to infer the execution engine (=> engine)
        * If ``engine`` is still empty, then it will construct the default
          engine defined by :func:`~.register_default_execution_engine`  (=> engine)
        * Now, ``engine`` must not be empty, if it is an object other than
          :class:`~fugue.execution.execution_engine.ExecutionEngine`, we will use
          :func:`~.parse_execution_engine` to construct (=> engine)
        * Now, ``engine`` must have been an ExecutionEngine object. We update its
          SQL engine if specified, then update its config using ``conf`` and ``kwargs``

    .. admonition:: Examples

        .. code-block:: python

            register_default_execution_engine(lambda conf: E1(conf))
            register_execution_engine("e2", lambda conf, **kwargs: E2(conf, **kwargs))

            register_sql_engine("s", lambda conf: S2(conf))

            # E1 + E1.create_default_sql_engine()
            make_execution_engine()

            # E2 + E2.create_default_sql_engine()
            make_execution_engine(e2)

            # E1 + S2
            make_execution_engine((None, "s"))

            # E2(conf, a=1, b=2) + S2
            make_execution_engine(("e2", "s"), conf, a=1, b=2)

            # SparkExecutionEngine + SparkSQLEngine
            make_execution_engine(SparkExecutionEngine)
            make_execution_engine(SparkExecutionEngine(spark_session, conf))

            # SparkExecutionEngine + S2
            make_execution_engine((SparkExecutionEngine, "s"))

            # assume object e2_df can infer E2 engine
            make_execution_engine(infer_by=[e2_df])  # an E2 engine

            # global
            e_global = E1(conf)
            e_global.set_global()
            make_execution_engine()  # e_global

            # context
            with E2(conf).as_context() as ec:
                make_execution_engine()  # ec
            make_execution_engine()  # e_global
    """
    if engine is None:
        engine = try_get_context_execution_engine()
        if engine is None and infer_by is not None:
            engine = infer_execution_engine(infer_by)

    if isinstance(engine, tuple):
        execution_engine = make_execution_engine(engine[0], conf=conf, **kwargs)
        sql_engine = make_sql_engine(engine[1], execution_engine)
        execution_engine.set_sql_engine(sql_engine)
        return execution_engine
    if isinstance(engine, ExecutionEngine):
        result = engine
    else:
        result = parse_execution_engine(engine, conf, **kwargs)
        sql_engine = make_sql_engine(None, result)
        result.set_sql_engine(sql_engine)
    result.conf.update(conf, on_dup=ParamDict.OVERWRITE)
    result.conf.update(kwargs, on_dup=ParamDict.OVERWRITE)
    return result


@fugue_plugin
def parse_execution_engine(
    engine: Any = None, conf: Any = None, **kwargs: Any
) -> ExecutionEngine:
    """Parse object as
    :class:`~fugue.execution.execution_engine.ExecutionEngine`

    :param engine: it can be empty string or null (use the default execution
      engine), a string (use the registered execution engine), an
      :class:`~fugue.execution.execution_engine.ExecutionEngine` type, or
      the :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
      , or a tuple of two values where the first value represents execution
      engine and the second value represents the sql engine (you can use ``None``
      for either of them to use the default one), defaults to None
    :param conf: |ParamsLikeObject|, defaults to None
    :param kwargs: additional parameters to initialize the execution engine

    :return: the :class:`~fugue.execution.execution_engine.ExecutionEngine`
      instance

    .. admonition:: Examples

        .. code-block:: python

            register_default_execution_engine(lambda conf: E1(conf))
            register_execution_engine("e2", lambda conf, **kwargs: E2(conf, **kwargs))

            register_sql_engine("s", lambda conf: S2(conf))

            # E1 + E1.default_sql_engine
            make_execution_engine()

            # E2 + E2.default_sql_engine
            make_execution_engine(e2)

            # E1 + S2
            make_execution_engine((None, "s"))

            # E2(conf, a=1, b=2) + S2
            make_execution_engine(("e2", "s"), conf, a=1, b=2)

            # SparkExecutionEngine + SparkSQLEngine
            make_execution_engine(SparkExecutionEngine)
            make_execution_engine(SparkExecutionEngine(spark_session, conf))

            # SparkExecutionEngine + S2
            make_execution_engine((SparkExecutionEngine, "s"))
    """
    if engine is None or (isinstance(engine, str) and engine == ""):
        return NativeExecutionEngine(conf)
    try:
        return to_instance(engine, ExecutionEngine, kwargs=dict(conf=conf, **kwargs))
    except Exception as e:
        raise FuguePluginsRegistrationError(
            f"Fugue execution engine is not recognized ({engine}, {conf}, {kwargs})."
            " You may need to register a parser for it."
        ) from e


def is_pandas_or(objs: List[Any], obj_type: Any) -> bool:
    """Check whether the input contains at least one ``obj_type`` object and the
    rest are Pandas DataFrames. This function is a utility function for extending
    :func:`~.infer_execution_engine`

    :param objs: the list of objects to check
    :return: whether all objs are of type ``obj_type`` or pandas DataFrame and at
      least one is of type ``obj_type``
    """
    tc = 0
    for obj in objs:
        if not isinstance(obj, pd.DataFrame):
            if isinstance(obj, obj_type):
                tc += 1
            else:
                return False
    return tc > 0


@fugue_plugin
def infer_execution_engine(obj: List[Any]) -> Any:
    """Infer the correspondent ExecutionEngine based on the input objects. This is
    used in express functions.

    :param objs: the objects
    :return: if the inference succeeded, it returns an object that can be used by
      :func:`~.parse_execution_engine` in the ``engine`` field to construct an
      ExecutionEngine. Otherwise, it returns None.

    .. admonition:: Examples

        .. code-block:: python

            from fugue import transform

            transform(spark_df)

        In this example, the SparkExecutionEngine is inferred from spark_df, it
        is equivalent to:

        .. code-block:: python

            from fugue import transform

            transform(spark_df, engine=current_spark_session)
    """
    return None


def make_sql_engine(
    engine: Any = None,
    execution_engine: Optional[ExecutionEngine] = None,
    **kwargs: Any,
) -> SQLEngine:
    """Create :class:`~fugue.execution.execution_engine.SQLEngine`
    with specified ``engine``

    :param engine: it can be empty string or null (use the default SQL
      engine), a string (use the registered SQL engine), an
      :class:`~fugue.execution.execution_engine.SQLEngine` type, or
      the :class:`~fugue.execution.execution_engine.SQLEngine` instance
      (you can use ``None`` to use the default one), defaults to None
    :param execution_engine: the
      :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
      to create
      the :class:`~fugue.execution.execution_engine.SQLEngine`. Normally you
      should always provide this value.
    :param kwargs: additional parameters to initialize the sql engine

    :return: the :class:`~fugue.execution.execution_engine.SQLEngine`
      instance

    .. note::

        For users, you normally don't need to call this function directly.
        Use ``make_execution_engine`` instead

    .. admonition:: Examples

        .. code-block:: python

            register_default_sql_engine(lambda conf: S1(conf))
            register_sql_engine("s2", lambda conf: S2(conf))

            engine = NativeExecutionEngine()

            # S1(engine)
            make_sql_engine(None, engine)

            # S1(engine, a=1)
            make_sql_engine(None, engine, a=1)

            # S2(engine)
            make_sql_engine("s2", engine)
    """
    if isinstance(engine, SQLEngine):
        assert_or_throw(
            execution_engine is None and len(kwargs) == 0,
            lambda: ValueError(
                f"{engine} is an instance, can't take arguments "
                f"execution_engine={execution_engine}, kwargs={kwargs}"
            ),
        )
        return engine
    return parse_sql_engine(engine, execution_engine, **kwargs)


@fugue_plugin
def parse_sql_engine(
    engine: Any = None,
    execution_engine: Optional[ExecutionEngine] = None,
    **kwargs: Any,
) -> SQLEngine:
    """Create :class:`~fugue.execution.execution_engine.SQLEngine`
    with specified ``engine``

    :param engine: it can be empty string or null (use the default SQL
      engine), a string (use the registered SQL engine), an
      :class:`~fugue.execution.execution_engine.SQLEngine` type, or
      the :class:`~fugue.execution.execution_engine.SQLEngine` instance
      (you can use ``None`` to use the default one), defaults to None
    :param execution_engine: the
      :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
      to create
      the :class:`~fugue.execution.execution_engine.SQLEngine`. Normally you
      should always provide this value.
    :param kwargs: additional parameters to initialize the sql engine

    :return: the :class:`~fugue.execution.execution_engine.SQLEngine`
      instance

    .. note::

        For users, you normally don't need to call this function directly.
        Use ``make_execution_engine`` instead

    .. admonition:: Examples

        .. code-block:: python

            register_default_sql_engine(lambda conf: S1(conf))
            register_sql_engine("s2", lambda conf: S2(conf))

            engine = NativeExecutionEngine()

            # S1(engine)
            make_sql_engine(None, engine)

            # S1(engine, a=1)
            make_sql_engine(None, engine, a=1)

            # S2(engine)
            make_sql_engine("s2", engine)
    """
    if engine is None or (isinstance(engine, str) and engine == ""):
        assert_or_throw(
            execution_engine is not None,
            ValueError("execution_engine must be provided"),
        )
        return execution_engine.sql_engine  # type: ignore
    try:
        return to_instance(
            engine, SQLEngine, kwargs=dict(execution_engine=execution_engine, **kwargs)
        )
    except Exception as e:
        raise FuguePluginsRegistrationError(
            f"Fugue SQL engine is not recognized ({engine}, {kwargs})."
            " You may need to register a parser for it."
        ) from e


def _get_priority(on_dup: str) -> int:
    if on_dup == "overwrite":
        return 2
    if on_dup == "ignore":
        return 0
    raise ValueError(f"{on_dup} is not valid")
