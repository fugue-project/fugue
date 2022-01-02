from typing import Any, Callable, Dict, Optional, Type, Union

from fugue.execution.execution_engine import ExecutionEngine, SQLEngine
from fugue.execution.native_execution_engine import NativeExecutionEngine
from triad.utils.convert import to_instance
from triad import assert_or_throw, ParamDict


class _ExecutionEngineFactory(object):
    def __init__(self):
        self._funcs: Dict[str, Callable] = {}
        self._type_funcs: Dict[Type, Callable] = {}
        self._sql_funcs: Dict[str, Callable] = {}
        self.register_default(lambda conf, **kwargs: NativeExecutionEngine(conf=conf))
        self.register_default_sql_engine(lambda engine, **kwargs: engine.sql_engine)

    def register(
        self, name_or_type: Union[str, Type], func: Callable, on_dup="overwrite"
    ) -> None:
        if isinstance(name_or_type, str):
            self._register(self._funcs, name=name_or_type, func=func, on_dup=on_dup)
        else:
            self._register(
                self._type_funcs, name=name_or_type, func=func, on_dup=on_dup
            )

    def register_default(self, func: Callable, on_dup="overwrite") -> None:
        self.register("", func, on_dup)

    def register_sql_engine(
        self, name: str, func: Callable, on_dup="overwrite"
    ) -> None:
        self._register(self._sql_funcs, name=name, func=func, on_dup=on_dup)

    def register_default_sql_engine(self, func: Callable, on_dup="overwrite") -> None:
        self.register_sql_engine("", func, on_dup)

    def make(
        self, engine: Any = None, conf: Any = None, **kwargs: Any
    ) -> ExecutionEngine:
        if isinstance(engine, tuple):
            execution_engine = self.make_execution_engine(
                engine[0], conf=conf, **kwargs
            )
            sql_engine = self.make_sql_engine(engine[1], execution_engine)
            execution_engine.set_sql_engine(sql_engine)
            return execution_engine
        else:
            return self.make((engine, None), conf=conf, **kwargs)

    def make_execution_engine(
        self, engine: Any = None, conf: Any = None, **kwargs: Any
    ) -> ExecutionEngine:
        # Apply this function to an Execution Engine instance can
        # make sure the compile conf is a superset of conf
        # TODO: it's a mess here, can we make the logic more intuitive?

        def make_engine(engine: Any) -> ExecutionEngine:
            if isinstance(engine, str) and engine in self._funcs:
                return self._funcs[engine](conf, **kwargs)
            for k, f in self._type_funcs.items():
                if isinstance(engine, k):
                    return f(engine, conf, **kwargs)
            if isinstance(engine, ExecutionEngine):
                if conf is not None:
                    engine.compile_conf.update(conf)
                engine.compile_conf.update(kwargs)
                return engine
            return to_instance(
                engine, ExecutionEngine, kwargs=dict(conf=conf, **kwargs)
            )

        result = make_engine(engine or "")
        result.compile_conf.update(result.conf, on_dup=ParamDict.IGNORE)
        result.compile_conf.update(conf, on_dup=ParamDict.OVERWRITE)
        result.compile_conf.update(kwargs, on_dup=ParamDict.OVERWRITE)
        return result

    def make_sql_engine(
        self,
        engine: Any = None,
        execution_engine: Optional[ExecutionEngine] = None,
        **kwargs: Any,
    ) -> SQLEngine:
        if engine is None:
            engine = ""
        if isinstance(engine, str) and engine in self._sql_funcs:
            return self._sql_funcs[engine](execution_engine, **kwargs)
        if isinstance(engine, SQLEngine):
            assert_or_throw(
                execution_engine is None and len(kwargs) == 0,
                lambda: ValueError(
                    f"{engine} is an instance, can't take arguments "
                    f"execution_engine={execution_engine}, kwargs={kwargs}"
                ),
            )
            return engine
        return to_instance(
            engine, SQLEngine, kwargs=dict(execution_engine=execution_engine, **kwargs)
        )

    def _register(
        self,
        callables: Dict[Any, Callable],
        name: Any,
        func: Callable,
        on_dup="overwrite",
    ) -> None:
        if name not in callables:
            callables[name] = func
        if on_dup in ["raise", "throw"]:
            raise KeyError(f"{name} is already registered")
        if on_dup == "overwrite":
            callables[name] = func
            return
        if on_dup == "ignore":
            return
        raise ValueError(on_dup)


_EXECUTION_ENGINE_FACTORY = _ExecutionEngineFactory()


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
      (not overwriting) or "throw" (throw exception), defaults to "overwrite".

    :raises KeyError: if ``on_dup`` is ``throw`` and the ``name`` already exists

    .. admonition:: Examples

        Alias registration examples:

        .. code-block:: python

            # create a new engine with name my (overwrites if existed)
            register_execution_engine("my", lambda conf: MyExecutionEngine(conf))

            # 0
            make_execution_engine("my")
            make_execution_engine("my", {"myconfig":"value})

            # 1
            with FugueWorkflow("my") as dag:
                dag.create([[0]],"a:int").show()

            # 2
            dag = FugueWorkflow()
            dag.create([[0]],"a:int").show()
            dag.run("my", {"myconfig":"value})

            # 3
            fsql('''
            CREATE [[0]] SCHEMA a:int
            PRINT
            ''').run("my")

        Type registration examples:

        .. code-block:: python

            from pyspark.sql import SparkSession
            from fugue_spark import SparkExecutionEngine
            from fugue_sql import fsql

            register_execution_engine(
                SparkSession,
                lambda session, conf: SparkExecutionEngine(session, conf))

            spark_session = SparkSession.builder.getOrCreate()

            fsql('''
            CREATE [[0]] SCHEMA a:int
            PRINT
            ''').run(spark_session)
    """
    _EXECUTION_ENGINE_FACTORY.register(name_or_type, func, on_dup)


def register_default_execution_engine(func: Callable, on_dup="overwrite") -> None:
    """Register :class:`~fugue.execution.execution_engine.ExecutionEngine` as the
    default engine.

    :param func: a callable taking |ParamsLikeObject| and ``**kwargs`` and returning an
      :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
    :param on_dup: action on duplicated ``name``. It can be "overwrite", "ignore"
      (not overwriting) or "throw" (throw exception), defaults to "overwrite".

    :raises KeyError: if ``on_dup`` is ``throw`` and the ``name`` already exists

    .. admonition:: Examples

        .. code-block:: python

            # create a new engine with name my (overwrites if existed)
            register_default_execution_engine(lambda conf: MyExecutionEngine(conf))

            # the following examples will use MyExecutionEngine

            # 0
            make_execution_engine()
            make_execution_engine(None, {"myconfig":"value})

            # 1
            with FugueWorkflow() as dag:
                dag.create([[0]],"a:int").show()

            # 2
            dag = FugueWorkflow()
            dag.create([[0]],"a:int").show()
            dag.run(None, {"myconfig":"value})

            # 3
            fsql('''
            CREATE [[0]] SCHEMA a:int
            PRINT
            ''').run("", {"myconfig":"value})
    """
    _EXECUTION_ENGINE_FACTORY.register_default(func, on_dup)


def register_sql_engine(name: str, func: Callable, on_dup="overwrite") -> None:
    """Register :class:`~fugue.execution.execution_engine.SQLEngine` with
    a given name.

    :param name: name of the SQL engine
    :param func: a callable taking
      :class:`~fugue.execution.execution_engine.ExecutionEngine`
      and ``**kwargs`` and returning a
      :class:`~fugue.execution.execution_engine.SQLEngine` instance
    :param on_dup: action on duplicated ``name``. It can be "overwrite", "ignore"
      (not overwriting) or "throw" (throw exception), defaults to "overwrite".

    :raises KeyError: if ``on_dup`` is ``throw`` and the ``name`` already exists

    .. admonition:: Examples

        .. code-block:: python

            # create a new engine with name my (overwrites if existed)
            register_sql_engine("mysql", lambda engine: MySQLEngine(engine))

            # create execution engine with MySQLEngine as the default
            make_execution_engine(("", "mysql"))

            # create DaskExecutionEngine with MySQLEngine as the default
            make_execution_engine(("dask", "mysql"))

            # default execution engine + MySQLEngine
            with FugueWorkflow(("","mysql")) as dag:
                dag.create([[0]],"a:int").show()
    """
    _EXECUTION_ENGINE_FACTORY.register_sql_engine(name, func, on_dup)


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
    """
    _EXECUTION_ENGINE_FACTORY.register_default_sql_engine(func, on_dup)


def make_execution_engine(
    engine: Any = None, conf: Any = None, **kwargs: Any
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
    return _EXECUTION_ENGINE_FACTORY.make(engine, conf, **kwargs)


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

            # SqliteEngine(engine)
            make_sql_engine(SqliteEngine)
    """
    return _EXECUTION_ENGINE_FACTORY.make_sql_engine(engine, execution_engine, **kwargs)
