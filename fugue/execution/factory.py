from typing import Any, Callable, Dict, Optional

from fugue.execution.execution_engine import ExecutionEngine, SQLEngine
from fugue.execution.native_execution_engine import NativeExecutionEngine
from triad.utils.convert import to_instance
from triad import assert_or_throw


class _ExecutionEngineFactory(object):
    def __init__(self):
        self._funcs: Dict[str, Callable] = {}
        self._sql_funcs: Dict[str, Callable] = {}
        self.register_default(lambda conf, **kwargs: NativeExecutionEngine(conf=conf))
        self.register_default_sql_engine(lambda engine, **kwargs: engine.sql_engine)

    def register(self, name: str, func: Callable, on_dup="overwrite") -> None:
        self._register(self._funcs, name=name, func=func, on_dup=on_dup)

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
        if engine is None:
            engine = ""
        if isinstance(engine, str) and engine in self._funcs:
            return self._funcs[engine](conf, **kwargs)
        if isinstance(engine, ExecutionEngine):
            assert_or_throw(
                conf is None and len(kwargs) == 0,
                ValueError(
                    f"{engine} is an instance, "
                    f"can't take arguments conf={conf}, kwargs={kwargs}"
                ),
            )
            return engine
        return to_instance(engine, ExecutionEngine, kwargs=dict(conf=conf, **kwargs))

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
                ValueError(
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
        callables: Dict[str, Callable],
        name: str,
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


def register_execution_engine(name: str, func: Callable, on_dup="overwrite") -> None:
    _EXECUTION_ENGINE_FACTORY.register(name, func, on_dup)


def register_default_execution_engine(func: Callable, on_dup="overwrite") -> None:
    _EXECUTION_ENGINE_FACTORY.register_default(func, on_dup)


def register_sql_engine(name: str, func: Callable, on_dup="overwrite") -> None:
    _EXECUTION_ENGINE_FACTORY.register_sql_engine(name, func, on_dup)


def register_default_sql_engine(func: Callable, on_dup="overwrite") -> None:
    _EXECUTION_ENGINE_FACTORY.register_default_sql_engine(func, on_dup)


def make_execution_engine(
    engine: Any = None, conf: Any = None, **kwargs: Any
) -> ExecutionEngine:
    return _EXECUTION_ENGINE_FACTORY.make(engine, conf, **kwargs)


def make_sql_engine(
    engine: Any = None,
    execution_engine: Optional[ExecutionEngine] = None,
    **kwargs: Any,
) -> SQLEngine:
    return _EXECUTION_ENGINE_FACTORY.make_sql_engine(engine, execution_engine, **kwargs)
