from typing import Any, Callable, Dict

from fugue.execution.execution_engine import ExecutionEngine
from fugue.execution.native_execution_engine import NativeExecutionEngine
from triad.utils.convert import to_instance
from triad import assert_or_throw


class _ExecutionEngineFactory(object):
    def __init__(self):
        self._funcs: Dict[str, Callable] = {}
        self.register_default(lambda conf, **kwargs: NativeExecutionEngine(conf=conf))

    def register(self, name: str, func: Callable, on_dup="overwrite") -> None:
        if name not in self._funcs:
            self._funcs[name] = func
        if on_dup in ["raise", "throw"]:
            raise KeyError(f"{name} is already registered")
        if on_dup == "overwrite":
            self._funcs[name] = func
            return
        if on_dup == "ignore":
            return
        raise ValueError(on_dup)

    def register_default(self, func: Callable, on_dup="overwrite") -> None:
        self.register("", func, on_dup)

    def make(
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


_EXECUTION_ENGINE_FACTORY = _ExecutionEngineFactory()


def register_execution_engine(name: str, func: Callable, on_dup="overwrite") -> None:
    _EXECUTION_ENGINE_FACTORY.register(name, func, on_dup)


def register_default_execution_engine(func: Callable, on_dup="overwrite") -> None:
    _EXECUTION_ENGINE_FACTORY.register_default(func, on_dup)


def make_execution_engine(
    engine: Any = None, conf: Any = None, **kwargs: Any
) -> ExecutionEngine:
    return _EXECUTION_ENGINE_FACTORY.make(engine, conf, **kwargs)
