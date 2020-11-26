from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type

from fugue import ExecutionEngine
from triad.utils.assertion import assert_or_throw


class ExecutionEngineProvider(object):
    def __init__(self):
        pass

    def can_accept(self, df: Any) -> bool:  # pragma: no cover
        raise NotImplementedError

    def get_execution_engine(self) -> ExecutionEngine:  # pragma: no cover
        raise NotImplementedError


class NativeExecutionEngineProvider(object):
    def __init__(self):
        import pandas  # noqa: F401

    def can_accept(self, df: Any) -> bool:
        import pandas as pd
        from fugue import LocalDataFrame

        if isinstance(df, (pd.DataFrame, LocalDataFrame)):
            return True
        return False

    def get_execution_engine(self) -> ExecutionEngine:
        from fugue import NativeExecutionEngine

        return NativeExecutionEngine()


class SparkExecutionEngineProvider(object):
    def __init__(self):
        import pyspark  # noqa: F401

    def can_accept(self, df: Any) -> bool:
        import pandas as pd
        import pyspark.sql as ps
        from fugue import LocalDataFrame
        from fugue_spark import SparkDataFrame

        if isinstance(df, (pd.DataFrame, ps.DataFrame, LocalDataFrame, SparkDataFrame)):
            return True
        return False

    def get_execution_engine(self) -> ExecutionEngine:
        from fugue_spark import SparkExecutionEngine

        return SparkExecutionEngine()


class DaskExecutionEngineProvider(object):
    def __init__(self):
        import dask  # noqa: F401

    def can_accept(self, df: Any) -> bool:
        import dask.dataframe as pdd
        import pandas as pd
        from fugue import LocalDataFrame
        from fugue_dask import DaskDataFrame

        if isinstance(df, (pd.DataFrame, pdd.DataFrame, LocalDataFrame, DaskDataFrame)):
            return True
        return False

    def get_execution_engine(self) -> ExecutionEngine:
        from fugue_dask import DaskExecutionEngine

        return DaskExecutionEngine()


class ExecutionEngineSelector(object):
    def __init__(self):
        self._providers: List[Tuple[Set[str], ExecutionEngineProvider]] = []
        self.add(["pandas", "local", "native"], NativeExecutionEngineProvider)
        self.add(["spark"], SparkExecutionEngineProvider)
        self.add(["dask"], DaskExecutionEngineProvider)

    def add(self, names: List[str], provider: Type[ExecutionEngineProvider]) -> None:
        try:
            self._providers.append((set(names), provider()))
        except Exception:
            return

    def get_execution_engine(self, name: Optional[str], *args: Any, **kwargs: Any):
        def get_candidates() -> Iterable[int]:
            yield -1
            for a in args:
                if isinstance(a, Dict):
                    for df in a.values():
                        yield self._get_index(name, df)
                else:
                    yield self._get_index(name, a)
            for df in kwargs:
                yield self._get_index(name, df)

        if name is not None:
            idx = self._get_index(name=name, df=None)
            assert_or_throw(idx >= 0, NotImplementedError(name))
        else:
            idx = max(get_candidates())
            assert_or_throw(
                idx >= 0, NotImplementedError("can't infer execution engine")
            )
        return self._providers[idx][1].get_execution_engine()

    def _get_index(self, name: Optional[str], df: Any) -> int:
        n = 0
        if name is None:
            for _, p in self._providers:
                if p.can_accept(df):
                    return n
                n += 1
        else:
            for names, _ in self._providers:
                if name in names:
                    return n
                n += 1
        return -1


DEFAULT_EXECUTION_ENGINE_SELECTOR = ExecutionEngineSelector()
