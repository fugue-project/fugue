from abc import abstractmethod
from typing import Any, Callable, Optional, List, Tuple

import ibis
from fugue import DataFrame, DataFrames, ExecutionEngine

_ENGINE_FUNC: List[
    Tuple[int, int, Callable[[ExecutionEngine, Any], Optional["IbisEngine"]]]
] = []


def register_ibis_engine(
    priority: int, func: Callable[[ExecutionEngine, Any], Optional["IbisEngine"]]
) -> None:
    _ENGINE_FUNC.append((priority, len(_ENGINE_FUNC), func))
    _ENGINE_FUNC.sort()


def to_ibis_engine(
    execution_engine: ExecutionEngine, ibis_engine: Any = None
) -> "IbisEngine":
    if isinstance(ibis_engine, IbisEngine):
        return ibis_engine
    for _, _, f in _ENGINE_FUNC:
        e = f(execution_engine, ibis_engine)
        if e is not None:
            return e
    raise NotImplementedError(
        f"can't get ibis engine from {execution_engine}, {ibis_engine}"
    )


class IbisEngine:
    """The abstract base class for different ibis execution implementations.

    :param execution_engine: the execution engine this ibis engine will run on
    """

    def __init__(self, execution_engine: ExecutionEngine) -> None:
        self._execution_engine = execution_engine

    @property
    def execution_engine(self) -> ExecutionEngine:
        """the execution engine this ibis engine will run on"""
        return self._execution_engine

    @abstractmethod
    def select(
        self, dfs: DataFrames, ibis_func: Callable[[ibis.BaseBackend], ibis.Expr]
    ) -> DataFrame:  # pragma: no cover
        """Execute the ibis select expression.

        :param dfs: a collection of dataframes that must have keys
        :param ibis_func: the ibis compute function
        :return: result of the ibis function

        .. admonition:: Examples

            >>> dfs = DataFrames(a=df1, b=df2)
            >>> sql_engine.select(dfs, "SELECT * FROM a UNION SELECT * FROM b")

        .. note::

            There can be tables that is not in ``dfs``. For example you want to select
            from hive without input DataFrames:

            >>> sql_engine.select(DataFrames(), "SELECT * FROM hive.a.table")
        """
        raise NotImplementedError
