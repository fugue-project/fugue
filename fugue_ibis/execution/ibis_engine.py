from abc import abstractmethod
from typing import Any, Callable

import ibis

from fugue import DataFrame, DataFrames, ExecutionEngine, ExecutionEngineFacet
from fugue._utils.registry import fugue_plugin

from .._compat import IbisTable


@fugue_plugin
def parse_ibis_engine(obj: Any, engine: ExecutionEngine) -> "IbisEngine":
    if isinstance(obj, IbisEngine):
        return obj
    raise NotImplementedError(
        f"Ibis execution engine can't be parsed from {obj}."
        " You may need to register a parser for it."
    )


class IbisEngine(ExecutionEngineFacet):
    """The abstract base class for different ibis execution implementations.

    :param execution_engine: the execution engine this ibis engine will run on
    """

    @abstractmethod
    def select(
        self, dfs: DataFrames, ibis_func: Callable[[ibis.BaseBackend], IbisTable]
    ) -> DataFrame:  # pragma: no cover
        """Execute the ibis select expression.

        :param dfs: a collection of dataframes that must have keys
        :param ibis_func: the ibis compute function
        :return: result of the ibis function

        .. note::

            This interface is experimental, so it is subjected to change.
        """
        raise NotImplementedError
