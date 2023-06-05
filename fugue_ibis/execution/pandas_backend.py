from typing import Any, Callable

import ibis
import pandas as pd
from ibis.backends.pandas import Backend
from triad.utils.assertion import assert_or_throw

from fugue import (
    DataFrame,
    DataFrames,
    ExecutionEngine,
    NativeExecutionEngine,
    PandasDataFrame,
)
from fugue_ibis._utils import to_ibis_schema, to_schema

from .._compat import IbisTable
from .ibis_engine import IbisEngine, parse_ibis_engine


class PandasIbisEngine(IbisEngine):
    def select(
        self, dfs: DataFrames, ibis_func: Callable[[ibis.BaseBackend], IbisTable]
    ) -> DataFrame:  # pragma: no cover
        pdfs = {k: v.as_pandas() for k, v in dfs.items()}
        be = _BackendWrapper().connect(pdfs)
        be.set_schemas(dfs)
        expr = ibis_func(be)
        schema = to_schema(expr.schema())
        result = expr.execute()
        assert_or_throw(
            isinstance(result, pd.DataFrame), "result must be a pandas DataFrame"
        )
        return PandasDataFrame(result, schema=schema)


@parse_ibis_engine.candidate(
    lambda obj, *args, **kwargs: isinstance(obj, NativeExecutionEngine)
)
def _pd_to_ibis_engine(obj: Any, engine: ExecutionEngine) -> IbisEngine:
    return PandasIbisEngine(engine)


class _BackendWrapper(Backend):
    def set_schemas(self, dfs: DataFrames) -> None:
        self._schemas = {k: to_ibis_schema(v.schema) for k, v in dfs.items()}

    def table(self, name: str, schema: Any = None):
        return super().table(
            name,
            schema=self._schemas[name]
            if schema is None and name in self._schemas
            else schema,
        )
