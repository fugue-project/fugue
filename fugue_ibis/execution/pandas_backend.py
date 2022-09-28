from typing import Any, Callable, Optional

import ibis
import pandas as pd
from fugue import (
    DataFrame,
    DataFrames,
    ExecutionEngine,
    NativeExecutionEngine,
    PandasDataFrame,
)
from fugue_ibis._utils import to_ibis_schema, to_schema
from fugue_ibis.execution.ibis_engine import IbisEngine
from ibis.backends.pandas import Backend
from triad.utils.assertion import assert_or_throw

from .._compat import IbisTable


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


def _to_pandas_ibis_engine(
    engine: ExecutionEngine, ibis_engine: Any
) -> Optional[IbisEngine]:
    if isinstance(engine, NativeExecutionEngine):
        if ibis_engine is None:
            return PandasIbisEngine(engine)
    return None  # pragma: no cover


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
