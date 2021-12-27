from typing import Any, Callable, Optional

import ibis
import ibis.expr.types as ir
from fugue import (
    DataFrame,
    DataFrames,
    ExecutionEngine,
    PandasDataFrame,
    NativeExecutionEngine,
)
from triad.utils.assertion import assert_or_throw
import pandas as pd

from fugue_ibis.execution.ibis_engine import IbisEngine, register_ibis_engine
from fugue_ibis._utils import to_schema, to_ibis_schema
from ibis.backends.pandas import Backend


class PandasIbisEngine(IbisEngine):
    def select(
        self, dfs: DataFrames, ibis_func: Callable[[ibis.BaseBackend], ir.TableExpr]
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


register_ibis_engine(1, _to_pandas_ibis_engine)
