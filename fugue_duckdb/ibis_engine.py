from typing import Any, Callable, Optional

import ibis
import ibis.expr.types as ir
from fugue import DataFrame, DataFrames, ExecutionEngine
from fugue_ibis._utils import to_ibis_schema
from fugue_ibis.execution.ibis_engine import IbisEngine, register_ibis_engine
from ibis.backends.pandas import Backend

from fugue_duckdb.execution_engine import DuckDBEngine, DuckExecutionEngine


class DuckDBIbisEngine(IbisEngine):
    def select(
        self, dfs: DataFrames, ibis_func: Callable[[ibis.BaseBackend], ir.TableExpr]
    ) -> DataFrame:
        be = _BackendWrapper().connect({})
        be.set_schemas(dfs)
        expr = ibis_func(be)
        sql = str(
            ibis.postgres.compile(expr).compile(compile_kwargs={"literal_binds": True})
        )
        engine = DuckDBEngine(self.execution_engine)
        return engine.select(dfs, sql)


def _to_duckdb_ibis_engine(
    engine: ExecutionEngine, ibis_engine: Any
) -> Optional[IbisEngine]:
    if isinstance(ibis_engine, str) and ibis_engine in ["duck", "duckdb"]:
        return DuckDBIbisEngine(engine)
    if isinstance(engine, DuckExecutionEngine):
        if ibis_engine is None:
            return DuckDBIbisEngine(engine)
    return None  # pragma: no cover


class _BackendWrapper(Backend):
    def set_schemas(self, dfs: DataFrames) -> None:
        self._schemas = {k: to_ibis_schema(v.schema) for k, v in dfs.items()}

    def table(self, name: str, schema: Any = None):
        return ibis.table(self._schemas[name], name=name)


register_ibis_engine(0, _to_duckdb_ibis_engine)
