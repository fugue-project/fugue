from typing import Any, Callable, Optional

import ibis
from ibis.backends.pandas import Backend

from fugue import DataFrame, DataFrames, ExecutionEngine
from fugue_duckdb.execution_engine import DuckDBEngine, DuckExecutionEngine
from fugue_ibis import IbisTable
from fugue_ibis._utils import to_ibis_schema
from fugue_ibis.execution.ibis_engine import IbisEngine, parse_ibis_engine


class DuckDBIbisEngine(IbisEngine):
    def select(
        self, dfs: DataFrames, ibis_func: Callable[[ibis.BaseBackend], IbisTable]
    ) -> DataFrame:
        be = _BackendWrapper().connect({})
        be.set_schemas(dfs)
        expr = ibis_func(be)
        sql = str(
            ibis.postgres.compile(expr).compile(compile_kwargs={"literal_binds": True})
        )
        engine = DuckDBEngine(self.execution_engine)
        return engine.select(dfs, sql)


@parse_ibis_engine.candidate(
    lambda obj, *args, **kwargs: isinstance(obj, DuckExecutionEngine)
    or (isinstance(obj, str) and obj in ["duck", "duckdb"])
)
def _to_duck_ibis_engine(obj: Any, engine: ExecutionEngine) -> Optional[IbisEngine]:
    return DuckDBIbisEngine(engine)


class _BackendWrapper(Backend):
    def set_schemas(self, dfs: DataFrames) -> None:
        self._schemas = {k: to_ibis_schema(v.schema) for k, v in dfs.items()}

    def table(self, name: str, schema: Any = None):
        return ibis.table(self._schemas[name], name=name)
