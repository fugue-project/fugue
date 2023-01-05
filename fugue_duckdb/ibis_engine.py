from typing import Any, Callable, Dict, Optional, Tuple

import ibis
from ibis.backends.pandas import Backend

from fugue import DataFrame, DataFrames, ExecutionEngine
from fugue.collections.sql import StructuredRawSQL, TempTableName
from fugue_ibis import IbisTable
from fugue_ibis._utils import to_ibis_schema
from fugue_ibis.execution.ibis_engine import IbisEngine, parse_ibis_engine

from .execution_engine import DuckDBEngine, DuckExecutionEngine


class DuckDBIbisEngine(IbisEngine):
    def select(
        self, dfs: DataFrames, ibis_func: Callable[[ibis.BaseBackend], IbisTable]
    ) -> DataFrame:
        be = _BackendWrapper().connect({})
        be.set_schemas(dfs)
        expr = ibis_func(be)
        sql = StructuredRawSQL.from_expr(
            str(
                ibis.postgres.compile(expr).compile(
                    compile_kwargs={"literal_binds": True}
                )
            ),
            prefix='"<tmpdf:',
            suffix='>"',
            dialect="postgres",
        )

        engine = DuckDBEngine(self.execution_engine)
        _dfs = DataFrames({be._name_map[k][0].key: v for k, v in dfs.items()})
        return engine.select(_dfs, sql)


@parse_ibis_engine.candidate(
    lambda obj, *args, **kwargs: isinstance(obj, DuckExecutionEngine)
    or (isinstance(obj, str) and obj in ["duck", "duckdb"])
)
def _to_duck_ibis_engine(obj: Any, engine: ExecutionEngine) -> Optional[IbisEngine]:
    return DuckDBIbisEngine(engine)


class _BackendWrapper(Backend):
    def set_schemas(self, dfs: DataFrames) -> None:
        self._schemas = {k: to_ibis_schema(v.schema) for k, v in dfs.items()}
        self._name_map: Dict[str, Tuple[TempTableName, IbisTable]] = {}

    def table(self, name: str, schema: Any = None) -> IbisTable:
        if name not in self._name_map:
            tn = TempTableName()
            tb = ibis.table(self._schemas[name], name=(str(tn)))
            self._name_map[name] = (tn, tb)
        return self._name_map[name][1]
