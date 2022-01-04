# flake8: noqa
from fugue import register_execution_engine, register_sql_engine

from fugue_duckdb.execution_engine import DuckDBEngine, DuckExecutionEngine

try:
    from fugue_duckdb.ibis_engine import DuckDBIbisEngine
except Exception:  # pragma: no cover
    pass


def register() -> None:
    """Register engines for DuckDB"""
    register_sql_engine("duck", lambda engine: DuckDBEngine(engine))
    register_sql_engine("duckdb", lambda engine: DuckDBEngine(engine))
    register_execution_engine("duck", lambda conf: DuckExecutionEngine(conf))
    register_execution_engine("duckdb", lambda conf: DuckExecutionEngine(conf))


register()
