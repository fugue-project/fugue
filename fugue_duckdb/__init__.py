from fugue_duckdb.execution_engine import DuckExeuctionEngine, DuckDBEngine
from fugue import register_execution_engine, register_sql_engine


def register() -> None:
    """Register engines for DuckDB"""
    register_sql_engine("duck", lambda engine: DuckDBEngine(engine))
    register_sql_engine("duckdb", lambda engine: DuckDBEngine(engine))
    register_execution_engine("duck", lambda conf: DuckExeuctionEngine(conf))
    register_execution_engine("duckdb", lambda conf: DuckExeuctionEngine(conf))


register()
