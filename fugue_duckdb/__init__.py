# flake8: noqa
from fugue import register_execution_engine, register_sql_engine

from fugue_duckdb.execution_engine import DuckDBEngine, DuckExecutionEngine

try:
    from fugue_duckdb.dask import DuckDaskExecutionEngine
except Exception:  # pragma: no cover
    pass

try:
    from fugue_duckdb.ibis_engine import DuckDBIbisEngine
except Exception:  # pragma: no cover
    pass
