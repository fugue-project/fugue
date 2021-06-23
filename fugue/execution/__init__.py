# flake8: noqa
from fugue.execution.execution_engine import ExecutionEngine, SQLEngine
from fugue.execution.factory import (
    make_execution_engine,
    make_sql_engine,
    register_default_execution_engine,
    register_default_sql_engine,
    register_execution_engine,
    register_sql_engine,
)
from fugue.execution.native_execution_engine import (
    NativeExecutionEngine,
    QPDPandasEngine,
    SqliteEngine,
)

register_execution_engine(
    "native", lambda conf: NativeExecutionEngine(conf), on_dup="ignore"
)
register_execution_engine(
    "pandas", lambda conf: NativeExecutionEngine(conf), on_dup="ignore"
)
register_sql_engine("sqlite", lambda engine: SqliteEngine(engine), on_dup="ignore")
register_sql_engine(
    "qpdpandas", lambda engine: QPDPandasEngine(engine), on_dup="ignore"
)
register_sql_engine(
    "qpd_pandas", lambda engine: QPDPandasEngine(engine), on_dup="ignore"
)
