from fugue.execution.factory import register_execution_engine, register_sql_engine
from fugue.execution.native_execution_engine import (
    NativeExecutionEngine,
    QPDPandasEngine,
)


def _register() -> None:
    """Register Fugue core additional types

    .. note::

        This function is automatically called when you do

        >>> import fugue
    """
    _register_engines()


def _register_engines() -> None:
    register_execution_engine(
        "native", lambda conf: NativeExecutionEngine(conf), on_dup="ignore"
    )
    register_execution_engine(
        "pandas", lambda conf: NativeExecutionEngine(conf), on_dup="ignore"
    )
    register_sql_engine(
        "qpdpandas", lambda engine: QPDPandasEngine(engine), on_dup="ignore"
    )
    register_sql_engine(
        "qpd_pandas", lambda engine: QPDPandasEngine(engine), on_dup="ignore"
    )
