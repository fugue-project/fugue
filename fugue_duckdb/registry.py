from typing import Any

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from triad import run_at_def

from fugue import (
    DataFrame,
    ExecutionEngine,
    register_execution_engine,
    register_sql_engine,
)
from fugue.dev import (
    DataFrameParam,
    ExecutionEngineParam,
    fugue_annotated_param,
    is_pandas_or,
)
from fugue.plugins import infer_execution_engine
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_duckdb.execution_engine import DuckDBEngine, DuckExecutionEngine


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, (DuckDBPyRelation, DuckDataFrame))
)
def _infer_duckdb_client(objs: Any) -> Any:
    return "duckdb"


def _register_engines() -> None:
    register_execution_engine(
        "duck",
        lambda conf, **kwargs: DuckExecutionEngine(conf=conf),
        on_dup="ignore",
    )
    register_execution_engine(
        "duckdb",
        lambda conf, **kwargs: DuckExecutionEngine(conf=conf),
        on_dup="ignore",
    )
    try:
        from fugue_duckdb.dask import DuckDaskExecutionEngine

        register_execution_engine(
            "duckdask",
            lambda conf, **kwargs: DuckDaskExecutionEngine(conf=conf),
            on_dup="ignore",
        )
        register_execution_engine(
            "duckdbdask",
            lambda conf, **kwargs: DuckDaskExecutionEngine(conf=conf),
            on_dup="ignore",
        )
        register_execution_engine(
            "dd",
            lambda conf, **kwargs: DuckDaskExecutionEngine(conf=conf),
            on_dup="ignore",
        )
    except Exception:  # pragma: no cover
        pass
    register_execution_engine(
        DuckDBPyConnection,
        lambda con, conf, **kwargs: DuckExecutionEngine(conf=conf, connection=con),
        on_dup="ignore",
    )
    register_sql_engine("duck", lambda engine: DuckDBEngine(engine))
    register_sql_engine("duckdb", lambda engine: DuckDBEngine(engine))


@fugue_annotated_param(DuckExecutionEngine)
class _DuckExecutionEngineParam(ExecutionEngineParam):
    pass


@fugue_annotated_param(DuckDBPyConnection)
class _DuckDBPyConnectionParam(ExecutionEngineParam):
    def to_input(self, engine: ExecutionEngine) -> Any:
        assert isinstance(engine, DuckExecutionEngine)
        return engine.connection  # type:ignore


@fugue_annotated_param(DuckDBPyRelation)
class _DuckDBPyRelationParam(DataFrameParam):
    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, DuckExecutionEngine)
        return ctx.to_df(df).native  # type: ignore

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, DuckDBPyRelation)
        assert isinstance(ctx, DuckExecutionEngine)
        return DuckDataFrame(output)

    def count(self, df: Any) -> int:  # pragma: no cover
        return df.count()


@run_at_def
def _register() -> None:
    """Register DuckDB Execution Engine

    .. note::

        This function is automatically called when you do

        >>> import fugue_duckdb
    """
    _register_engines()
