from typing import Any

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from fugue.dataframe.function_wrapper import DataFrameParam, fugue_annotated_param
from fugue.execution.execution_engine import ExecutionEngine, ExecutionEngineParam
from fugue.execution.factory import (
    is_pandas_or,
    register_execution_engine,
    register_sql_engine,
)
from fugue.plugins import infer_execution_engine

from .dataframe import DataFrame, DuckDataFrame
from .execution_engine import DuckDBEngine, NativeExecutionEngine, _to_duck_df


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, (DuckDBPyRelation, DuckDataFrame))
)
def _infer_duckdb_client(objs: Any) -> Any:
    return "duckdb"


def _register_engines() -> None:
    register_execution_engine(
        "native", lambda conf: NativeExecutionEngine(conf), on_dup="ignore"
    )
    register_execution_engine(
        "duck", lambda conf: NativeExecutionEngine(conf), on_dup="ignore"
    )
    register_execution_engine(
        "duckdb", lambda conf: NativeExecutionEngine(conf), on_dup="ignore"
    )
    register_execution_engine(
        "pandas", lambda conf: NativeExecutionEngine(conf), on_dup="ignore"
    )
    register_execution_engine(
        DuckDBPyConnection,
        lambda con, conf, **kwargs: NativeExecutionEngine(conf=conf, connection=con),
        on_dup="ignore",
    )

    register_sql_engine("duck", lambda engine: DuckDBEngine(engine))
    register_sql_engine("duckdb", lambda engine: DuckDBEngine(engine))


@fugue_annotated_param(NativeExecutionEngine)
class _NativeExecutionEngineParam(ExecutionEngineParam):
    pass


@fugue_annotated_param(DuckDBPyConnection)
class _DuckDBPyConnectionParam(ExecutionEngineParam):
    def to_input(self, engine: ExecutionEngine) -> Any:
        assert isinstance(engine, NativeExecutionEngine)
        return engine.connection  # type:ignore


@fugue_annotated_param(DuckDBPyRelation)
class _DuckDBPyRelationParam(DataFrameParam):
    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, NativeExecutionEngine)
        return _to_duck_df(ctx, df, create_view=False).native  # type:ignore

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, DuckDBPyRelation)
        assert isinstance(ctx, NativeExecutionEngine)
        return DuckDataFrame(output)

    def count(self, df: Any) -> int:  # pragma: no cover
        return df.count()
