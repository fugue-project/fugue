from typing import Any, Tuple

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
from fugue.plugins import infer_execution_engine, parse_execution_engine
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_duckdb.execution_engine import DuckDBEngine, DuckExecutionEngine

from .tester import DuckDBTestBackend  # noqa: F401  # pylint: disable-all

try:
    from .tester import DuckDaskTestBackend  # noqa: F401  # pylint: disable-all
except ImportError:  # pragma: no cover
    pass


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


try:
    from fugue_duckdb.dask import DuckDaskExecutionEngine
    from dask.distributed import Client

    @parse_execution_engine.candidate(
        lambda engine, conf, **kwargs: isinstance(engine, list)
        and len(engine) == 2
        and isinstance(engine[0], DuckDBPyConnection)
        and isinstance(engine[1], Client),
    )
    def _parse_duck_dask_client(
        engine: Tuple[DuckDBPyConnection, Client], conf: Any, **kwargs: Any
    ) -> DuckDaskExecutionEngine:
        return DuckDaskExecutionEngine(
            connection=engine[0], dask_client=engine[1], conf=conf
        )

except Exception:  # pragma: no cover
    pass


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
