import inspect
from typing import Any, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from fugue import (
    DataFrame,
    ExecutionEngine,
    register_execution_engine,
    register_sql_engine,
)
from fugue._utils.interfaceless import (
    DataFrameParam,
    ExecutionEngineParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue.workflow import register_raw_df_type
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_duckdb.execution_engine import DuckDBEngine, DuckExecutionEngine


def register() -> None:
    """Register DuckDB Execution Engine

    .. note::

        This function is automatically called when you do

        >>> import fugue_duckdb
    """
    _register_raw_dataframes()
    _register_engines()
    _register_annotation_converters()


def _register_raw_dataframes() -> None:
    register_raw_df_type(DuckDBPyRelation)


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


def _register_annotation_converters() -> None:
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            DuckDBPyConnection,
            lambda param: _DuckDBPyConnectionParam(param),
        ),
    )
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            DuckDBPyRelation,
            lambda param: _DuckDBPyRelationParam(param),
        ),
    )


class _DuckDBPyConnectionParam(ExecutionEngineParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
    ):
        super().__init__(
            param, annotation="DuckDBPyConnection", engine_type=DuckExecutionEngine
        )

    def to_input(self, engine: ExecutionEngine) -> Any:
        return super().to_input(engine).connection  # type:ignore


class _DuckDBPyRelationParam(DataFrameParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, annotation="DuckDBPyRelation")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, DuckExecutionEngine)
        return ctx.to_df(df).native

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, DuckDBPyRelation)
        assert isinstance(ctx, DuckExecutionEngine)
        return DuckDataFrame(output)

    def count(self, df: Any) -> int:  # pragma: no cover
        return df.count()
