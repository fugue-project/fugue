from typing import Any, Optional, Tuple

import pyspark.rdd as pr
import pyspark.sql as ps
from pyspark import SparkContext
from pyspark.sql import SparkSession
from triad import run_at_def

from fugue import DataFrame, ExecutionEngine, register_execution_engine
from fugue.dev import (
    DataFrameParam,
    ExecutionEngineParam,
    fugue_annotated_param,
    is_pandas_or,
)
from fugue.extensions import namespace_candidate
from fugue.plugins import as_fugue_dataset, infer_execution_engine, parse_creator
from fugue_spark.dataframe import SparkDataFrame
from fugue_spark.execution_engine import SparkExecutionEngine

from ._utils.misc import SparkConnectDataFrame, SparkConnectSession, is_spark_dataframe

_is_sparksql = namespace_candidate("sparksql", lambda x: isinstance(x, str))


@infer_execution_engine.candidate(
    lambda objs: (
        is_pandas_or(objs, (ps.DataFrame, SparkConnectDataFrame, SparkDataFrame))
        if SparkConnectDataFrame is not None
        else is_pandas_or(objs, (ps.DataFrame, SparkDataFrame))
    )
    or any(_is_sparksql(obj) for obj in objs)
)
def _infer_spark_client(obj: Any) -> Any:
    return SparkSession.builder.getOrCreate()


@as_fugue_dataset.candidate(lambda df, **kwargs: is_spark_dataframe(df))
def _spark_as_fugue_df(df: ps.DataFrame, **kwargs: Any) -> SparkDataFrame:
    return SparkDataFrame(df, **kwargs)


@parse_creator.candidate(_is_sparksql)
def _parse_sparksql_creator(sql: Tuple[str, str]):
    def _run_sql(spark: SparkSession) -> ps.DataFrame:
        return spark.sql(sql[1])

    return _run_sql


def _register_engines() -> None:
    register_execution_engine(
        "spark",
        lambda conf, **kwargs: SparkExecutionEngine(conf=conf),
        on_dup="ignore",
    )
    register_execution_engine(
        SparkSession,
        lambda session, conf, **kwargs: SparkExecutionEngine(session, conf=conf),
        on_dup="ignore",
    )
    if SparkConnectSession is not None:
        register_execution_engine(
            SparkConnectSession,
            lambda session, conf, **kwargs: SparkExecutionEngine(session, conf=conf),
            on_dup="ignore",
        )


@fugue_annotated_param(SparkExecutionEngine)
class _SparkExecutionEngineParam(ExecutionEngineParam):
    pass


@fugue_annotated_param(SparkSession)
class _SparkSessionParam(ExecutionEngineParam):
    def to_input(self, engine: ExecutionEngine) -> Any:
        assert isinstance(engine, SparkExecutionEngine)
        return engine.spark_session  # type:ignore


@fugue_annotated_param(SparkContext)
class _SparkContextParam(ExecutionEngineParam):
    def to_input(self, engine: ExecutionEngine) -> Any:
        assert isinstance(engine, SparkExecutionEngine)
        return engine.spark_session.sparkContext  # type:ignore


@fugue_annotated_param(ps.DataFrame)
class _SparkDataFrameParam(DataFrameParam):
    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, SparkExecutionEngine)
        return ctx.to_df(df).native

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert is_spark_dataframe(output)
        assert isinstance(ctx, SparkExecutionEngine)
        return ctx.to_df(output, schema=schema)

    def count(self, df: Any) -> int:  # pragma: no cover
        raise NotImplementedError("not allowed")


@fugue_annotated_param(pr.RDD)
class _RddParam(DataFrameParam):
    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, SparkExecutionEngine)
        return ctx.to_df(df).native.rdd

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, pr.RDD)
        assert isinstance(ctx, SparkExecutionEngine)
        return ctx.to_df(output, schema=schema)

    def count(self, df: Any) -> int:  # pragma: no cover
        raise NotImplementedError("not allowed")

    def need_schema(self) -> Optional[bool]:
        return True


@run_at_def
def _register() -> None:
    """Register Spark Execution Engine

    .. note::

        This function is automatically called when you do

        >>> import fugue_spark
    """
    _register_engines()
