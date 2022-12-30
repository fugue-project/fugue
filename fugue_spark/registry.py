import inspect
from typing import Any, Optional

import pyspark.rdd as pr
import pyspark.sql as ps
from pyspark import SparkContext
from pyspark.sql import SparkSession
from triad import run_at_def

from fugue import DataFrame, ExecutionEngine, is_pandas_or, register_execution_engine
from fugue._utils.interfaceless import (
    DataFrameParam,
    ExecutionEngineParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue.plugins import as_fugue_dataset, infer_execution_engine, parse_creator

from fugue_spark.dataframe import SparkDataFrame
from fugue_spark.execution_engine import SparkExecutionEngine


def _is_sparksql(obj: Any) -> bool:
    if not isinstance(obj, str):
        return False
    obj = obj[:20].lower()
    return obj.startswith("--sparksql") or obj.startswith("/*sparksql*/")


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, (ps.DataFrame, SparkDataFrame))
    or any(_is_sparksql(obj) for obj in objs)
)
def _infer_spark_client(obj: Any) -> Any:
    return SparkSession.builder.getOrCreate()


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, ps.DataFrame))
def _spark_as_fugue_df(df: ps.DataFrame, **kwargs: Any) -> SparkDataFrame:
    return SparkDataFrame(df, **kwargs)


@parse_creator.candidate(lambda obj: _is_sparksql(obj))
def _parse_sparksql_creator(sql):
    def _run_sql(spark: SparkSession) -> ps.DataFrame:
        return spark.sql(sql)

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


def _register_annotation_converters() -> None:
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            SparkExecutionEngine,
            lambda param: _SparkExecutionEngineParam(param),
        ),
    )
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            SparkSession,
            lambda param: _SparkSessionParam(param),
        ),
    )
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            SparkContext,
            lambda param: _SparkContextParam(param),
        ),
    )
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            ps.DataFrame,
            lambda param: _SparkDataFrameParam(param),
        ),
    )
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            pr.RDD,
            lambda param: _RddParam(param),
        ),
    )


class _SparkExecutionEngineParam(ExecutionEngineParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
    ):
        super().__init__(
            param, annotation="SparkExecutionEngine", engine_type=SparkExecutionEngine
        )


class _SparkSessionParam(ExecutionEngineParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
    ):
        super().__init__(
            param, annotation="SparkSession", engine_type=SparkExecutionEngine
        )

    def to_input(self, engine: ExecutionEngine) -> Any:
        return super().to_input(engine).spark_session  # type:ignore


class _SparkContextParam(ExecutionEngineParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
    ):
        super().__init__(
            param, annotation="SparkContext", engine_type=SparkExecutionEngine
        )

    def to_input(self, engine: ExecutionEngine) -> Any:
        return super().to_input(engine).spark_session.sparkContext  # type:ignore


class _SparkDataFrameParam(DataFrameParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, annotation="pyspark.sql.DataFrame")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, SparkExecutionEngine)
        return ctx.to_df(df).native

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, ps.DataFrame)
        assert isinstance(ctx, SparkExecutionEngine)
        return ctx.to_df(output, schema=schema)

    def count(self, df: Any) -> int:  # pragma: no cover
        raise NotImplementedError("not allowed")


class _RddParam(DataFrameParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, annotation="pyspark.rdd.RDD")

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
    _register_annotation_converters()
