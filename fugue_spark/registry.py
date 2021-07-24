import inspect
from typing import Any, Optional

import pyspark.rdd as pr
import pyspark.sql as ps
from fugue import DataFrame, ExecutionEngine, register_execution_engine
from fugue._utils.interfaceless import (
    DataFrameParam,
    ExecutionEngineParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue.workflow import register_raw_df_type
from pyspark import SparkContext
from pyspark.sql import SparkSession

from fugue_spark.execution_engine import SparkExecutionEngine


def register() -> None:
    """Register Spark Execution Engine

    .. note::

        This function is automatically called when you do

        >>> import fugue_spark
    """
    _register_raw_dataframes()
    _register_engines()
    _register_annotation_converters()


def _register_raw_dataframes() -> None:
    register_raw_df_type(ps.DataFrame)


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
