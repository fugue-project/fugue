import inspect
from typing import Any, Optional

import ray.data as rd
from triad import run_at_def

from fugue import (
    DataFrame,
    infer_execution_engine,
    is_pandas_or,
    register_execution_engine,
)
from fugue._utils.interfaceless import (
    DataFrameParam,
    ExecutionEngineParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue.workflow import register_raw_df_type

from .dataframe import RayDataFrame
from .execution_engine import RayExecutionEngine


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, (rd.Dataset, RayDataFrame))
)
def _infer_ray_client(objs: Any) -> Any:
    return "ray"


def _register_raw_dataframes() -> None:
    register_raw_df_type(rd.Dataset)


def _register_engines() -> None:
    register_execution_engine(
        "ray",
        lambda conf, **kwargs: RayExecutionEngine(conf=conf),
        on_dup="ignore",
    )


def _register_annotation_converters() -> None:
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            RayExecutionEngine,
            lambda param: _RayExecutionEngineParam(param),
        ),
    )
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(rd.Dataset, lambda param: _RayDatasetParam(param)),
    )


class _RayExecutionEngineParam(ExecutionEngineParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
    ):
        super().__init__(
            param, annotation="RayExecutionEngine", engine_type=RayExecutionEngine
        )


class _RayDatasetParam(DataFrameParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, annotation="ray.data.Dataset")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, RayExecutionEngine)
        return ctx._to_ray_df(df).native

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, rd.Dataset)
        assert isinstance(ctx, RayExecutionEngine)
        return RayDataFrame(output, schema=schema)

    def count(self, df: DataFrame) -> int:  # pragma: no cover
        raise NotImplementedError("not allowed")


@run_at_def
def _register() -> None:
    """Register Ray Execution Engine"""
    _register_raw_dataframes()
    _register_engines()
    _register_annotation_converters()
