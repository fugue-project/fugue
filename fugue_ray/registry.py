from typing import Any

import ray.data as rd
from triad import run_at_def

from fugue import DataFrame, register_execution_engine
from fugue.dev import (
    DataFrameParam,
    ExecutionEngineParam,
    fugue_annotated_param,
    is_pandas_or,
)
from fugue.plugins import as_fugue_dataset, infer_execution_engine

from .dataframe import RayDataFrame
from .execution_engine import RayExecutionEngine


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, (rd.Dataset, RayDataFrame))
)
def _infer_ray_client(objs: Any) -> Any:
    return "ray"


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, rd.Dataset))
def _ray_as_fugue_df(df: rd.Dataset, **kwargs: Any) -> RayDataFrame:
    return RayDataFrame(df, **kwargs)


def _register_engines() -> None:
    register_execution_engine(
        "ray", lambda conf, **kwargs: RayExecutionEngine(conf=conf), on_dup="ignore"
    )


@fugue_annotated_param(RayExecutionEngine)
class _RayExecutionEngineParam(ExecutionEngineParam):
    pass


@fugue_annotated_param(rd.Dataset)
class _RayDatasetParam(DataFrameParam):
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
    _register_engines()
