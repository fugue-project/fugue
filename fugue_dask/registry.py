import inspect
from typing import Any, Optional

import dask.dataframe as dd
from dask.distributed import Client
from triad import run_at_def

from fugue import DataFrame, is_pandas_or, register_execution_engine
from fugue._utils.interfaceless import (
    DataFrameParam,
    ExecutionEngineParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue.plugins import as_fugue_dataset, infer_execution_engine
from fugue_dask._utils import DASK_UTILS
from fugue_dask.dataframe import DaskDataFrame
from fugue_dask.execution_engine import DaskExecutionEngine


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, (dd.DataFrame, DaskDataFrame))
)
def _infer_dask_client(objs: Any) -> Any:
    return DASK_UTILS.get_or_create_client()


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, dd.DataFrame))
def _dask_as_fugue_df(df: dd.DataFrame, **kwargs: Any) -> DaskDataFrame:
    return DaskDataFrame(df, **kwargs)


def _register_engines() -> None:
    register_execution_engine(
        "dask",
        lambda conf, **kwargs: DaskExecutionEngine(conf=conf),
        on_dup="ignore",
    )
    register_execution_engine(
        Client,
        lambda engine, conf, **kwargs: DaskExecutionEngine(
            dask_client=engine, conf=conf
        ),
        on_dup="ignore",
    )


def _register_annotation_converters() -> None:
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            DaskExecutionEngine,
            lambda param: _DaskExecutionEngineParam(param),
        ),
    )
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            dd.DataFrame, lambda param: _DaskDataFrameParam(param)
        ),
    )


class _DaskExecutionEngineParam(ExecutionEngineParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
    ):
        super().__init__(
            param, annotation="DaskExecutionEngine", engine_type=DaskExecutionEngine
        )


class _DaskDataFrameParam(DataFrameParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, annotation="dask.dataframe.DataFrame")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, DaskExecutionEngine)
        return ctx.to_df(df).native

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, dd.DataFrame)
        assert isinstance(ctx, DaskExecutionEngine)
        return ctx.to_df(output, schema=schema)

    def count(self, df: DataFrame) -> int:  # pragma: no cover
        raise NotImplementedError("not allowed")


@run_at_def
def _register() -> None:
    """Register Dask Execution Engine

    .. note::

        This function is automatically called when you do

        >>> import fugue_dask
    """
    _register_engines()
    _register_annotation_converters()
