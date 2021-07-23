import inspect
from typing import Any, Optional

import dask.dataframe as dd
from fugue import DataFrame, register_execution_engine
from fugue._utils.interfaceless import (
    DataFrameParam,
    ExecutionEngineParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue.workflow import register_raw_df_type

from fugue_dask.execution_engine import DaskExecutionEngine


def register() -> None:
    """Register Dask Execution Engine

    .. note::

        This function is automatically called when you do

        >>> import fugue_dask
    """
    _register_raw_dataframes()
    _register_engines()
    _register_annotation_converters()


def _register_raw_dataframes() -> None:
    register_raw_df_type(dd.DataFrame)


def _register_engines() -> None:
    register_execution_engine(
        "dask",
        lambda conf, **kwargs: DaskExecutionEngine(conf=conf),
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
