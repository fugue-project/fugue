from typing import Any

import dask.dataframe as dd
from dask.distributed import Client

from fugue import DataFrame
from fugue.dev import (
    DataFrameParam,
    ExecutionEngineParam,
    fugue_annotated_param,
    is_pandas_or,
)
from fugue.plugins import (
    as_fugue_dataset,
    infer_execution_engine,
    parse_execution_engine,
)
from fugue_dask._utils import DASK_UTILS
from fugue_dask.dataframe import DaskDataFrame
from fugue_dask.execution_engine import DaskExecutionEngine

from .tester import DaskTestBackend  # noqa: F401  # pylint: disable-all


@infer_execution_engine.candidate(
    lambda objs: is_pandas_or(objs, (dd.DataFrame, DaskDataFrame))
)
def _infer_dask_client(objs: Any) -> Any:
    return DASK_UTILS.get_or_create_client()


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, dd.DataFrame))
def _dask_as_fugue_df(df: dd.DataFrame, **kwargs: Any) -> DaskDataFrame:
    return DaskDataFrame(df, **kwargs)


@parse_execution_engine.candidate(
    lambda engine, conf, **kwargs: isinstance(engine, Client),
    priority=4,  # TODO: this is to overwrite dask-sql fugue integration
)
def _parse_dask_client(engine: Client, conf: Any, **kwargs: Any) -> DaskExecutionEngine:
    return DaskExecutionEngine(dask_client=engine, conf=conf)


@parse_execution_engine.candidate(
    lambda engine, conf, **kwargs: isinstance(engine, str) and engine == "dask",
    priority=4,  # TODO: this is to overwrite dask-sql fugue integration
)
def _parse_dask_str(engine: str, conf: Any, **kwargs: Any) -> DaskExecutionEngine:
    return DaskExecutionEngine(conf=conf)


@fugue_annotated_param(DaskExecutionEngine)
class _DaskExecutionEngineParam(ExecutionEngineParam):
    pass


@fugue_annotated_param(dd.DataFrame)
class _DaskDataFrameParam(DataFrameParam):
    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, DaskExecutionEngine)
        return ctx.to_df(df).native

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, dd.DataFrame)
        assert isinstance(ctx, DaskExecutionEngine)
        return ctx.to_df(output, schema=schema)

    def count(self, df: DataFrame) -> int:  # pragma: no cover
        raise NotImplementedError("not allowed")
