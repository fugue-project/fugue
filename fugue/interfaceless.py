from typing import Any, List

from fugue.dataframe import DataFrame
from fugue.workflow import FugueWorkflow
from fugue.workflow.workflow import _DEFAULT_IGNORE_ERRORS


def transform(
    df: Any,
    using: Any,
    schema: Any = None,
    params: Any = None,
    partition: Any = None,
    ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
    engine: Any = None,
    engine_conf: Any = None,
) -> Any:
    dag = FugueWorkflow()
    dag.df(df).transform(
        using=using,
        schema=schema,
        params=params,
        pre_partition=partition,
        ignore_errors=ignore_errors,
    ).yield_dataframe_as("result")
    result = dag.run(engine, conf=engine_conf)["result"]
    if isinstance(df, DataFrame):
        return df
    return result.as_pandas() if result.is_local else result.native  # type:ignore


def out_transform(
    df: Any,
    using: Any,
    params: Any = None,
    partition: Any = None,
    ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
    engine: Any = None,
    engine_conf: Any = None,
) -> None:
    dag = FugueWorkflow()
    dag.df(df).out_transform(
        using=using,
        params=params,
        pre_partition=partition,
        ignore_errors=ignore_errors,
    )
    dag.run(engine, conf=engine_conf)
