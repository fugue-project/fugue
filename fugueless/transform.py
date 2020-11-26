from typing import Any, Optional

from fugue import FugueWorkflow

from fugueless._utils import DEFAULT_EXECUTION_ENGINE_SELECTOR


def transform(
    df: Any,
    using: Any,
    params: Any = None,
    schema: Any = None,
    partition: Any = None,
    execution_engine: Optional[str] = None,
) -> Any:
    dag = FugueWorkflow()
    result = dag.df(df).transform(
        using=using, schema=schema, params=params, pre_partition=partition
    )
    engine = DEFAULT_EXECUTION_ENGINE_SELECTOR.get_execution_engine(
        execution_engine, df
    )
    dag.run(engine)
    return result.result.native  # type: ignore
