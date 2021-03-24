from typing import Any, List, Optional

from fugue.dataframe import DataFrame
from fugue.workflow import FugueWorkflow


def transform(
    df: Any,
    using: Any,
    schema: Any = None,
    params: Any = None,
    partition: Any = None,
    ignore_errors: Optional[List[Any]] = None,
    engine: Any = None,
    engine_conf: Any = None,
) -> Any:
    """Transform this dataframe using transformer. It's a wrapper of
    :meth:`~fugue.workflow.workflow.FugueWorkflow.transform` and
    :meth:`~fugue.workflow.workflow.FugueWorkflow.run`. It let you do the
    basic dataframe transformation without using
    :class:`~fugue.workflow.workflow.FugueWorkflow` and
    :class:`~fugue.dataframe.dataframe.DataFrame`. Both input and output
    can be native types only.

    Please read the
    :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`

    :param using: transformer-like object, can't be a string expression
    :param schema: |SchemaLikeObject|, defaults to None. The transformer
      will be able to access this value from
      :meth:`~fugue.extensions.context.ExtensionContext.output_schema`
    :param params: |ParamsLikeObject| to run the processor, defaults to None.
      The transformer will be able to access this value from
      :meth:`~fugue.extensions.context.ExtensionContext.params`
    :param partition: |PartitionLikeObject|, defaults to None.
    :param ignore_errors: list of exception types the transformer can ignore,
      defaults to None (empty list)
    :param engine: it can be empty string or null (use the default execution
      engine), a string (use the registered execution engine), an
      :class:`~fugue.execution.execution_engine.ExecutionEngine` type, or
      the :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
      , or a tuple of two values where the first value represents execution
      engine and the second value represents the sql engine (you can use ``None``
      for either of them to use the default one), defaults to None
    :param engine_conf: |ParamsLikeObject|, defaults to None

    :return: the transformed dataframe, if ``df`` is a native dataframe (e.g.
      pd.DataFrame, spark dataframe, etc), the output will be a native dataframe,
      the type is determined by the execution engine you use. But if ``df`` is
      of type :class:`~fugue.dataframe.dataframe.DataFrame`, then the output will
      also be a :class:`~fugue.dataframe.dataframe.DataFrame`

    :Notice:

    This function may be lazy and return the transformed dataframe.
    """
    dag = FugueWorkflow()
    dag.df(df).transform(
        using=using,
        schema=schema,
        params=params,
        pre_partition=partition,
        ignore_errors=ignore_errors or [],
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
    ignore_errors: Optional[List[Any]] = None,
    engine: Any = None,
    engine_conf: Any = None,
) -> None:
    """Transform this dataframe using transformer. It's a wrapper of
    :meth:`~fugue.workflow.workflow.FugueWorkflow.out_transform` and
    :meth:`~fugue.workflow.workflow.FugueWorkflow.run`. It let you do the
    basic dataframe transformation without using
    :class:`~fugue.workflow.workflow.FugueWorkflow` and
    :class:`~fugue.dataframe.dataframe.DataFrame`. The input can be native
    type only

    Please read the
    :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`

    :param using: transformer-like object, can't be a string expression
    :param params: |ParamsLikeObject| to run the processor, defaults to None.
      The transformer will be able to access this value from
      :meth:`~fugue.extensions.context.ExtensionContext.params`
    :param partition: |PartitionLikeObject|, defaults to None.
    :param ignore_errors: list of exception types the transformer can ignore,
      defaults to None (empty list)
    :param engine: it can be empty string or null (use the default execution
      engine), a string (use the registered execution engine), an
      :class:`~fugue.execution.execution_engine.ExecutionEngine` type, or
      the :class:`~fugue.execution.execution_engine.ExecutionEngine` instance
      , or a tuple of two values where the first value represents execution
      engine and the second value represents the sql engine (you can use ``None``
      for either of them to use the default one), defaults to None
    :param engine_conf: |ParamsLikeObject|, defaults to None

    :Notice:

    This transformation is guaranteed to execute immediately (eager)
    and return nothing
    """
    dag = FugueWorkflow()
    dag.df(df).out_transform(
        using=using,
        params=params,
        pre_partition=partition,
        ignore_errors=ignore_errors or [],
    )
    dag.run(engine, conf=engine_conf)
