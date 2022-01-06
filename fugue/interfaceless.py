from typing import Any, List, Optional

from fugue.collections.yielded import Yielded
from fugue.constants import FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT
from fugue.dataframe import DataFrame
from fugue.workflow import FugueWorkflow


def transform(
    df: Any,
    using: Any,
    schema: Any = None,
    params: Any = None,
    partition: Any = None,
    callback: Any = None,
    ignore_errors: Optional[List[Any]] = None,
    engine: Any = None,
    engine_conf: Any = None,
    force_output_fugue_dataframe: bool = False,
    persist: bool = False,
    as_local: bool = False,
) -> Any:
    """Transform this dataframe using transformer. It's a wrapper of
    :meth:`~fugue.workflow.workflow.FugueWorkflow.transform` and
    :meth:`~fugue.workflow.workflow.FugueWorkflow.run`. It let you do the
    basic dataframe transformation without using
    :class:`~fugue.workflow.workflow.FugueWorkflow` and
    :class:`~fugue.dataframe.dataframe.DataFrame`. Both input and output
    can be native types only.

    Please read |TransformerTutorial|

    :param using: transformer-like object, can't be a string expression
    :param schema: |SchemaLikeObject|, defaults to None. The transformer
      will be able to access this value from
      :meth:`~fugue.extensions.context.ExtensionContext.output_schema`
    :param params: |ParamsLikeObject| to run the processor, defaults to None.
      The transformer will be able to access this value from
      :meth:`~fugue.extensions.context.ExtensionContext.params`
    :param partition: |PartitionLikeObject|, defaults to None
    :param callback: |RPCHandlerLikeObject|, defaults to None
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
    :param force_output_fugue_dataframe: If true, the function will always return
      a ``FugueDataFrame``, otherwise, if ``df`` is in native dataframe types such
      as pandas dataframe, then the output will also in its native format. Defaults
      to False
    :param persist: Whether to persist(materialize) the dataframe before returning
    :param as_local: If true, the result will be converted to a ``LocalDataFrame``

    :return: the transformed dataframe, if ``df`` is a native dataframe (e.g.
      pd.DataFrame, spark dataframe, etc), the output will be a native dataframe,
      the type is determined by the execution engine you use. But if ``df`` is
      of type :class:`~fugue.dataframe.dataframe.DataFrame`, then the output will
      also be a :class:`~fugue.dataframe.dataframe.DataFrame`

    .. note::

      This function may be lazy and return the transformed dataframe.

    .. note::

      When you use callback in this function, you must be careful that the output
      dataframe must be materialized. Otherwise, if the real compute happens out of
      the function call, the callback receiver is already shut down. To do that you
      can either use ``persist`` or ``as_local``, both will materialize the dataframe
      before the callback receiver shuts down.
    """
    dag = FugueWorkflow(conf={FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT: 0})
    tdf = dag.df(df).transform(
        using=using,
        schema=schema,
        params=params,
        pre_partition=partition,
        callback=callback,
        ignore_errors=ignore_errors or [],
    )
    if persist:
        tdf = tdf.persist()
    tdf.yield_dataframe_as("result", as_local=as_local)
    result = dag.run(engine, conf=engine_conf)["result"]
    if force_output_fugue_dataframe or isinstance(df, (DataFrame, Yielded)):
        return result
    return result.as_pandas() if result.is_local else result.native  # type:ignore


def out_transform(
    df: Any,
    using: Any,
    params: Any = None,
    partition: Any = None,
    callback: Any = None,
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

    Please read |TransformerTutorial|

    :param using: transformer-like object, can't be a string expression
    :param params: |ParamsLikeObject| to run the processor, defaults to None.
      The transformer will be able to access this value from
      :meth:`~fugue.extensions.context.ExtensionContext.params`
    :param partition: |PartitionLikeObject|, defaults to None.
    :param callback: |RPCHandlerLikeObject|, defaults to None
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

    .. note::

      This transformation is guaranteed to execute immediately (eager)
      and return nothing
    """
    dag = FugueWorkflow(conf={FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT: 0})
    dag.df(df).out_transform(
        using=using,
        params=params,
        pre_partition=partition,
        callback=callback,
        ignore_errors=ignore_errors or [],
    )
    dag.run(engine, conf=engine_conf)
