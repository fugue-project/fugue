from collections import defaultdict
from threading import RLock
from typing import Any, Dict, Iterable, List, Optional, Set, TypeVar

from adagio.specs import WorkflowSpec
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame
from fugue.dataframe.dataframes import DataFrames
from fugue.exceptions import FugueWorkflowError
from fugue.extensions._builtins import (
    AssertEqual,
    CreateData,
    DropColumns,
    Load,
    Rename,
    RunJoin,
    Distinct,
    RunSetOperation,
    RunSQLSelect,
    RunTransformer,
    Save,
    SelectColumns,
    Show,
    Zip,
)
from fugue.extensions.transformer.convert import _to_transformer
from fugue.workflow._tasks import Create, FugueTask, Output, Process
from fugue.workflow._workflow_context import (
    FugueWorkflowContext,
    _FugueInteractiveWorkflowContext,
)
from triad.collections import Schema
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_or_throw

_DEFAULT_IGNORE_ERRORS: List[Any] = []

TDF = TypeVar("TDF", bound="WorkflowDataFrame")


class WorkflowDataFrame(DataFrame):
    """It represents the edges in the graph constructed by :class:`~.FugueWorkflow`.
    In Fugue, we use DAG to represent workflows, and the edges are strictly
    dataframes. DAG construction and execution are different steps, this class is
    used in the construction step. Although it inherits from
    :class:`~fugue.dataframe.dataframe.DataFrame`, it's not concerete data. So a
    lot of the operations are not allowed. If you want to obtain the concrete
    Fugue :class:`~fugue.dataframe.dataframe.DataFrame`, use :meth:`~.compute()`
    to execute the workflow.

    Normally, you don't construct it by yourself, you will just use the methods of it.

    :param workflow: the parent workflow it belongs to
    :param task: the task that generates this dataframe
    :param metadata: dict-like metadata, defaults to None
    """

    def __init__(
        self, workflow: "FugueWorkflow", task: FugueTask, metadata: Any = None
    ):
        super().__init__("_0:int", metadata)
        self._workflow = workflow
        self._task = task

    def spec_uuid(self) -> str:
        """UUID of its task spec"""
        return self._task.__uuid__()

    @property
    def name(self) -> str:
        """Name of its task spec"""
        return self._task.name

    @property
    def workflow(self) -> "FugueWorkflow":
        """The parent workflow"""
        return self._workflow

    @property
    def result(self) -> DataFrame:
        """The concrete DataFrame obtained from :meth:`~.compute()`.
        This property will not trigger compute again, but compute should
        have been called earlier and the result is cached.
        """
        return self.workflow.get_result(self)

    def compute(self, *args, **kwargs) -> DataFrame:
        """Trigger the parent workflow to
        :meth:`~fugue.workflow.workflow.FugueWorkflow.run` and to generate and cache
        the result dataframe this instance represent.

        :Examples:

        >>> df = FugueWorkflow().df([[0]],"a:int").transform(a_transformer)
        >>> df.compute().as_pandas()  # pandas dataframe
        >>> df.compute(SparkExecutionEngine).native  # spark dataframe

        :Notice:

        Consider using :meth:`fugue.workflow.workflow.FugueWorkflow.run` instead.
        Because this method actually triggers the entire workflow to run, so it may
        be confusing to use this method because extra time may be taken to compute
        unrelated dataframes.

        .. code-block:: python

            dag = FugueWorkflow()
            df1 = dag.df([[0]],"a:int").transform(a_transformer)
            df2 = dag.df([[0]],"b:int")

            dag.run(SparkExecutionEngine)
            df1.result.show()
            df2.result.show()
        """
        # TODO: it computes entire graph
        self.workflow.run(*args, **kwargs)
        return self.result

    def process(
        self: TDF,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
    ) -> TDF:
        """Run a processor on this dataframe. It's a simple wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.process`

        Please read the :ref:`Processor Tutorial <tutorial:/tutorials/processor.ipynb>`

        :param using: processor-like object
        :param schema: |SchemaLikeObject|, defaults to None. The processor
          will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.output_schema`
        :param params: |ParamsLikeObject| to run the processor, defaults to None.
          The processor will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The processor will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`
        :return: result dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        if pre_partition is None:
            pre_partition = self._metadata.get("pre_partition", PartitionSpec())
        df = self.workflow.process(
            self, using=using, schema=schema, params=params, pre_partition=pre_partition
        )
        return self._to_self_type(df)

    def output(self, using: Any, params: Any = None, pre_partition: Any = None) -> None:
        """Run a outputter on this dataframe. It's a simple wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.output`

        Please read the :ref:`Outputter Tutorial <tutorial:/tutorials/outputter.ipynb>`

        :param using: outputter-like object
        :param params: |ParamsLikeObject| to run the outputter, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`
        """
        if pre_partition is None:
            pre_partition = self._metadata.get("pre_partition", PartitionSpec())
        self.workflow.output(
            self, using=using, params=params, pre_partition=pre_partition
        )

    def show(
        self,
        rows: int = 10,
        show_count: bool = False,
        title: Optional[str] = None,
        best_width: int = 100,
    ) -> None:
        """Show the dataframe.
        See :ref:`examples <tutorial:/tutorials/dag.ipynb#initialize-a-workflow>`.

        :param rows: max number of rows, defaults to 10
        :param show_count: whether to show total count, defaults to False
        :param title: title to display on top of the dataframe, defaults to None
        :param best_width: max width for the output table, defaults to 100

        :Notice:

        * When you call this method, it means you want the dataframe to be
          printed when the workflow executes. So the dataframe won't show until
          you run the workflow.
        * When ``show_count`` is True, it can trigger expensive calculation for
          a distributed dataframe. So if you call this function directly, you may
          need to :meth:`~.persist` the dataframe. Or you can turn on
          :ref:`tutorial:/tutorials/useful_config.ipynb#auto-persist`
        """
        # TODO: best_width is not used
        self.workflow.show(self, rows=rows, show_count=show_count, title=title)

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        """Wrapper of :meth:`fugue.workflow.workflow.FugueWorkflow.assert_eq` to
        compare this dataframe with other dataframes.

        :param dfs: |DataFramesLikeObject|
        :param digits: precision on float number comparison, defaults to 8
        :param check_order: if to compare the row orders, defaults to False
        :param check_schema: if compare schemas, defaults to True
        :param check_content: if to compare the row values, defaults to True
        :param check_metadata: if to compare the dataframe metadatas, defaults to True
        :param no_pandas: if true, it will compare the string representations of the
          dataframes, otherwise, it will convert both to pandas dataframe to compare,
          defaults to False

        :raises AssertionError: if not equal
        """
        self.workflow.assert_eq(self, *dfs, **params)

    def transform(
        self: TDF,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
        ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
    ) -> TDF:
        """Transform this dataframe using transformer. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.transform`

        Please read the
        :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`

        :param using: transformer-like object
        :param schema: |SchemaLikeObject|, defaults to None. The transformer
          will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.output_schema`
        :param params: |ParamsLikeObject| to run the processor, defaults to None.
          The transformer will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None. It's
          recommended to use the equivalent wayt, which is to call
          :meth:`~.partition` and then call :meth:`~.transform` without this parameter
        :param ignore_errors: list of exception types the transformer can ignore,
          defaults to empty list
        :return: the transformed dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        if pre_partition is None:
            pre_partition = self._metadata.get("pre_partition", PartitionSpec())
        df = self.workflow.transform(
            self,
            using=using,
            schema=schema,
            params=params,
            pre_partition=pre_partition,
            ignore_errors=ignore_errors,
        )
        return self._to_self_type(df)

    def join(self: TDF, *dfs: Any, how: str, on: Optional[Iterable[str]] = None) -> TDF:
        """Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param how: can accept ``semi``, ``left_semi``, ``anti``, ``left_anti``,
          ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        df = self.workflow.join(self, *dfs, how=how, on=on)
        return self._to_self_type(df)

    def inner_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        """INNER Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="inner", on=on)

    def semi_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        """LEFT SEMI Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="semi", on=on)

    def left_semi_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        """LEFT SEMI Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="left_semi", on=on)

    def anti_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        """LEFT ANTI Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="anti", on=on)

    def left_anti_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        """LEFT ANTI Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="left_anti", on=on)

    def left_outer_join(
        self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None
    ) -> TDF:
        """LEFT OUTER Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="left_outer", on=on)

    def right_outer_join(
        self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None
    ) -> TDF:
        """RIGHT OUTER Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="right_outer", on=on)

    def full_outer_join(
        self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None
    ) -> TDF:
        """CROSS Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="full_outer", on=on)

    def cross_join(self: TDF, *dfs: Any) -> TDF:
        """CROSS Join this dataframe with dataframes. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.join`. |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :return: joined dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        return self.join(*dfs, how="cross")

    def union(self: TDF, *dfs: Any, distinct: bool = True) -> TDF:
        """Union this dataframe with ``dfs``.

        :param dfs: |DataFramesLikeObject|
        :param distinct: whether to perform `distinct` after union,
          default to True
        :return: unioned dataframe

        :Notice:

        Currently, all dataframes in ``dfs`` must have identical schema, otherwise
        exception will be thrown.
        """
        df = self.workflow.union(self, *dfs, distinct=distinct)
        return self._to_self_type(df)

    def subtract(self: TDF, *dfs: Any, distinct: bool = True) -> TDF:
        """Subtract ``dfs`` from this dataframe.

        :param dfs: |DataFramesLikeObject|
        :param distinct: whether to perform `distinct` after subtraction,
          default to True
        :return: subtracted dataframe

        :Notice:

        Currently, all dataframes in ``dfs`` must have identical schema, otherwise
        exception will be thrown.
        """
        df = self.workflow.subtract(self, *dfs, distinct=distinct)
        return self._to_self_type(df)

    def intersect(self: TDF, *dfs: Any, distinct: bool = True) -> TDF:
        """Intersect this dataframe with ``dfs``.

        :param dfs: |DataFramesLikeObject|
        :param distinct: whether to perform `distinct` after intersection,
          default to True
        :return: intersected dataframe

        :Notice:

        Currently, all dataframes in ``dfs`` must have identical schema, otherwise
        exception will be thrown.
        """
        df = self.workflow.intersect(self, *dfs, distinct=distinct)
        return self._to_self_type(df)

    def distinct(self: TDF) -> TDF:
        """Get distinct dataframe. Equivalent to ``SELECT DISTINCT * FROM df``

        :return: dataframe with unique records
        """
        df = self.workflow.process(self, using=Distinct)
        return self._to_self_type(df)

    def checkpoint(self: TDF, namespace: Any = None) -> TDF:
        """[CURRENTLY NO EFFECT] set checkpoint for the current dataframe

        :return: dataframe loaded from checkpoint
        :rtype: :class:`~.WorkflowDataFrame`
        """
        self._task.checkpoint(namespace)
        return self

    def persist(self: TDF, level: Any = None) -> TDF:
        """Persist the current dataframe

        :param level: the parameter passed to the underlying framework, defaults to None
        :return: the persisted dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        self._task.persist("" if level is None else level)
        return self

    def broadcast(self: TDF) -> TDF:
        """Broadcast the current dataframe

        :return: the broadcasted dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        self._task.broadcast()
        return self

    def partition(self: TDF, *args, **kwargs) -> TDF:
        """Partition the current dataframe. Please read |PartitionTutorial|

        :param args: |PartitionLikeObject|
        :param kwargs: |PartitionLikeObject|
        :return: dataframe with the partition hint
        :rtype: :class:`~.WorkflowDataFrame`

        :Notice:

        Normally this step is fast because it's to add a partition hint
        for the next step.
        """
        return self._to_self_type(
            WorkflowDataFrame(
                self.workflow,
                self._task,
                {"pre_partition": PartitionSpec(*args, **kwargs)},
            )
        )

    def _to_self_type(self: TDF, df: "WorkflowDataFrame") -> TDF:
        return df  # type: ignore

    def drop(  # type: ignore
        self: TDF, columns: List[str], if_exists: bool = False
    ) -> TDF:
        """Drop columns from the dataframe.

        :param columns: columns to drop
        :param if_exists: if setting to True, it will ignore non-existent columns,
          defaults to False
        :return: the dataframe after dropping columns
        :rtype: :class:`~.WorkflowDataFrame`
        """
        df = self.workflow.process(
            self, using=DropColumns, params=dict(columns=columns, if_exists=if_exists)
        )
        return self._to_self_type(df)

    def rename(self: TDF, *args: Any, **kwargs: Any) -> TDF:
        """Rename the dataframe using a mapping dict

        :param args: list of dicts containing rename maps
        :param kwargs: rename map
        :return: a new dataframe with the new names
        :rtype: :class:`~.WorkflowDataFrame`

        :Notice:

        This interface is more flexible than
        :meth:`fugue.dataframe.dataframe.DataFrame.rename`

        :Examples:

        >>> df.rename({"a": "b"}, c="d", e="f")
        """
        m: Dict[str, str] = {}
        for a in args:
            m.update(a)
        m.update(kwargs)
        df = self.workflow.process(self, using=Rename, params=dict(columns=m))
        return self._to_self_type(df)

    def zip(
        self: TDF,
        *dfs: Any,
        how: str = "inner",
        partition: Any = None,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
    ) -> TDF:
        """Zip this data frame with multiple dataframes together
        with given partition specifications. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.zip`.

        :param dfs: |DataFramesLikeObject|
        :param how: can accept ``inner``, ``left_outer``, ``right_outer``,
          ``full_outer``, ``cross``, defaults to ``inner``
        :param partition: |PartitionLikeObject|, defaults to None.
        :param temp_path: file path to store the data (used only if the serialized data
          is larger than ``to_file_threshold``), defaults to None
        :param to_file_threshold: file byte size threshold, defaults to -1

        :return: a zipped dataframe
        :rtype: :class:`~.WorkflowDataFrame`

        :Notice:

        * ``dfs`` must be list like, the zipped dataframe will be list like
        * ``dfs`` is fine to be empty
        * If you want dict-like zip, use
          :meth:`fugue.workflow.workflow.FugueWorkflow.zip`

        Read :ref:`CoTransformer <tutorial:/tutorials/dag.ipynb#cotransformer>`
        and :ref:`Zip & Comap <tutorial:/tutorials/execution_engine.ipynb#zip-&-comap>`
        for details
        """
        if partition is None:
            partition = self._metadata.get("pre_partition", PartitionSpec())
        df = self.workflow.zip(
            self,
            *dfs,
            how=how,
            partition=partition,
            temp_path=temp_path,
            to_file_threshold=to_file_threshold,
        )
        return self._to_self_type(df)

    def __getitem__(self: TDF, columns: List[Any]) -> TDF:
        df = self.workflow.process(
            self, using=SelectColumns, params=dict(columns=columns)
        )
        return self._to_self_type(df)

    def save(
        self,
        path: str,
        fmt: str = "",
        mode: str = "overwrite",
        partition: Any = None,
        single: bool = False,
        **kwargs: Any,
    ) -> None:
        """Save this dataframe to a persistent storage

        :param path: output path
        :param fmt: format hint can accept ``parquet``, ``csv``, ``json``,
          defaults to None, meaning to infer
        :param mode: can accept ``overwrite``, ``append``, ``error``,
          defaults to "overwrite"
        :param partition: |PartitionLikeObject|, how to partition the
          dataframe before saving, defaults to empty
        :param single: force the output as a single file, defaults to False
        :param kwargs: parameters to pass to the underlying framework

        For more details and examples, read
        :ref:`Save & Load <tutorial:/tutorials/dag.ipynb#save-&-load>`.
        """
        if partition is None:
            partition = self._metadata.get("pre_partition", PartitionSpec())
        self.workflow.output(
            self,
            using=Save,
            pre_partition=partition,
            params=dict(path=path, fmt=fmt, mode=mode, single=single, params=kwargs),
        )

    @property
    def schema(self) -> Schema:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    @property
    def is_local(self) -> bool:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def as_local(self) -> DataFrame:  # type: ignore  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    @property
    def is_bounded(self) -> bool:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    @property
    def empty(self) -> bool:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    @property
    def num_partitions(self) -> int:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def peek_array(self) -> Any:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def count(self) -> int:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:  # pragma: no cover
        """
        :raises NotImplementedError: don't call this method
        """
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def _drop_cols(self: TDF, cols: List[str]) -> DataFrame:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def _select_cols(self, keys: List[Any]) -> DataFrame:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")


class FugueWorkflow(object):
    """Fugue Workflow, also known as the Fugue Programming Interface.

    In Fugue, we use DAG to represent workflows, DAG construction and execution
    are different steps, this class is mainly used in the construction step, so all
    things you added to the workflow is **description** and they are not executed
    until you call :meth:`~.run`

    Read :ref:`The Tutorial <tutorial:/tutorials/dag.ipynb#initialize-a-workflow>`
    to learn how to initialize it in different ways and pros and cons.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        self._lock = RLock()
        self._spec = WorkflowSpec()
        self._workflow_ctx = self._to_ctx(*args, **kwargs)
        self._computed = False
        self._graph = _Graph()

    @property
    def conf(self) -> ParamDict:
        """All configs of this workflow and underlying
        :class:`~fugue.execution.execution_engine.ExecutionEngine` (if given)
        """
        return self._workflow_ctx.conf

    def spec_uuid(self) -> str:
        """UUID of the workflow spec (`description`)"""
        return self._spec.__uuid__()

    def run(self, *args: Any, **kwargs: Any) -> None:
        """Execute the workflow and compute all dataframes.
        If not arguments, it will use
        :class:`~fugue.execution.native_execution_engine.NativeExecutionEngine`
        to run the workflow.

        :Examples:

        .. code-block:: python

            dag = FugueWorkflow()
            df1 = dag.df([[0]],"a:int").transform(a_transformer)
            df2 = dag.df([[0]],"b:int")

            dag.run(SparkExecutionEngine)
            df1.result.show()
            df2.result.show()

        Read :ref:`The Tutorial <tutorial:/tutorials/dag.ipynb#initialize-a-workflow>`
        to learn how to run in different ways and pros and cons.
        """
        with self._lock:
            self._computed = False
            if len(args) > 0 or len(kwargs) > 0:
                self._workflow_ctx = self._to_ctx(*args, **kwargs)
            self._workflow_ctx.run(self._spec, {})
            self._computed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.run()

    def get_result(self, df: WorkflowDataFrame) -> DataFrame:
        """After :meth:`~.run`, get the result of a dataframe defined in the dag

        :return: a calculated dataframe

        :Examples:

        .. code-block:: python

            dag = FugueWorkflow()
            df1 = dag.df([[0]],"a:int")
            dag.run()
            dag.get_result(df1).show()
        """
        assert_or_throw(self._computed, FugueWorkflowError("not computed"))
        return self._workflow_ctx.get_result(id(df._task))

    def create(
        self, using: Any, schema: Any = None, params: Any = None
    ) -> WorkflowDataFrame:
        """Run a creator to create a dataframe.

        Please read the :ref:`Creator Tutorial <tutorial:/tutorials/creator.ipynb>`

        :param using: creator-like object
        :param schema: |SchemaLikeObject|, defaults to None. The creator
          will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.output_schema`
        :param params: |ParamsLikeObject| to run the creator,
          defaults to None. The creator will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The creator will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`
        :return: result dataframe
        """
        task = Create(creator=using, schema=schema, params=params)
        return self.add(task)

    def process(
        self,
        *dfs: Any,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
    ) -> WorkflowDataFrame:
        """Run a processor on the dataframes.

        Please read the :ref:`Processor Tutorial <tutorial:/tutorials/processor.ipynb>`

        :param dfs: |DataFramesLikeObject|
        :param using: processor-like object
        :param schema: |SchemaLikeObject|, defaults to None. The processor
          will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.output_schema`
        :param params: |ParamsLikeObject| to run the processor, defaults to None.
          The processor will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The processor will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`

        :return: result dataframe
        """
        dfs = self._to_dfs(*dfs)
        task = Process(
            len(dfs),
            processor=using,
            schema=schema,
            params=params,
            pre_partition=pre_partition,
            input_names=None if not dfs.has_key else list(dfs.keys()),
        )
        if dfs.has_key:
            return self.add(task, **dfs)
        else:
            return self.add(task, *dfs.values())

    def output(
        self, *dfs: Any, using: Any, params: Any = None, pre_partition: Any = None
    ) -> None:
        """Run a outputter on dataframes.

        Please read the :ref:`Outputter Tutorial <tutorial:/tutorials/outputter.ipynb>`

        :param using: outputter-like object
        :param params: |ParamsLikeObject| to run the outputter, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`
        """
        dfs = self._to_dfs(*dfs)
        task = Output(
            len(dfs),
            outputter=using,
            params=params,
            pre_partition=pre_partition,
            input_names=None if not dfs.has_key else list(dfs.keys()),
        )
        if dfs.has_key:
            self.add(task, **dfs)
        else:
            self.add(task, *dfs.values())

    def create_data(
        self, data: Any, schema: Any = None, metadata: Any = None
    ) -> WorkflowDataFrame:
        """Create dataframe.

        :param data: |DataFrameLikeObject|
        :param schema: |SchemaLikeObject|, defaults to None
        :param metadata: |ParamsLikeObject|, defaults to None
        :return: a dataframe of the current workflow
        """
        if isinstance(data, WorkflowDataFrame):
            assert_or_throw(
                data.workflow is self, f"{data} does not belong to this workflow"
            )
            return data
        schema = None if schema is None else Schema(schema)
        return self.create(
            using=CreateData, params=dict(data=data, schema=schema, metadata=metadata)
        )

    def df(
        self, data: Any, schema: Any = None, metadata: Any = None
    ) -> WorkflowDataFrame:
        """Create dataframe. Alias of :meth:`~.create_data`

        :param data: |DataFrameLikeObject|
        :param schema: |SchemaLikeObject|, defaults to None
        :param metadata: |ParamsLikeObject|, defaults to None
        :return: a dataframe of the current workflow
        """
        return self.create_data(data, schema, metadata)

    def load(
        self, path: str, fmt: str = "", columns: Any = None, **kwargs: Any
    ) -> WorkflowDataFrame:
        """Load dataframe from persistent storage.
        Read :ref:`this <tutorial:/tutorials/dag.ipynb#save-&-load>` for details

        :param path: file path
        :param fmt: format hint can accept ``parquet``, ``csv``, ``json``,
          defaults to "", meaning to infer
        :param columns: list of columns or a |SchemaLikeObject|, defaults to None
        :return: dataframe from the file
        :rtype: WorkflowDataFrame
        """
        return self.create(
            using=Load, params=dict(path=path, fmt=fmt, columns=columns, params=kwargs)
        )

    def show(
        self,
        *dfs: Any,
        rows: int = 10,
        show_count: bool = False,
        title: Optional[str] = None,
    ) -> None:
        """Show the dataframes.
        See :ref:`examples <tutorial:/tutorials/dag.ipynb#initialize-a-workflow>`.

        :param dfs: |DataFramesLikeObject|
        :param rows: max number of rows, defaults to 10
        :param show_count: whether to show total count, defaults to False
        :param title: title to display on top of the dataframe, defaults to None
        :param best_width: max width for the output table, defaults to 100

        :Notice:

        * When you call this method, it means you want the dataframe to be
          printed when the workflow executes. So the dataframe won't show until
          you run the workflow.
        * When ``show_count`` is True, it can trigger expensive calculation for
          a distributed dataframe. So if you call this function directly, you may
          need to :meth:`~.WorkflowDataFrame.persist` the dataframe. Or you can turn on
          :ref:`tutorial:/tutorials/useful_config.ipynb#auto-persist`
        """
        self.output(
            *dfs, using=Show, params=dict(rows=rows, show_count=show_count, title=title)
        )

    def join(
        self, *dfs: Any, how: str, on: Optional[Iterable[str]] = None
    ) -> WorkflowDataFrame:
        """Join dataframes.
        |ReadJoin|

        :param dfs: |DataFramesLikeObject|
        :param how: can accept ``semi``, ``left_semi``, ``anti``, ``left_anti``,
          ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys. Default to None
        :return: joined dataframe
        """
        _on: List[str] = list(on) if on is not None else []
        return self.process(*dfs, using=RunJoin, params=dict(how=how, on=_on))

    def set_op(self, how: str, *dfs: Any, distinct: bool = True) -> WorkflowDataFrame:
        """Union, subtract or intersect dataframes.

        :param how: can accept ``union``, ``left_semi``, ``anti``, ``left_anti``,
          ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
        :param dfs: |DataFramesLikeObject|
        :param distinct: whether to perform `distinct` after the set operation,
          default to True
        :return: result dataframe of the set operation

        :Notice:

        Currently, all dataframes in ``dfs`` must have identical schema, otherwise
        exception will be thrown.
        """
        return self.process(
            *dfs, using=RunSetOperation, params=dict(how=how, distinct=distinct)
        )

    def union(self, *dfs: Any, distinct: bool = True) -> WorkflowDataFrame:
        """Union dataframes in ``dfs``.

        :param dfs: |DataFramesLikeObject|
        :param distinct: whether to perform `distinct` after union,
          default to True
        :return: unioned dataframe

        :Notice:

        Currently, all dataframes in ``dfs`` must have identical schema, otherwise
        exception will be thrown.
        """
        return self.set_op("union", *dfs, distinct=distinct)

    def subtract(self, *dfs: Any, distinct: bool = True) -> WorkflowDataFrame:
        """Subtract ``dfs[1:]`` from ``dfs[0]``.

        :param dfs: |DataFramesLikeObject|
        :param distinct: whether to perform `distinct` after subtraction,
          default to True
        :return: subtracted dataframe

        :Notice:

        Currently, all dataframes in ``dfs`` must have identical schema, otherwise
        exception will be thrown.
        """
        return self.set_op("subtract", *dfs, distinct=distinct)

    def intersect(self, *dfs: Any, distinct: bool = True) -> WorkflowDataFrame:
        """Intersect dataframes in ``dfs``.

        :param dfs: |DataFramesLikeObject|
        :param distinct: whether to perform `distinct` after intersection,
          default to True
        :return: intersected dataframe

        :Notice:

        Currently, all dataframes in ``dfs`` must have identical schema, otherwise
        exception will be thrown.
        """
        return self.set_op("intersect", *dfs, distinct=distinct)

    def zip(
        self,
        *dfs: Any,
        how: str = "inner",
        partition: Any = None,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
    ) -> WorkflowDataFrame:
        """Zip multiple dataframes together with given partition
        specifications.

        :param dfs: |DataFramesLikeObject|
        :param how: can accept ``inner``, ``left_outer``, ``right_outer``,
          ``full_outer``, ``cross``, defaults to ``inner``
        :param partition: |PartitionLikeObject|, defaults to None.
        :param temp_path: file path to store the data (used only if the serialized data
          is larger than ``to_file_threshold``), defaults to None
        :param to_file_threshold: file byte size threshold, defaults to -1

        :return: a zipped dataframe

        :Notice:

        * If ``dfs`` is dict like, the zipped dataframe will be dict like,
          If ``dfs`` is list like, the zipped dataframe will be list like
        * It's fine to contain only one dataframe in ``dfs``

        Read :ref:`CoTransformer <tutorial:/tutorials/dag.ipynb#cotransformer>`
        and :ref:`Zip & Comap <tutorial:/tutorials/execution_engine.ipynb#zip-&-comap>`
        for details
        """
        return self.process(
            *dfs,
            using=Zip,
            params=dict(
                how=how, temp_path=temp_path, to_file_threshold=to_file_threshold
            ),
            pre_partition=partition,
        )

    def transform(
        self,
        *dfs: Any,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
        ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
    ) -> WorkflowDataFrame:
        """Transform dataframes using transformer.

        Please read the
        :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`

        :param dfs: |DataFramesLikeObject|
        :param using: transformer-like object
        :param schema: |SchemaLikeObject|, defaults to None. The transformer
          will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.output_schema`
        :param params: |ParamsLikeObject| to run the processor, defaults to None.
          The transformer will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None. It's
          recommended to use the equivalent wayt, which is to call
          :meth:`~.partition` and then call :meth:`~.transform` without this parameter
        :param ignore_errors: list of exception types the transformer can ignore,
          defaults to empty list
        :return: the transformed dataframe
        """
        assert_or_throw(
            len(dfs) == 1,
            NotImplementedError("transform supports only single dataframe"),
        )
        tf = _to_transformer(using, schema)
        return self.process(
            *dfs,
            using=RunTransformer,
            schema=None,
            params=dict(transformer=tf, ignore_errors=ignore_errors, params=params),
            pre_partition=pre_partition,
        )

    def select(self, *statements: Any, sql_engine: Any = None) -> WorkflowDataFrame:
        """Execute ``SELECT`` statement using
        :class:`~fugue.execution.execution_engine.SQLEngine`

        :param statements: a list of sub-statements in string
          or :class:`~.WorkflowDataFrame`
        :param sql_engine: :class:`~fugue.execution.execution_engine.SQLEngine`
          type for this select statement, defaults to None to use default sql engine
        :return: result of the ``SELECT`` statement

        :Example:

        .. code-block:: python

            with FugueWorkflow() as dag:
                a = dag.df([[0,"a"]],a:int,b:str)
                b = dag.df([[0]],a:int)
                c = dag.select("SELECT a FROM",a,"UNION SELECT * FROM",b)

        Please read :ref:`this <tutorial:/tutorials/dag.ipynb#select-query>`
        for more examples
        """
        s_str: List[str] = []
        dfs: Dict[str, DataFrame] = {}
        for s in statements:
            if isinstance(s, str):
                s_str.append(s)
            if isinstance(s, DataFrame):
                ws = self.df(s)
                dfs[ws.name] = ws
                s_str.append(ws.name)
        sql = " ".join(s_str).strip()
        if not sql.upper().startswith("SELECT"):
            sql = "SELECT " + sql
        return self.process(
            dfs, using=RunSQLSelect, params=dict(statement=sql, sql_engine=sql_engine)
        )

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        """Compare if these dataframes are equal. Is for internal, unit test
        purpose only. It will convert both dataframes to
        :class:`~fugue.dataframe.dataframe.LocalBoundedDataFrame`, so it assumes
        all dataframes are small and fast enough to convert. DO NOT use it
        on critical or expensive tasks.

        :param dfs: |DataFramesLikeObject|
        :param digits: precision on float number comparison, defaults to 8
        :param check_order: if to compare the row orders, defaults to False
        :param check_schema: if compare schemas, defaults to True
        :param check_content: if to compare the row values, defaults to True
        :param check_metadata: if to compare the dataframe metadatas, defaults to True
        :param no_pandas: if true, it will compare the string representations of the
          dataframes, otherwise, it will convert both to pandas dataframe to compare,
          defaults to False

        :raises AssertionError: if not equal
        """
        self.output(*dfs, using=AssertEqual, params=params)

    def add(self, task: FugueTask, *args: Any, **kwargs: Any) -> WorkflowDataFrame:
        """This method should not be called directly by users. Use
        :meth:`~.create`, :meth:`~.process`, :meth:`~.output` instead
        """
        assert_or_throw(task._node_spec is None, f"can't reuse {task}")
        dep = _Dependencies(self, task, {}, *args, **kwargs)
        name = "_" + str(len(self._spec.tasks))
        wt = self._spec.add_task(name, task, dep.dependency)
        # TODO: this is auto persist, the implementation needs imrpovement
        for v in dep.dependency.values():
            v = v.split(".")[0]
            self._graph.add(name, v)
            if len(self._graph.down[v]) > 1 and self.conf.get(
                "fugue.workflow.auto_persist", False
            ):
                self._spec.tasks[v]._persist = self.conf.get(
                    "fugue.workflow.auto_persist_value", ""
                )
        return WorkflowDataFrame(self, wt)

    def _to_dfs(self, *args: Any, **kwargs: Any) -> DataFrames:
        return DataFrames(*args, **kwargs).convert(self.create_data)

    def _to_ctx(self, *args: Any, **kwargs) -> FugueWorkflowContext:
        if len(args) == 1 and isinstance(args[0], FugueWorkflowContext):
            return args[0]
        return FugueWorkflowContext(*args, **kwargs)


class _FugueInteractiveWorkflow(FugueWorkflow):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__()
        self._workflow_ctx = self._to_ctx(*args, **kwargs)

    def run(self, *args: Any, **kwargs: Any) -> None:
        assert_or_throw(
            len(args) == 0 and len(kwargs) == 0,
            FugueWorkflowError(
                "can't reset workflow context in _FugueInteractiveWorkflow"
            ),
        )
        with self._lock:
            self._computed = False
            self._workflow_ctx.run(self._spec, {})
            self._computed = True

    def get_result(self, df: WorkflowDataFrame) -> DataFrame:
        return self._workflow_ctx.get_result(id(df._task))

    def __enter__(self):
        raise FugueWorkflowError(
            "with statement is invalid for _FugueInteractiveWorkflow"
        )

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        raise FugueWorkflowError(  # pragma: no cover
            "with statement is invalid for _FugueInteractiveWorkflow"
        )

    def add(self, task: FugueTask, *args: Any, **kwargs: Any) -> WorkflowDataFrame:
        df = super().add(task, *args, **kwargs)
        self.run()
        return df

    def _to_ctx(self, *args: Any, **kwargs) -> _FugueInteractiveWorkflowContext:
        if len(args) == 1 and isinstance(args[0], _FugueInteractiveWorkflowContext):
            return args[0]
        return _FugueInteractiveWorkflowContext(*args, **kwargs)


class _Dependencies(object):
    def __init__(
        self,
        workflow: "FugueWorkflow",
        task: FugueTask,
        local_vars: Dict[str, Any],
        *args: Any,
        **kwargs: Any,
    ):
        self.workflow = workflow
        self._local_vars = local_vars
        self.dependency: Dict[str, str] = {}
        for i in range(len(args)):
            key = task.inputs.get_key_by_index(i)
            self.dependency[key] = self._parse_single_dependency(args[i])
        for k, v in kwargs.items():
            self.dependency[k] = self._parse_single_dependency(v)

    def _parse_single_dependency(self, dep: Any) -> str:
        # if isinstance(dep, tuple):  # (cursor_like_obj, output_name)
        #     cursor = self._parse_cursor(dep[0])
        #     return cursor._task.name + "." + dep[1]
        return self._parse_cursor(dep)._task.single_output_expression

    def _parse_cursor(self, dep: Any) -> WorkflowDataFrame:
        if isinstance(dep, WorkflowDataFrame):
            return dep
        # if isinstance(dep, DataFrame):
        #     return self.workflow.create_data(dep)
        # if isinstance(dep, str):
        #     assert_or_throw(
        #         dep in self._local_vars, KeyError(f"{dep} is not a local variable")
        #     )
        #     if isinstance(self._local_vars[dep], WorkflowDataFrame):
        #         return self._local_vars[dep]
        #     # TODO: should also accept dataframe?
        #     raise TypeError(f"{self._local_vars[dep]} is not a valid dependency type")
        raise TypeError(f"{dep} is not a valid dependency type")  # pragma: no cover


# TODO: this should not exist, dependency libraries should do the job
class _Graph(object):
    def __init__(self):
        self.down: Dict[str, Set[str]] = defaultdict(set)
        self.up: Dict[str, Set[str]] = defaultdict(set)

    def add(self, name: str, depend_on: str) -> None:
        depend_on = depend_on.split(".")[0]
        self.down[depend_on].add(name)
        self.up[name].add(depend_on)
