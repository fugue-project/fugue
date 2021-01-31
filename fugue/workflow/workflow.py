from collections import defaultdict
from threading import RLock
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)
from uuid import uuid4

from adagio.specs import WorkflowSpec
from fugue.collections.partition import PartitionSpec
from fugue.collections.yielded import Yielded
from fugue.constants import (
    FUGUE_CONF_WORKFLOW_AUTO_PERSIST,
    FUGUE_CONF_WORKFLOW_AUTO_PERSIST_VALUE,
)
from fugue.dataframe import DataFrame
from fugue.dataframe.dataframes import DataFrames
from fugue.exceptions import FugueWorkflowCompileError, FugueWorkflowError
from fugue.execution import SQLEngine
from fugue.extensions._builtins import (
    AlterColumns,
    AssertEqual,
    AssertNotEqual,
    Distinct,
    DropColumns,
    Dropna,
    Fillna,
    Load,
    LoadYielded,
    Rename,
    RunJoin,
    RunOutputTransformer,
    RunSetOperation,
    RunSQLSelect,
    RunTransformer,
    Sample,
    Save,
    SaveAndUse,
    SelectColumns,
    Show,
    Take,
    Zip,
)
from fugue.extensions.transformer.convert import _to_output_transformer, _to_transformer
from fugue.rpc import to_rpc_handler
from fugue.rpc.base import EmptyRPCHandler
from fugue.workflow._checkpoint import FileCheckpoint, WeakCheckpoint
from fugue.workflow._tasks import Create, CreateData, FugueTask, Output, Process
from fugue.workflow._workflow_context import FugueWorkflowContext
from triad import ParamDict, Schema, assert_or_throw
from fugue.execution.factory import make_execution_engine

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

    @property
    def partition_spec(self) -> PartitionSpec:
        """The partition spec set on the dataframe for next steps to use

        :Examples:

        .. code-block:: python

            dag = FugueWorkflow()
            df = dag.df([[0],[1]], "a:int")
            assert df.partition_spec.empty
            df2 = df.partition(by=["a"])
            assert df.partition_spec.empty
            assert df2.partition_spec == PartitionSpec(by=["a"])
        """
        return self._metadata.get("pre_partition", PartitionSpec())

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

        :param using: processor-like object, can't be a string expression
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
        assert_or_throw(
            not isinstance(using, str), f"processor {using} can't be string expression"
        )
        if pre_partition is None:
            pre_partition = self.partition_spec
        df = self.workflow.process(
            self, using=using, schema=schema, params=params, pre_partition=pre_partition
        )
        return self._to_self_type(df)

    def output(self, using: Any, params: Any = None, pre_partition: Any = None) -> None:
        """Run a outputter on this dataframe. It's a simple wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.output`

        Please read the :ref:`Outputter Tutorial <tutorial:/tutorials/outputter.ipynb>`

        :param using: outputter-like object, can't be a string expression
        :param params: |ParamsLikeObject| to run the outputter, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`
        """
        assert_or_throw(
            not isinstance(using, str), f"outputter {using} can't be string expression"
        )
        if pre_partition is None:
            pre_partition = self.partition_spec
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

    def assert_not_eq(self, *dfs: Any, **params: Any) -> None:
        """Wrapper of :meth:`fugue.workflow.workflow.FugueWorkflow.assert_not_eq` to
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

        :raises AssertionError: if any dataframe is equal to the first dataframe
        """
        self.workflow.assert_not_eq(self, *dfs, **params)

    def transform(
        self: TDF,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
        ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
        callback: Any = None,
    ) -> TDF:
        """Transform this dataframe using transformer. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.transform`

        Please read the
        :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`

        :param using: transformer-like object, can't be a string expression
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
        :param callback: |RPCHandlerLikeObject|, defaults to None
        :return: the transformed dataframe
        :rtype: :class:`~.WorkflowDataFrame`

        :Notice:

        :meth:`~.transform` can be lazy and will return the transformed dataframe,
        :meth:`~.out_transform` is guaranteed to execute immediately (eager) and
        return nothing
        """
        assert_or_throw(
            not isinstance(using, str),
            f"transformer {using} can't be string expression",
        )
        if pre_partition is None:
            pre_partition = self.partition_spec
        df = self.workflow.transform(
            self,
            using=using,
            schema=schema,
            params=params,
            pre_partition=pre_partition,
            ignore_errors=ignore_errors,
            callback=callback,
        )
        return self._to_self_type(df)

    def out_transform(
        self: TDF,
        using: Any,
        params: Any = None,
        pre_partition: Any = None,
        ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
        callback: Any = None,
    ) -> None:
        """Transform this dataframe using transformer. It's a wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.out_transform`

        Please read the
        :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`

        :param using: transformer-like object, can't be a string expression
        :param params: |ParamsLikeObject| to run the processor, defaults to None.
          The transformer will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None. It's
          recommended to use the equivalent wayt, which is to call
          :meth:`~.partition` and then call :meth:`~.transform` without this parameter
        :param ignore_errors: list of exception types the transformer can ignore,
          defaults to empty list
        :param callback: |RPCHandlerLikeObject|, defaults to None

        :Notice:

        :meth:`~.transform` can be lazy and will return the transformed dataframe,
        :meth:`~.out_transform` is guaranteed to execute immediately (eager) and
        return nothing
        """
        assert_or_throw(
            not isinstance(using, str),
            f"output transformer {using} can't be string expression",
        )
        if pre_partition is None:
            pre_partition = self.partition_spec
        self.workflow.out_transform(
            self,
            using=using,
            params=params,
            pre_partition=pre_partition,
            ignore_errors=ignore_errors,
            callback=callback,
        )

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

    def dropna(
        self: TDF, how: str = "any", thresh: int = None, subset: List[str] = None
    ) -> TDF:
        """Drops records containing NA records

        :param how: 'any' or 'all'. 'any' drops rows that contain any nulls.
          'all' drops rows that contain all nulls.
        :param thresh: int, drops rows that have less than thresh non-null values
        :param subset: list of columns to operate on
        :return: dataframe with incomplete records dropped
        """
        params = dict(how=how, thresh=thresh, subset=subset)
        params = {k: v for k, v in params.items() if v is not None}
        df = self.workflow.process(self, using=Dropna, params=params)
        return self._to_self_type(df)

    def fillna(self: TDF, value: Any, subset: List[str] = None) -> TDF:
        """Fills NA values with replacement values

        :param value: if scalar, fills all columns with same value.
            if dictionary, fills NA using the keys as column names and the
            values as the replacement values.
        :param subset: list of columns to operate on. ignored if value is
            a dictionary

        :return: dataframe with NA records filled
        """
        params = dict(value=value, subset=subset)
        params = {k: v for k, v in params.items() if v is not None}
        df = self.workflow.process(self, using=Fillna, params=params)
        return self._to_self_type(df)

    def sample(
        self: TDF,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> TDF:
        """
        Sample dataframe by number of rows or by fraction

        :param n: number of rows to sample, one and only one of ``n`` and ``fact``
          must be set
        :param frac: fraction [0,1] to sample, one and only one of ``n`` and ``fact``
          must be set
        :param replace: whether replacement is allowed. With replacement,
          there may be duplicated rows in the result, defaults to False
        :param seed: seed for randomness, defaults to None

        :return: sampled dataframe
        """
        params: Dict[str, Any] = dict(replace=replace, seed=seed)
        if n is not None:
            params["n"] = n
        if frac is not None:
            params["frac"] = frac
        df = self.workflow.process(self, using=Sample, params=params)
        return self._to_self_type(df)

    def take(self: TDF, n: int, presort: str = None, na_position: str = "last") -> TDF:
        """
        Get the first n rows of a DataFrame per partition. If a presort is defined,
        use the presort before applying take. presort overrides partition_spec.presort

        :param n: number of rows to return
        :param presort: presort expression similar to partition presort
        :param na_position: position of null values during the presort.
            can accept ``first`` or ``last``

        :return: n rows of DataFrame per partition
        """
        params: Dict[str, Any] = dict()
        params["n"] = n
        # Note float is converted to int with triad _get_or
        assert_or_throw(
            isinstance(n, int),
            ValueError("n needs to be an integer"),
        )
        assert_or_throw(
            na_position in ("first", "last"),
            ValueError("na_position must be either 'first' or 'last'"),
        )
        params["na_position"] = na_position
        if presort is not None:
            params["presort"] = presort

        df = self.workflow.process(
            self, using=Take, pre_partition=self.partition_spec, params=params
        )
        return self._to_self_type(df)

    def weak_checkpoint(self: TDF, lazy: bool = False, **kwargs: Any) -> TDF:
        """Cache the dataframe in memory

        :param lazy: whether it is a lazy checkpoint, defaults to False (eager)
        :param kwargs: paramteters for the underlying execution engine function
        :return: the cached dataframe

        :Notice:

        Weak checkpoint in most cases is the best choice for caching a dataframe to
        avoid duplicated computation. However it does not guarantee to break up the
        the compute dependency for this dataframe, so when you have very complicated
        compute, you may encounter issues such as stack overflow. Also, weak checkpoint
        normally caches the dataframe in memory, if memory is a concern, then you should
        consider :meth:`~.strong_checkpoint`
        """
        self._task.set_checkpoint(WeakCheckpoint(lazy=lazy, **kwargs))
        return self

    def strong_checkpoint(
        self: TDF,
        lazy: bool = False,
        partition: Any = None,
        single: bool = False,
        **kwargs: Any,
    ) -> TDF:
        """Cache the dataframe as a temporary file

        :param lazy: whether it is a lazy checkpoint, defaults to False (eager)
        :param partition: |PartitionLikeObject|, defaults to None.
        :param single: force the output as a single file, defaults to False
        :param kwargs: paramteters for the underlying execution engine function
        :return: the cached dataframe

        :Notice:

        Strong checkpoint guarantees the output dataframe compute dependency is
        from the temporary file. Use strong checkpoint only when
        :meth:`~.weak_checkpoint` can't be used.

        Strong checkpoint file will be removed after the execution of the workflow.
        """
        self._task.set_checkpoint(
            FileCheckpoint(
                file_id=str(uuid4()),
                deterministic=False,
                permanent=False,
                lazy=lazy,
                partition=partition,
                single=single,
                **kwargs,
            )
        )
        return self

    def deterministic_checkpoint(
        self: TDF,
        lazy: bool = False,
        partition: Any = None,
        single: bool = False,
        namespace: Any = None,
        **kwargs: Any,
    ) -> TDF:
        """Cache the dataframe as a temporary file

        :param lazy: whether it is a lazy checkpoint, defaults to False (eager)
        :param partition: |PartitionLikeObject|, defaults to None.
        :param single: force the output as a single file, defaults to False
        :param kwargs: paramteters for the underlying execution engine function
        :param namespace: a value to control determinism, defaults to None.
        :return: the cached dataframe

        :Notice:

        The difference vs :meth:`~.strong_checkpoint` is that this checkpoint is not
        removed after execution, so it can take effect cross execution if the dependent
        compute logic is not changed.
        """
        self._task.set_checkpoint(
            FileCheckpoint(
                file_id=self._task.__uuid__(),
                deterministic=True,
                permanent=True,
                lazy=lazy,
                partition=partition,
                single=single,
                namespace=namespace,
                **kwargs,
            )
        )
        return self

    def yield_as(self: TDF, name: str) -> None:
        """Cache the dataframe in memory

        :param name: the name of the yielded dataframe

        :Notice:

        In only the following cases you can yield:

        * you have not checkpointed (persisted) the dataframe, for example
          ``df.yield_as("a")``
        * you have used :meth:`~.deterministic_checkpoint`, for example
          ``df.deterministic_checkpoint().yield_as("a")``
        * yield is workflow, compile level logic

        For the first case, the yield will also be a strong checkpoint so
        whenever you yield a dataframe, the dataframe has been saved as a file
        and loaded back as a new dataframe.
        """
        if not self._task.has_checkpoint:
            # the following == a non determinitic, but permanent checkpoint
            self.deterministic_checkpoint(namespace=str(uuid4()))
        self.workflow._yields[name] = self._task.yielded

    def output_as(self: TDF, name: str) -> TDF:
        """Register the dataframe as an output

        :param name: the name of the dataframe

        :Notice:

        output_as is runtime level operation, it is different from :meth:`~.yield_as`

        :Examples:

        .. code-block:: python

            dag = FugueWorkflow()
            dag.df([[0]],"a:int").transform(a_transformer).output_as("k")
            result = dag.run()
            result.native_dfs["k"]  # a pd.DataFrame of [[0]]
        """
        self.workflow._output[name] = self
        return self

    def persist(self: TDF) -> TDF:
        """Persist the current dataframe

        :return: the persisted dataframe
        :rtype: :class:`~.WorkflowDataFrame`

        :Notice:

        ``persist`` can only guarantee the persisted dataframe will be computed
        for only once. However this doesn't mean the backend really breaks up the
        execution dependency at the persisting point. Commonly, it doesn't cause
        any issue, but if your execution graph is long, it may cause expected
        problems for example, stack overflow.

        ``persist`` method is considered as weak checkpoint. Sometimes, it may be
        necessary to use strong checkpint, which is :meth:`~.checkpoint`
        """
        return self.weak_checkpoint(lazy=False)

    def checkpoint(self: TDF) -> TDF:
        return self.strong_checkpoint(lazy=False)

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

    def alter_columns(self: TDF, columns: Any) -> TDF:
        """Change column types

        :param columns: |SchemaLikeObject|
        :return: a new dataframe with the new column types
        :rtype: :class:`~.WorkflowDataFrame`

        :Notice:

        The output dataframe will not change the order of original schema.

        :Examples:

        >>> df.alter_columns("a:int,b;str")
        """
        df = self.workflow.process(
            self, using=AlterColumns, params=dict(columns=columns)
        )
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
            partition = self.partition_spec
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
            partition = self.partition_spec
        self.workflow.output(
            self,
            using=Save,
            pre_partition=partition,
            params=dict(path=path, fmt=fmt, mode=mode, single=single, params=kwargs),
        )

    def save_and_use(
        self: TDF,
        path: str,
        fmt: str = "",
        mode: str = "overwrite",
        partition: Any = None,
        single: bool = False,
        **kwargs: Any,
    ) -> TDF:
        """Save this dataframe to a persistent storage and load back to use
        in the following steps

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
            partition = self.partition_spec
        df = self.workflow.process(
            self,
            using=SaveAndUse,
            pre_partition=partition,
            params=dict(path=path, fmt=fmt, mode=mode, single=single, params=kwargs),
        )
        return self._to_self_type(df)

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


class WorkflowDataFrames(DataFrames):
    """Ordered dictionary of WorkflowDataFrames. There are two modes: with keys
    and without keys. If without key ``_<n>`` will be used as the key
    for each dataframe, and it will be treated as an array in Fugue framework.

    It's immutable, once initialized, you can't add or remove element from it.

    It's a subclass of
    :class:`~fugue.dataframe.dataframes.DataFrames`, but different from
    DataFrames, in the initialization you should always use
    :class:`~fugue.workflow.workflow.WorkflowDataFrame`, and they should all
    come from the same :class:`~fugue.workflow.workflow.FugueWorkflow`.

    :Examples:

        .. code-block:: python

            dag = FugueWorkflow()
            df1 = dag.df([[0]],"a:int").transform(a_transformer)
            df2 = dag.df([[0]],"b:int")
            dfs1 = WorkflowDataFrames(df1, df2)  # as array
            dfs2 = WorkflowDataFrames([df1, df2])  # as array
            dfs3 = WorkflowDataFrames(a=df1, b=df2)  # as dict
            dfs4 = WorkflowDataFrames(dict(a=df1, b=df2))  # as dict
            dfs5 = WorkflowDataFrames(dfs4, c=df2)  # copy and update
            dfs5["b"].show()  # how you get element when it's a dict
            dfs1[0].show()  # how you get element when it's an array
    """

    def __init__(self, *args: Any, **kwargs: Any):
        self._parent: Optional["FugueWorkflow"] = None
        super().__init__(*args, **kwargs)

    def __setitem__(  # type: ignore
        self, key: str, value: WorkflowDataFrame, *args: Any, **kwds: Any
    ) -> None:
        assert_or_throw(
            isinstance(value, WorkflowDataFrame),
            ValueError(f"{key}:{value} is not WorkflowDataFrame)"),
        )
        if self._parent is None:
            self._parent = value.workflow
        else:
            assert_or_throw(
                self._parent is value.workflow,
                ValueError("different parent workflow detected in dataframes"),
            )
        super().__setitem__(key, value, *args, **kwds)

    def __getitem__(self, key: Union[str, int]) -> WorkflowDataFrame:  # type: ignore
        return super().__getitem__(key)  # type: ignore


class WorkflowResult(DataFrames):
    """Workflow execution result"""

    def __init__(self):
        super().__init__()
        self._readonly = False

    def __setitem__(  # type: ignore
        self, key: str, value: DataFrame, *args: Any, **kwds: Any
    ) -> None:
        assert_or_throw(isinstance(value, WorkflowDataFrame), f"{value}")
        super().__setitem__(key, value, *args, **kwds)

    def __getitem__(self, key: Union[str, int]) -> DataFrame:  # type: ignore
        return super().__getitem__(key).result  # type: ignore


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
        self._output = WorkflowResult()
        self._computed = False
        self._graph = _Graph()
        self._yields: Dict[str, Yielded] = {}

    @property
    def conf(self) -> ParamDict:
        """All configs of this workflow and underlying
        :class:`~fugue.execution.execution_engine.ExecutionEngine` (if given)
        """
        return self._workflow_ctx.conf

    def spec_uuid(self) -> str:
        """UUID of the workflow spec (`description`)"""
        return self._spec.__uuid__()

    def run(self, *args: Any, **kwargs: Any) -> WorkflowResult:
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
        return self._output

    @property
    def yields(self) -> Dict[str, Yielded]:
        return self._yields

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

        :param using: creator-like object, can't be a string expression
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
        assert_or_throw(
            not isinstance(using, str),
            f"creator {using} can't be string expression",
        )
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
        :param using: processor-like object, can't be a string expression
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
        assert_or_throw(
            not isinstance(using, str),
            f"processor {using} can't be string expression",
        )
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

        :param using: outputter-like object, can't be a string expression
        :param params: |ParamsLikeObject| to run the outputter, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`
        """
        assert_or_throw(
            not isinstance(using, str),
            f"outputter {using} can't be string expression",
        )
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
        self,
        data: Any,
        schema: Any = None,
        metadata: Any = None,
        data_determiner: Optional[Callable[[Any], str]] = None,
    ) -> WorkflowDataFrame:
        """Create dataframe.

        :param data: |DataFrameLikeObject| or :class:`~fugue.workflow.yielded.Yielded`
        :param schema: |SchemaLikeObject|, defaults to None
        :param metadata: |ParamsLikeObject|, defaults to None
        :param data_determiner: a function to compute unique id from ``data``
        :return: a dataframe of the current workflow

        :Notice:

        By default, the input ``data`` does not affect the determinism of the workflow
        (but ``schema`` and ``etadata`` do), because the amount of compute can be
        unpredictable. But if you want ``data`` to affect the
        determinism of the workflow, you can provide the function to compute the unique
        id of ``data`` using ``data_determiner``
        """
        if isinstance(data, WorkflowDataFrame):
            assert_or_throw(
                data.workflow is self,
                FugueWorkflowCompileError(f"{data} does not belong to this workflow"),
            )
            assert_or_throw(
                schema is None and metadata is None,
                FugueWorkflowCompileError(
                    "schema and metadata must be None when data is WorkflowDataFrame"
                ),
            )
            return data
        if isinstance(data, Yielded):
            assert_or_throw(
                schema is None and metadata is None,
                FugueWorkflowCompileError(
                    "schema and metadata must be None when data is Yielded"
                ),
            )
            return self.create(using=LoadYielded, params=dict(yielded=data))
        task = CreateData(
            data=data, schema=schema, metadata=metadata, data_determiner=data_determiner
        )
        return self.add(task)

    def df(
        self,
        data: Any,
        schema: Any = None,
        metadata: Any = None,
        data_determiner: Optional[Callable[[Any], str]] = None,
    ) -> WorkflowDataFrame:
        """Create dataframe. Alias of :meth:`~.create_data`

        :param data: |DataFrameLikeObject| or :class:`~fugue.workflow.yielded.Yielded`
        :param schema: |SchemaLikeObject|, defaults to None
        :param metadata: |ParamsLikeObject|, defaults to None
        :param data_determiner: a function to compute unique id from ``data``
        :return: a dataframe of the current workflow

        :Notice:

        By default, the input ``data`` does not affect the determinism of the workflow
        (but ``schema`` and ``etadata`` do), because the amount of compute can be
        unpredictable. But if you want ``data`` to affect the
        determinism of the workflow, you can provide the function to compute the unique
        id of ``data`` using ``data_determiner``
        """
        return self.create_data(
            data=data, schema=schema, metadata=metadata, data_determiner=data_determiner
        )

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
        callback: Any = None,
    ) -> WorkflowDataFrame:
        """Transform dataframes using transformer.

        Please read the
        :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`

        :param dfs: |DataFramesLikeObject|
        :param using: transformer-like object, can't be a string expression
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
        :param callback: |RPCHandlerLikeObject|, defaults to None
        :return: the transformed dataframe

        :Notice:

        :meth:`~.transform` can be lazy and will return the transformed dataframe,
        :meth:`~.out_transform` is guaranteed to execute immediately (eager) and
        return nothing
        """
        assert_or_throw(
            not isinstance(using, str),
            f"transformer {using} can't be string expression",
        )
        assert_or_throw(
            len(dfs) == 1,
            NotImplementedError("transform supports only single dataframe"),
        )
        tf = _to_transformer(using, schema)
        tf._partition_spec = PartitionSpec(pre_partition)  # type: ignore
        callback = to_rpc_handler(callback)
        tf._has_rpc_client = not isinstance(callback, EmptyRPCHandler)  # type: ignore
        tf.validate_on_compile()
        return self.process(
            *dfs,
            using=RunTransformer,
            schema=None,
            params=dict(
                transformer=tf,
                ignore_errors=ignore_errors,
                params=params,
                rpc_handler=callback,
            ),
            pre_partition=pre_partition,
        )

    def out_transform(
        self,
        *dfs: Any,
        using: Any,
        params: Any = None,
        pre_partition: Any = None,
        ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
        callback: Any = None,
    ) -> None:
        """Transform dataframes using transformer, it materializes the execution
        immediately and returns nothing

        Please read the
        :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`

        :param dfs: |DataFramesLikeObject|
        :param using: transformer-like object, can't be a string expression
        :param schema: |SchemaLikeObject|, defaults to None. The transformer
          will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.output_schema`
        :param params: |ParamsLikeObject| to run the processor, defaults to None.
          The transformer will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None. It's
          recommended to use the equivalent wayt, which is to call
          :meth:`~.partition` and then call :meth:`~.out_transform` without this
          parameter
        :param ignore_errors: list of exception types the transformer can ignore,
          defaults to empty list
        :param callback: |RPCHandlerLikeObject|, defaults to None

        :Notice:

        :meth:`~.transform` can be lazy and will return the transformed dataframe,
        :meth:`~.out_transform` is guaranteed to execute immediately (eager) and
        return nothing
        """
        assert_or_throw(
            not isinstance(using, str),
            f"output transformer {using} can't be string expression",
        )
        assert_or_throw(
            len(dfs) == 1,
            NotImplementedError("output transform supports only single dataframe"),
        )
        tf = _to_output_transformer(using)
        tf._partition_spec = PartitionSpec(pre_partition)  # type: ignore
        callback = to_rpc_handler(callback)
        tf._has_rpc_client = not isinstance(callback, EmptyRPCHandler)  # type: ignore
        tf.validate_on_compile()
        self.output(
            *dfs,
            using=RunOutputTransformer,
            params=dict(
                transformer=tf,
                ignore_errors=ignore_errors,
                params=params,
                rpc_handler=callback,
            ),
            pre_partition=pre_partition,
        )

    def select(
        self,
        *statements: Any,
        sql_engine: Optional[Type[SQLEngine]] = None,
        sql_engine_params: Any = None,
    ) -> WorkflowDataFrame:
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
            dfs,
            using=RunSQLSelect,
            params=dict(
                statement=sql,
                sql_engine=sql_engine,
                sql_engine_params=ParamDict(sql_engine_params),
            ),
        )

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        """Compare if these dataframes are equal. It's for internal, unit test
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

    def assert_not_eq(self, *dfs: Any, **params: Any) -> None:
        """Assert if all dataframes are not equal to the first dataframe.
        It's for internal, unit test purpose only. It will convert both dataframes to
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

        :raises AssertionError: if any dataframe equals to the first dataframe
        """
        self.output(*dfs, using=AssertNotEqual, params=params)

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
            if len(self._graph.down[v]) > 1 and self.conf.get_or_throw(
                FUGUE_CONF_WORKFLOW_AUTO_PERSIST, bool
            ):
                self._spec.tasks[v].set_checkpoint(
                    WeakCheckpoint(
                        lazy=False,
                        level=self.conf.get_or_none(
                            FUGUE_CONF_WORKFLOW_AUTO_PERSIST_VALUE, object
                        ),
                    )
                )
        return WorkflowDataFrame(self, wt)

    def _to_dfs(self, *args: Any, **kwargs: Any) -> DataFrames:
        return DataFrames(*args, **kwargs).convert(self.create_data)

    def _to_ctx(self, *args: Any, **kwargs) -> FugueWorkflowContext:
        if len(args) == 1 and isinstance(args[0], FugueWorkflowContext):
            return args[0]
        return FugueWorkflowContext(make_execution_engine(*args, **kwargs))


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
