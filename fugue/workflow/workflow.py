import sys
from collections import defaultdict
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)
from uuid import uuid4

from adagio.specs import WorkflowSpec
from triad import (
    ParamDict,
    Schema,
    SerializableRLock,
    assert_or_throw,
    extensible_class,
)

from fugue._utils.exception import modify_traceback
from fugue.collections.partition import PartitionSpec
from fugue.collections.sql import StructuredRawSQL
from fugue.collections.yielded import Yielded
from fugue.column import ColumnExpr
from fugue.column import SelectColumns as ColumnsSelect
from fugue.column import all_cols, col, lit
from fugue.constants import (
    _FUGUE_GLOBAL_CONF,
    FUGUE_CONF_WORKFLOW_AUTO_PERSIST,
    FUGUE_CONF_WORKFLOW_AUTO_PERSIST_VALUE,
    FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE,
    FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT,
    FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE,
    FUGUE_SQL_DEFAULT_DIALECT,
)
from fugue.dataframe import DataFrame, LocalBoundedDataFrame, YieldedDataFrame
from fugue.dataframe.api import is_df
from fugue.dataframe.dataframes import DataFrames
from fugue.exceptions import FugueWorkflowCompileError, FugueWorkflowError
from fugue.execution.api import engine_context
from fugue.extensions._builtins import (
    Aggregate,
    AlterColumns,
    AssertEqual,
    AssertNotEqual,
    Assign,
    CreateData,
    Distinct,
    DropColumns,
    Dropna,
    Fillna,
    Filter,
    Load,
    Rename,
    RunJoin,
    RunOutputTransformer,
    RunSetOperation,
    RunSQLSelect,
    RunTransformer,
    Sample,
    Save,
    SaveAndUse,
    Select,
    SelectColumns,
    Show,
    Take,
    Zip,
)
from fugue.extensions.transformer.convert import _to_output_transformer, _to_transformer
from fugue.rpc import to_rpc_handler
from fugue.rpc.base import EmptyRPCHandler
from fugue.workflow._checkpoint import StrongCheckpoint, WeakCheckpoint
from fugue.workflow._tasks import Create, FugueTask, Output, Process
from fugue.workflow._workflow_context import FugueWorkflowContext

_DEFAULT_IGNORE_ERRORS: List[Any] = []

TDF = TypeVar("TDF", bound="WorkflowDataFrame")


@extensible_class
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
    """

    def __init__(self, workflow: "FugueWorkflow", task: FugueTask):
        super().__init__("_0:int")
        self._workflow = workflow
        self._task = task

    def spec_uuid(self) -> str:
        """UUID of its task spec"""
        return self._task.__uuid__()

    @property
    def native(self) -> Any:  # pragma: no cover
        raise NotImplementedError

    def native_as_df(self) -> Any:  # pragma: no cover
        raise NotImplementedError

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

        .. admonition:: Examples

            .. code-block:: python

                dag = FugueWorkflow()
                df = dag.df([[0],[1]], "a:int")
                assert df.partition_spec.empty
                df2 = df.partition(by=["a"])
                assert df.partition_spec.empty
                assert df2.partition_spec == PartitionSpec(by=["a"])
        """
        return self.metadata.get("pre_partition", PartitionSpec())

    def compute(self, *args, **kwargs) -> DataFrame:
        """Trigger the parent workflow to
        :meth:`~fugue.workflow.workflow.FugueWorkflow.run` and to generate and cache
        the result dataframe this instance represent.

        .. admonition:: Examples

            >>> df = FugueWorkflow().df([[0]],"a:int").transform(a_transformer)
            >>> df.compute().as_pandas()  # pandas dataframe
            >>> df.compute(SparkExecutionEngine).native  # spark dataframe

        .. note::

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

        Please read the
        :doc:`Processor Tutorial <tutorial:tutorials/extensions/processor>`

        :param using: processor-like object, if it is a string, then it must be
          the alias of a registered processor
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
            pre_partition = self.partition_spec
        df = self.workflow.process(
            self, using=using, schema=schema, params=params, pre_partition=pre_partition
        )
        return self._to_self_type(df)

    def output(self, using: Any, params: Any = None, pre_partition: Any = None) -> None:
        """Run a outputter on this dataframe. It's a simple wrapper of
        :meth:`fugue.workflow.workflow.FugueWorkflow.output`

        Please read the
        :doc:`Outputter Tutorial <tutorial:tutorials/extensions/outputter>`

        :param using: outputter-like object, if it is a string, then it must be
          the alias of a registered outputter
        :param params: |ParamsLikeObject| to run the outputter, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`
        """
        if pre_partition is None:
            pre_partition = self.partition_spec
        self.workflow.output(
            self, using=using, params=params, pre_partition=pre_partition
        )

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:  # pragma: no cover
        raise NotImplementedError

    def show(
        self,
        n: int = 10,
        with_count: bool = False,
        title: Optional[str] = None,
        best_width: int = 100,
    ) -> None:
        """Show the dataframe.
        See
        :ref:`examples <tutorial:tutorials/advanced/dag:initialize a workflow>`.

        :param n: max number of rows, defaults to 10
        :param with_count: whether to show total count, defaults to False
        :param title: title to display on top of the dataframe, defaults to None
        :param best_width: max width for the output table, defaults to 100

        .. note::

            * When you call this method, it means you want the dataframe to be
              printed when the workflow executes. So the dataframe won't show until
              you run the workflow.
            * When ``with_count`` is True, it can trigger expensive calculation for
              a distributed dataframe. So if you call this function directly, you may
              need to :meth:`~.persist` the dataframe. Or you can turn on
              :ref:`tutorial:tutorials/advanced/useful_config:auto persist`
        """
        # TODO: best_width is not used
        self.workflow.show(self, n=n, with_count=with_count, title=title)

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        """Wrapper of :meth:`fugue.workflow.workflow.FugueWorkflow.assert_eq` to
        compare this dataframe with other dataframes.

        :param dfs: |DataFramesLikeObject|
        :param digits: precision on float number comparison, defaults to 8
        :param check_order: if to compare the row orders, defaults to False
        :param check_schema: if compare schemas, defaults to True
        :param check_content: if to compare the row values, defaults to True
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
        :param no_pandas: if true, it will compare the string representations of the
          dataframes, otherwise, it will convert both to pandas dataframe to compare,
          defaults to False

        :raises AssertionError: if any dataframe is equal to the first dataframe
        """
        self.workflow.assert_not_eq(self, *dfs, **params)

    def select(
        self: TDF,
        *columns: Union[str, ColumnExpr],
        where: Optional[ColumnExpr] = None,
        having: Optional[ColumnExpr] = None,
        distinct: bool = False,
    ) -> TDF:
        """The functional interface for SQL select statement

        :param columns: column expressions, for strings they will represent
          the column names
        :param where: ``WHERE`` condition expression, defaults to None
        :param having: ``having`` condition expression, defaults to None. It
          is used when ``cols`` contains aggregation columns, defaults to None
        :param distinct: whether to return distinct result, defaults to False
        :return: the select result as a new dataframe

        .. admonition:: New Since
            :class: hint

            **0.6.0**

        .. attention::

            This interface is experimental, it's subjected to change in new versions.

        .. seealso::

            Please find more expression examples in :mod:`fugue.column.sql` and
            :mod:`fugue.column.functions`

        .. admonition:: Examples

            .. code-block:: python

                import fugue.column.functions as f
                from fugue import FugueWorkflow

                dag = FugueWorkflow()
                df = dag.df(pandas_df)

                # select existed and new columns
                df.select("a","b",lit(1,"another")))
                df.select("a",(col("b")+lit(1)).alias("x"))

                # select distinct
                df.select("a","b",lit(1,"another")),distinct=True)

                # aggregation
                # SELECT COUNT(DISTINCT *) AS x FROM df
                df.select(f.count_distinct(all_cols()).alias("x"))

                # SELECT a, MAX(b+1) AS x FROM df GROUP BY a
                df.select("a",f.max(col("b")+lit(1)).alias("x"))

                # SELECT a, MAX(b+1) AS x FROM df
                #   WHERE b<2 AND a>1
                #   GROUP BY a
                #   HAVING MAX(b+1)>0
                df.select(
                    "a",f.max(col("b")+lit(1)).alias("x"),
                    where=(col("b")<2) & (col("a")>1),
                    having=f.max(col("b")+lit(1))>0
                )
        """

        def _to_col(s: str) -> ColumnExpr:
            return col(s) if s != "*" else all_cols()

        sc = ColumnsSelect(
            *[_to_col(x) if isinstance(x, str) else x for x in columns],
            arg_distinct=distinct,
        )
        df = self.workflow.process(
            self, using=Select, params=dict(columns=sc, where=where, having=having)
        )
        return self._to_self_type(df)

    def filter(self: TDF, condition: ColumnExpr) -> TDF:
        """Filter rows by the given condition

        :param df: the dataframe to be filtered
        :param condition: (boolean) column expression
        :return: a new filtered dataframe

        .. admonition:: New Since
            :class: hint

            **0.6.0**

        .. seealso::

            Please find more expression examples in :mod:`fugue.column.sql` and
            :mod:`fugue.column.functions`

        .. admonition:: Examples

            .. code-block:: python

                import fugue.column.functions as f
                from fugue import FugueWorkflow

                dag = FugueWorkflow()
                df = dag.df(pandas_df)

                df.filter((col("a")>1) & (col("b")=="x"))
                df.filter(f.coalesce(col("a"),col("b"))>1)
        """
        df = self.workflow.process(self, using=Filter, params=dict(condition=condition))
        return self._to_self_type(df)

    def assign(self: TDF, *args: ColumnExpr, **kwargs: Any) -> TDF:
        """Update existing columns with new values and add new columns

        :param df: the dataframe to set columns
        :param args: column expressions
        :param kwargs: column expressions to be renamed to the argument names,
          if a value is not `ColumnExpr`, it will be treated as a literal
        :return: a new dataframe with the updated values

        .. tip::

            This can be used to cast data types, alter column values or add new
            columns. But you can't use aggregation in columns.

        .. admonition:: New Since
            :class: hint

            **0.6.0**

        .. seealso::

            Please find more expression examples in :mod:`fugue.column.sql` and
            :mod:`fugue.column.functions`

        .. admonition:: Examples

            .. code-block:: python

                from fugue import FugueWorkflow

                dag = FugueWorkflow()
                df = dag.df(pandas_df)

                # add/set 1 as column x
                df.assign(lit(1,"x"))
                df.assign(x=1)

                # add/set x to be a+b
                df.assign((col("a")+col("b")).alias("x"))
                df.assign(x=col("a")+col("b"))

                # cast column a data type to double
                df.assign(col("a").cast(float))

                # cast + new columns
                df.assign(col("a").cast(float),x=1,y=col("a")+col("b"))
        """
        kv: List[ColumnExpr] = [
            v.alias(k) if isinstance(v, ColumnExpr) else lit(v).alias(k)
            for k, v in kwargs.items()
        ]
        df = self.workflow.process(
            self, using=Assign, params=dict(columns=list(args) + kv)
        )
        return self._to_self_type(df)

    def aggregate(self: TDF, *agg_cols: ColumnExpr, **kwagg_cols: ColumnExpr) -> TDF:
        """Aggregate on dataframe

        :param df: the dataframe to aggregate on
        :param agg_cols: aggregation expressions
        :param kwagg_cols: aggregation expressions to be renamed to the argument names
        :return: the aggregated result as a dataframe

        .. admonition:: New Since
            :class: hint

            **0.6.0**

        .. seealso::

            Please find more expression examples in :mod:`fugue.column.sql` and
            :mod:`fugue.column.functions`

        .. admonition:: Examples

            .. code-block:: python

                import fugue.column.functions as f

                # SELECT MAX(b) AS b FROM df
                df.aggregate(f.max(col("b")))

                # SELECT a, MAX(b) AS x FROM df GROUP BY a
                df.partition_by("a").aggregate(f.max(col("b")).alias("x"))
                df.partition_by("a").aggregate(x=f.max(col("b")))
        """
        columns: List[ColumnExpr] = list(agg_cols) + [
            v.alias(k) for k, v in kwagg_cols.items()
        ]
        df = self.workflow.process(
            self,
            using=Aggregate,
            params=dict(columns=columns),
            pre_partition=self.partition_spec,
        )
        return self._to_self_type(df)

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

        Please read |TransformerTutorial|

        :param using: transformer-like object, if it is a string, then it must be
          the alias of a registered transformer/cotransformer
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

        .. note::

            :meth:`~.transform` can be lazy and will return the transformed dataframe,
            :meth:`~.out_transform` is guaranteed to execute immediately (eager) and
            return nothing
        """
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

        Please read |TransformerTutorial|

        :param using: transformer-like object, if it is a string, then it must be
          the alias of a registered output transformer/cotransformer
        :param params: |ParamsLikeObject| to run the processor, defaults to None.
          The transformer will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None. It's
          recommended to use the equivalent wayt, which is to call
          :meth:`~.partition` and then call :meth:`~.transform` without this parameter
        :param ignore_errors: list of exception types the transformer can ignore,
          defaults to empty list
        :param callback: |RPCHandlerLikeObject|, defaults to None

        .. note::

            :meth:`~.transform` can be lazy and will return the transformed dataframe,
            :meth:`~.out_transform` is guaranteed to execute immediately (eager) and
            return nothing
        """
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

        .. note::

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

        .. note::

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

        .. note::

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
        params: Dict[str, Any] = dict(replace=replace)
        if seed is not None:
            params["seed"] = seed
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

        .. note::

            Weak checkpoint in most cases is the best choice for caching a dataframe to
            avoid duplicated computation. However it does not guarantee to break up the
            the compute dependency for this dataframe, so when you have very complicated
            compute, you may encounter issues such as stack overflow. Also, weak
            checkpoint normally caches the dataframe in memory, if memory is a concern,
            then you should consider :meth:`~.strong_checkpoint`
        """
        self._task.set_checkpoint(WeakCheckpoint(lazy=lazy, **kwargs))
        return self

    def strong_checkpoint(
        self: TDF,
        storage_type: str = "file",
        lazy: bool = False,
        partition: Any = None,
        single: bool = False,
        **kwargs: Any,
    ) -> TDF:
        """Cache the dataframe as a temporary file

        :param storage_type: can be either ``file`` or ``table``, defaults to ``file``
        :param lazy: whether it is a lazy checkpoint, defaults to False (eager)
        :param partition: |PartitionLikeObject|, defaults to None.
        :param single: force the output as a single file, defaults to False
        :param kwargs: paramteters for the underlying execution engine function
        :return: the cached dataframe

        .. note::

            Strong checkpoint guarantees the output dataframe compute dependency is
            from the temporary file. Use strong checkpoint only when
            :meth:`~.weak_checkpoint` can't be used.

            Strong checkpoint file will be removed after the execution of the workflow.
        """
        self._task.set_checkpoint(
            StrongCheckpoint(
                storage_type=storage_type,
                obj_id=str(uuid4()),
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
        storage_type: str = "file",
        lazy: bool = False,
        partition: Any = None,
        single: bool = False,
        namespace: Any = None,
        **kwargs: Any,
    ) -> TDF:
        """Cache the dataframe as a temporary file

        :param storage_type: can be either ``file`` or ``table``, defaults to ``file``
        :param lazy: whether it is a lazy checkpoint, defaults to False (eager)
        :param partition: |PartitionLikeObject|, defaults to None.
        :param single: force the output as a single file, defaults to False
        :param kwargs: paramteters for the underlying execution engine function
        :param namespace: a value to control determinism, defaults to None.
        :return: the cached dataframe

        .. note::

            The difference vs :meth:`~.strong_checkpoint` is that this checkpoint is not
            removed after execution, so it can take effect cross execution if the
            dependent compute logic is not changed.
        """
        self._task.set_checkpoint(
            StrongCheckpoint(
                storage_type=storage_type,
                obj_id=self._task.__uuid__(),
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

    def yield_file_as(self: TDF, name: str) -> None:
        """Cache the dataframe in file

        :param name: the name of the yielded dataframe

        .. note::

            In only the following cases you can yield file/table:

            * you have not checkpointed (persisted) the dataframe, for example
              ``df.yield_file_as("a")``
            * you have used :meth:`~.deterministic_checkpoint`, for example
              ``df.deterministic_checkpoint().yield_file_as("a")``
            * yield is workflow, compile level logic

            For the first case, the yield will also be a strong checkpoint so
            whenever you yield a dataframe as a file, the dataframe has been saved as a
            file and loaded back as a new dataframe.
        """
        if not self._task.has_checkpoint:
            # the following == a non determinitic, but permanent checkpoint
            self.deterministic_checkpoint(storage_type="file", namespace=str(uuid4()))
        self.workflow._yields[name] = self._task.yielded

    def yield_table_as(self: TDF, name: str) -> None:
        """Cache the dataframe as a table

        :param name: the name of the yielded dataframe

        .. note::

            In only the following cases you can yield file/table:

            * you have not checkpointed (persisted) the dataframe, for example
              ``df.yield_file_as("a")``
            * you have used :meth:`~.deterministic_checkpoint`, for example
              ``df.deterministic_checkpoint().yield_file_as("a")``
            * yield is workflow, compile level logic

            For the first case, the yield will also be a strong checkpoint so
            whenever you yield a dataframe as a file, the dataframe has been saved as a
            file and loaded back as a new dataframe.
        """
        if not self._task.has_checkpoint:
            # the following == a non determinitic, but permanent checkpoint
            self.deterministic_checkpoint(storage_type="table", namespace=str(uuid4()))
        self.workflow._yields[name] = self._task.yielded

    def yield_dataframe_as(self: TDF, name: str, as_local: bool = False) -> None:
        """Yield a dataframe that can be accessed without
        the current execution engine

        :param name: the name of the yielded dataframe
        :param as_local: yield the local version of the dataframe

        .. note::

            When ``as_local`` is True, it can trigger an additional compute
            to do the conversion. To avoid recompute, you should add
            ``persist`` before yielding.
        """
        yielded = YieldedDataFrame(self._task.__uuid__())
        self.workflow._yields[name] = yielded
        self._task.set_yield_dataframe_handler(
            lambda df: yielded.set_value(df), as_local=as_local
        )

    def persist(self: TDF) -> TDF:
        """Persist the current dataframe

        :return: the persisted dataframe
        :rtype: :class:`~.WorkflowDataFrame`

        .. note::

            ``persist`` can only guarantee the persisted dataframe will be computed
            for only once. However this doesn't mean the backend really breaks up the
            execution dependency at the persisting point. Commonly, it doesn't cause
            any issue, but if your execution graph is long, it may cause expected
            problems for example, stack overflow.

            ``persist`` method is considered as weak checkpoint. Sometimes, it may be
            necessary to use strong checkpint, which is :meth:`~.checkpoint`
        """
        return self.weak_checkpoint(lazy=False)

    def checkpoint(self: TDF, storage_type: str = "file") -> TDF:
        return self.strong_checkpoint(storage_type=storage_type, lazy=False)

    def broadcast(self: TDF) -> TDF:
        """Broadcast the current dataframe

        :return: the broadcasted dataframe
        :rtype: :class:`~.WorkflowDataFrame`
        """
        self._task.broadcast()
        return self

    def partition(self: TDF, *args: Any, **kwargs: Any) -> TDF:
        """Partition the current dataframe. Please read |PartitionTutorial|

        :param args: |PartitionLikeObject|
        :param kwargs: |PartitionLikeObject|
        :return: dataframe with the partition hint
        :rtype: :class:`~.WorkflowDataFrame`

        .. note::

            Normally this step is fast because it's to add a partition hint
            for the next step.
        """
        res = WorkflowDataFrame(self.workflow, self._task)
        res.reset_metadata({"pre_partition": PartitionSpec(*args, **kwargs)})
        return self._to_self_type(res)

    def partition_by(self: TDF, *keys: str, **kwargs: Any) -> TDF:
        """Partition the current dataframe by keys. Please read |PartitionTutorial|.
        This is a wrapper of :meth:`~.partition`

        :param keys: partition keys
        :param kwargs: |PartitionLikeObject| excluding ``by`` and ``partition_by``
        :return: dataframe with the partition hint
        :rtype: :class:`~.WorkflowDataFrame`
        """
        assert_or_throw(len(keys) > 0, FugueWorkflowCompileError("keys can't be empty"))
        assert_or_throw(
            "by" not in kwargs and "partition_by" not in kwargs,
            FugueWorkflowCompileError("by and partition_by can't be in kwargs"),
        )
        return self.partition(by=keys, **kwargs)

    def per_partition_by(self: TDF, *keys: str) -> TDF:
        """Partition the current dataframe by keys so each physical partition contains
        only one logical partition. Please read |PartitionTutorial|.
        This is a wrapper of :meth:`~.partition`

        :param keys: partition keys
        :return: dataframe that is both logically and physically partitioned by ``keys``
        :rtype: :class:`~.WorkflowDataFrame`

        .. note::

            This is a hint but not enforced, certain execution engines will not
            respect this hint.
        """
        return self.partition_by(*keys, algo="even")

    def per_row(self: TDF) -> TDF:
        """Partition the current dataframe to one row per partition.
        Please read |PartitionTutorial|. This is a wrapper of :meth:`~.partition`

        :return: dataframe that is evenly partitioned by row count
        :rtype: :class:`~.WorkflowDataFrame`

        .. note::

            This is a hint but not enforced, certain execution engines will not
            respect this hint.
        """
        return self.partition("per_row")

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

        .. note::

            This interface is more flexible than
            :meth:`fugue.dataframe.dataframe.DataFrame.rename`

        .. admonition:: Examples

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

        .. note::

            The output dataframe will not change the order of original schema.

        .. admonition:: Examples

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

        .. note::

            * ``dfs`` must be list like, the zipped dataframe will be list like
            * ``dfs`` is fine to be empty
            * If you want dict-like zip, use
              :meth:`fugue.workflow.workflow.FugueWorkflow.zip`

        .. seealso::

            Read |CoTransformer| and |ZipComap| for details
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
        :ref:`Save & Load <tutorial:tutorials/advanced/dag:save & load>`.
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
        :ref:`Save & Load <tutorial:tutorials/advanced/dag:save & load>`.
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

    def as_local_bounded(self) -> DataFrame:  # type: ignore  # pragma: no cover
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

    def peek_array(self) -> List[Any]:  # pragma: no cover
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

    .. admonition:: Examples

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

    @property
    def workflow(self) -> "FugueWorkflow":
        """The parent workflow"""
        assert_or_throw(
            self._parent is not None, ValueError("parent workflow is unknown")
        )
        return self._parent  # type: ignore

    def __setitem__(  # type: ignore
        self, key: str, value: WorkflowDataFrame, *args: Any, **kwds: Any
    ) -> None:
        assert_or_throw(
            isinstance(value, WorkflowDataFrame),
            lambda: ValueError(f"{key}:{value} is not WorkflowDataFrame)"),
        )
        if self._parent is None:
            self._parent = value.workflow
        else:
            assert_or_throw(
                self._parent is value.workflow,
                ValueError("different parent workflow detected in dataframes"),
            )
        super().__setitem__(key, value, *args, **kwds)

    def __getitem__(  # pylint: disable=W0235
        self, key: Union[str, int]  # type: ignore
    ) -> WorkflowDataFrame:
        return super().__getitem__(key)  # type: ignore

    def __getattr__(self, name: str) -> Any:  # pragma: no cover
        """The dummy method to avoid PyLint complaint"""
        raise AttributeError(name)


class FugueWorkflowResult(DataFrames):
    """The result object of :meth:`~.FugueWorkflow.run`. Users should not
    construct this object.

    :param DataFrames: yields of the workflow
    """

    def __init__(self, yields: Dict[str, Yielded]):
        self._yields = yields
        super().__init__(
            {k: v.result for k, v in yields.items() if isinstance(v, YieldedDataFrame)}
        )

    @property
    def yields(self) -> Dict[str, Any]:
        return self._yields


@extensible_class
class FugueWorkflow:
    """Fugue Workflow, also known as the Fugue Programming Interface.

    In Fugue, we use DAG to represent workflows, DAG construction and execution
    are different steps, this class is mainly used in the construction step, so all
    things you added to the workflow is **description** and they are not executed
    until you call :meth:`~.run`

    Read
    :ref:`this <tutorial:tutorials/advanced/dag:initialize a workflow>`
    to learn how to initialize it in different ways and pros and cons.
    """

    def __init__(self, compile_conf: Any = None):
        assert_or_throw(
            compile_conf is None or isinstance(compile_conf, (dict, ParamDict)),
            ValueError(
                f"FugueWorkflow no longer takes {type(compile_conf)} as the input"
            ),
        )

        self._lock = SerializableRLock()
        self._spec = WorkflowSpec()
        self._computed = False
        self._graph = _Graph()
        self._yields: Dict[str, Yielded] = {}
        self._compile_conf = ParamDict(
            {**_FUGUE_GLOBAL_CONF, **ParamDict(compile_conf)}
        )
        self._last_df: Optional[WorkflowDataFrame] = None

    @property
    def conf(self) -> ParamDict:
        """Compile time configs"""
        return self._compile_conf

    def spec_uuid(self) -> str:
        """UUID of the workflow spec (`description`)"""
        return self._spec.__uuid__()

    def run(
        self, engine: Any = None, conf: Any = None, **kwargs: Any
    ) -> FugueWorkflowResult:
        """Execute the workflow and compute all dataframes.

        .. note::

            For inputs, please read
            :func:`~.fugue.api.engine_context`

        :param engine: object that can be recognized as an engine, defaults to None
        :param conf: engine config, defaults to None
        :param kwargs: additional parameters to initialize the execution engine
        :return: the result set

        .. admonition:: Examples

            .. code-block:: python

                dag = FugueWorkflow()
                df1 = dag.df([[0]],"a:int").transform(a_transformer)
                df2 = dag.df([[0]],"b:int")

                dag.run(SparkExecutionEngine)
                df1.result.show()
                df2.result.show()

                dag = FugueWorkflow()
                df1 = dag.df([[0]],"a:int").transform(a_transformer)
                df1.yield_dataframe_as("x")

                result = dag.run(SparkExecutionEngine)
                result["x"]  # SparkDataFrame

        Read
        :ref:`this <tutorial:tutorials/advanced/dag:initialize a workflow>`
        to learn how to run in different ways and pros and cons.
        """
        with self._lock:
            with engine_context(engine, engine_conf=conf) as e:
                self._computed = False
                self._workflow_ctx = FugueWorkflowContext(
                    engine=e, compile_conf=self.conf
                )
                try:
                    self._workflow_ctx.run(self._spec, {})
                except Exception as ex:
                    if not self.conf.get_or_throw(
                        FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE, bool
                    ) or sys.version_info < (3, 7):
                        raise
                    conf = self.conf.get_or_throw(
                        FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE, str
                    )
                    pre = [p for p in conf.split(",") if p != ""]
                    if len(pre) == 0:
                        raise

                    # prune by prefix
                    ctb = modify_traceback(
                        sys.exc_info()[2],
                        lambda x: any(x.lower().startswith(xx) for xx in pre),
                    )
                    if ctb is None:  # pragma: no cover
                        raise
                    raise ex.with_traceback(ctb)
                self._computed = True
        return FugueWorkflowResult(self.yields)

    @property
    def yields(self) -> Dict[str, Yielded]:
        return self._yields

    @property
    def last_df(self) -> Optional[WorkflowDataFrame]:
        return self._last_df

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        return

    def get_result(self, df: WorkflowDataFrame) -> DataFrame:
        """After :meth:`~.run`, get the result of a dataframe defined in the dag

        :return: a calculated dataframe

        .. admonition:: Examples

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

        Please read the
        :doc:`Creator Tutorial <tutorial:tutorials/extensions/creator>`

        :param using: creator-like object, if it is a string, then it must be
          the alias of a registered creator
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
        task = Create(
            creator=CreateData(using)
            if is_df(using) or isinstance(using, Yielded)
            else using,
            schema=schema,
            params=params,
        )
        res = self.add(task)
        self._last_df = res
        return res

    def process(
        self,
        *dfs: Any,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
    ) -> WorkflowDataFrame:
        """Run a processor on the dataframes.

        Please read the
        :doc:`Processor Tutorial <tutorial:tutorials/extensions/processor>`

        :param dfs: |DataFramesLikeObject|
        :param using: processor-like object, if it is a string, then it must be
          the alias of a registered processor
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
        _dfs = self._to_dfs(*dfs)
        task = Process(
            len(_dfs),
            processor=using,
            schema=schema,
            params=params,
            pre_partition=pre_partition,
            input_names=None if not _dfs.has_key else list(_dfs.keys()),
        )
        if _dfs.has_key:
            res = self.add(task, **_dfs)
        else:
            res = self.add(task, *_dfs.values())
        self._last_df = res
        return res

    def output(
        self, *dfs: Any, using: Any, params: Any = None, pre_partition: Any = None
    ) -> None:
        """Run a outputter on dataframes.

        Please read the
        :doc:`Outputter Tutorial <tutorial:tutorials/extensions/outputter>`

        :param using: outputter-like object, if it is a string, then it must be
          the alias of a registered outputter
        :param params: |ParamsLikeObject| to run the outputter, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.params`
        :param pre_partition: |PartitionLikeObject|, defaults to None.
          The outputter will be able to access this value from
          :meth:`~fugue.extensions.context.ExtensionContext.partition_spec`
        """
        _dfs = self._to_dfs(*dfs)
        task = Output(
            len(_dfs),
            outputter=using,
            params=params,
            pre_partition=pre_partition,
            input_names=None if not _dfs.has_key else list(_dfs.keys()),
        )
        if _dfs.has_key:
            self.add(task, **_dfs)
        else:
            self.add(task, *_dfs.values())

    def create_data(
        self,
        data: Any,
        schema: Any = None,
        data_determiner: Optional[Callable[[Any], Any]] = None,
    ) -> WorkflowDataFrame:
        """Create dataframe.

        :param data: |DataFrameLikeObject| or :class:`~fugue.workflow.yielded.Yielded`
        :param schema: |SchemaLikeObject|, defaults to None
        :param data_determiner: a function to compute unique id from ``data``
        :return: a dataframe of the current workflow

        .. note::

            By default, the input ``data`` does not affect the determinism of the
            workflow (but ``schema`` and ``etadata`` do), because the amount of compute
            can be unpredictable. But if you want ``data`` to affect the
            determinism of the workflow, you can provide the function to compute the
            unique id of ``data`` using ``data_determiner``
        """
        if isinstance(data, WorkflowDataFrame):
            assert_or_throw(
                data.workflow is self,
                lambda: FugueWorkflowCompileError(
                    f"{data} does not belong to this workflow"
                ),
            )
            assert_or_throw(
                schema is None,
                FugueWorkflowCompileError(
                    "schema must be None when data is WorkflowDataFrame"
                ),
            )
            self._last_df = data
            return data
        if (
            (isinstance(data, (List, Iterable)) and not isinstance(data, str))
            or isinstance(data, Yielded)
            or is_df(data)
        ):
            return self.create(
                using=CreateData(
                    data,
                    schema=schema,
                    data_determiner=data_determiner,
                )
            )
        raise FugueWorkflowCompileError(
            f"Input data of type {type(data)} can't "
            "be converted to WorkflowDataFrame"
        )

    def df(
        self,
        data: Any,
        schema: Any = None,
        data_determiner: Optional[Callable[[Any], str]] = None,
    ) -> WorkflowDataFrame:
        """Create dataframe. Alias of :meth:`~.create_data`

        :param data: |DataFrameLikeObject| or :class:`~fugue.workflow.yielded.Yielded`
        :param schema: |SchemaLikeObject|, defaults to None
        :param data_determiner: a function to compute unique id from ``data``
        :return: a dataframe of the current workflow

        .. note::

            By default, the input ``data`` does not affect the determinism of the
            workflow (but ``schema`` and ``etadata`` do), because the amount of
            compute can be unpredictable. But if you want ``data`` to affect the
            determinism of the workflow, you can provide the function to compute
            the unique id of ``data`` using ``data_determiner``
        """
        return self.create_data(
            data=data, schema=schema, data_determiner=data_determiner
        )

    def load(
        self, path: str, fmt: str = "", columns: Any = None, **kwargs: Any
    ) -> WorkflowDataFrame:
        """Load dataframe from persistent storage.
        Read :ref:`this <tutorial:tutorials/advanced/dag:save & load>`
        for details.

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
        n: int = 10,
        with_count: bool = False,
        title: Optional[str] = None,
    ) -> None:
        """Show the dataframes.
        See
        :ref:`examples <tutorial:tutorials/advanced/dag:initialize a workflow>`.

        :param dfs: |DataFramesLikeObject|
        :param n: max number of rows, defaults to 10
        :param with_count: whether to show total count, defaults to False
        :param title: title to display on top of the dataframe, defaults to None
        :param best_width: max width for the output table, defaults to 100

        .. note::

            * When you call this method, it means you want the dataframe to be
              printed when the workflow executes. So the dataframe won't show until
              you run the workflow.
            * When ``with_count`` is True, it can trigger expensive calculation for
              a distributed dataframe. So if you call this function directly, you may
              need to :meth:`~.WorkflowDataFrame.persist` the dataframe. Or you can
              turn on |AutoPersist|
        """
        self.output(
            *dfs, using=Show, params=dict(n=n, with_count=with_count, title=title)
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

        .. note::

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

        .. note::

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

        .. note::

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

        .. note::

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

        .. note::

            * If ``dfs`` is dict like, the zipped dataframe will be dict like,
              If ``dfs`` is list like, the zipped dataframe will be list like
            * It's fine to contain only one dataframe in ``dfs``

        .. seealso::

            Read |CoTransformer| and |ZipComap| for details
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

        Please read |TransformerTutorial|

        :param dfs: |DataFramesLikeObject|
        :param using: transformer-like object, if it is a string, then it must be
          the alias of a registered transformer/cotransformer
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

        .. note::

            :meth:`~.transform` can be lazy and will return the transformed dataframe,
            :meth:`~.out_transform` is guaranteed to execute immediately (eager) and
            return nothing
        """
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

        Please read |TransformerTutorial|

        :param dfs: |DataFramesLikeObject|
        :param using: transformer-like object, if it is a string, then it must be
          the alias of a registered output transformer/cotransformer
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

        .. note::

            :meth:`~.transform` can be lazy and will return the transformed dataframe,
            :meth:`~.out_transform` is guaranteed to execute immediately (eager) and
            return nothing
        """
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
        sql_engine: Any = None,
        sql_engine_params: Any = None,
        dialect: Optional[str] = FUGUE_SQL_DEFAULT_DIALECT,
    ) -> WorkflowDataFrame:
        """Execute ``SELECT`` statement using
        :class:`~fugue.execution.execution_engine.SQLEngine`

        :param statements: a list of sub-statements in string
          or :class:`~.WorkflowDataFrame`
        :param sql_engine: it can be empty string or null (use the default SQL
          engine), a string (use the registered SQL engine), an
          :class:`~fugue.execution.execution_engine.SQLEngine` type, or
          the :class:`~fugue.execution.execution_engine.SQLEngine` instance
          (you can use ``None`` to use the default one), defaults to None
        :return: result of the ``SELECT`` statement

        .. admonition:: Examples

            .. code-block:: python

                with FugueWorkflow() as dag:
                    a = dag.df([[0,"a"]],a:int,b:str)
                    b = dag.df([[0]],a:int)
                    c = dag.select("SELECT a FROM",a,"UNION SELECT * FROM",b)
                dag.run()

        Please read :ref:`this <tutorial:tutorials/advanced/dag:select query>`
        for more examples
        """
        sql: List[Tuple[bool, str]] = []
        dfs: Dict[str, DataFrame] = {}
        for s in statements:
            if isinstance(s, str):
                sql.append((False, s))
            else:
                ws = self.df(s)
                dfs[ws.name] = ws
                sql.append((True, ws.name))
        if sql[0][0]:  # starts with reference
            sql.insert(0, (False, "SELECT"))
        else:  # start with string but without select
            start = sql[0][1].strip()
            if not start[:10].upper().startswith("SELECT") and not start[
                :10
            ].upper().startswith("WITH"):
                sql[0] = (False, "SELECT " + start)
        return self.process(
            dfs,
            using=RunSQLSelect,
            params=dict(
                statement=StructuredRawSQL(sql, dialect=dialect),
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
        if self.conf.get_or_throw(
            FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE, bool
        ) and sys.version_info >= (3, 7):
            dep = self.conf.get_or_throw(FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT, int)
            if dep > 0:
                conf = self.conf.get_or_throw(FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE, str)
                pre = [p for p in conf.split(",") if p != ""]
                task.reset_traceback(
                    limit=dep,
                    should_prune=lambda x: any(x.lower().startswith(xx) for xx in pre),
                )
        assert_or_throw(task._node_spec is None, lambda: f"can't reuse {task}")
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

    def __getattr__(self, name: str) -> Any:  # pragma: no cover
        """The dummy method to avoid PyLint complaint"""
        raise AttributeError(name)


class _Dependencies:
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
class _Graph:
    def __init__(self):
        self.down: Dict[str, Set[str]] = defaultdict(set)
        self.up: Dict[str, Set[str]] = defaultdict(set)

    def add(self, name: str, depend_on: str) -> None:
        depend_on = depend_on.split(".")[0]
        self.down[depend_on].add(name)
        self.up[name].add(depend_on)
