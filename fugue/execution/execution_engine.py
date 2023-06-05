import inspect
import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from uuid import uuid4

from triad import ParamDict, Schema, SerializableRLock, assert_or_throw, to_uuid
from triad.collections.fs import FileSystem
from triad.collections.function_wrapper import AnnotatedParam
from triad.exceptions import InvalidOperationError
from triad.utils.convert import to_size
from triad.utils.string import validate_triad_var_name

from fugue.bag import Bag, LocalBag
from fugue.collections.partition import (
    BagPartitionCursor,
    PartitionCursor,
    PartitionSpec,
)
from fugue.collections.sql import StructuredRawSQL, TempTableName
from fugue.collections.yielded import PhysicalYielded, Yielded
from fugue.column import (
    ColumnExpr,
    SelectColumns,
    SQLExpressionGenerator,
    all_cols,
    col,
    is_agg,
)
from fugue.constants import _FUGUE_GLOBAL_CONF, FUGUE_SQL_DEFAULT_DIALECT
from fugue.dataframe import AnyDataFrame, DataFrame, DataFrames, fugue_annotated_param
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import LocalDataFrame
from fugue.dataframe.utils import deserialize_df, serialize_df
from fugue.exceptions import FugueBug, FugueWorkflowRuntimeError

AnyExecutionEngine = TypeVar("AnyExecutionEngine", object, None)

_FUGUE_EXECUTION_ENGINE_CONTEXT = ContextVar(
    "_FUGUE_EXECUTION_ENGINE_CONTEXT", default=None
)

_CONTEXT_LOCK = SerializableRLock()


class _GlobalExecutionEngineContext:
    def __init__(self):
        self._engine: Optional["ExecutionEngine"] = None

    def set(self, engine: Optional["ExecutionEngine"]):
        with _CONTEXT_LOCK:
            if self._engine is not None:
                self._engine._is_global = False
                self._engine._exit_context()
            self._engine = engine
            if engine is not None:
                engine._enter_context()
                engine._is_global = True

    def get(self) -> Optional["ExecutionEngine"]:
        return self._engine


_FUGUE_GLOBAL_EXECUTION_ENGINE_CONTEXT = _GlobalExecutionEngineContext()


class FugueEngineBase(ABC):
    @abstractmethod
    def to_df(
        self, df: AnyDataFrame, schema: Any = None
    ) -> DataFrame:  # pragma: no cover
        """Convert a data structure to this engine compatible DataFrame

        :param data: :class:`~fugue.dataframe.dataframe.DataFrame`,
          pandas DataFramme or list or iterable of arrays or others that
          is supported by certain engine implementation
        :param schema: |SchemaLikeObject|, defaults to None
        :return: engine compatible dataframe

        .. note::

            There are certain conventions to follow for a new implementation:

            * if the input is already in compatible dataframe type, it should return
              itself
            * all other methods in the engine interface should take arbitrary
              dataframes and call this method to convert before doing anything
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def log(self) -> logging.Logger:  # pragma: no cover
        """Logger of this engine instance"""
        raise NotImplementedError

    @property
    @abstractmethod
    def conf(self) -> ParamDict:  # pragma: no cover
        """All configurations of this engine instance.

        .. note::

            It can contain more than you providec, for example
            in :class:`~.fugue_spark.execution_engine.SparkExecutionEngine`,
            the Spark session can bring in more config, they are all accessible
            using this property.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def is_distributed(self) -> bool:  # pragma: no cover
        """Whether this engine is a distributed engine"""
        raise NotImplementedError


class EngineFacet(FugueEngineBase):
    """The base class for different factes of the execution engines.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def __init__(self, execution_engine: "ExecutionEngine") -> None:
        tp = self.execution_engine_constraint
        if not isinstance(execution_engine, tp):
            raise TypeError(
                f"{self} expects the engine type to be "
                f"{tp}, but got {type(execution_engine)}"
            )
        self._execution_engine = execution_engine

    @property
    def execution_engine(self) -> "ExecutionEngine":
        """the execution engine this sql engine will run on"""
        return self._execution_engine

    @property
    def log(self) -> logging.Logger:
        return self.execution_engine.log

    @property
    def conf(self) -> ParamDict:
        return self.execution_engine.conf

    def to_df(self, df: AnyDataFrame, schema: Any = None) -> DataFrame:
        return self.execution_engine.to_df(df, schema)

    @property
    def execution_engine_constraint(self) -> Type["ExecutionEngine"]:
        """This defines the required ExecutionEngine type of this facet

        :return: a subtype of :class:`~.ExecutionEngine`
        """
        return ExecutionEngine


class SQLEngine(EngineFacet):
    """The abstract base class for different SQL execution implementations. Please read
    :ref:`this <tutorial:tutorials/advanced/execution_engine:sqlengine>`
    to understand the concept

    :param execution_engine: the execution engine this sql engine will run on
    """

    def __init__(self, execution_engine: "ExecutionEngine") -> None:
        super().__init__(execution_engine)
        self._uid = "_" + str(uuid4())[:5] + "_"

    @property
    def dialect(self) -> Optional[str]:  # pragma: no cover
        return None

    def encode_name(self, name: str) -> str:
        return self._uid + name

    def encode(
        self, dfs: DataFrames, statement: StructuredRawSQL
    ) -> Tuple[DataFrames, str]:
        d = DataFrames({self.encode_name(k): v for k, v in dfs.items()})
        s = statement.construct(self.encode_name, dialect=self.dialect, log=self.log)
        return d, s

    @abstractmethod
    def select(
        self, dfs: DataFrames, statement: StructuredRawSQL
    ) -> DataFrame:  # pragma: no cover
        """Execute select statement on the sql engine.

        :param dfs: a collection of dataframes that must have keys
        :param statement: the ``SELECT`` statement using the ``dfs`` keys as tables.
        :return: result of the ``SELECT`` statement

        .. admonition:: Examples

            .. code-block:: python

                dfs = DataFrames(a=df1, b=df2)
                sql_engine.select(
                    dfs,
                    [(False, "SELECT * FROM "),
                     (True,"a"),
                     (False," UNION SELECT * FROM "),
                     (True,"b")])

        .. note::

            There can be tables that is not in ``dfs``. For example you want to select
            from hive without input DataFrames:

            >>> sql_engine.select(DataFrames(), "SELECT * FROM hive.a.table")
        """
        raise NotImplementedError

    def table_exists(self, table: str) -> bool:  # pragma: no cover
        """Whether the table exists

        :param table: the table name
        :return: whether the table exists
        """
        raise NotImplementedError

    def load_table(self, table: str, **kwargs: Any) -> DataFrame:  # pragma: no cover
        """Load table as a dataframe

        :param table: the table name
        :return: an engine compatible dataframe
        """
        raise NotImplementedError

    def save_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs: Any,
    ) -> None:  # pragma: no cover
        """Save the dataframe to a table

        :param df: the dataframe to save
        :param table: the table name
        :param mode: can accept ``overwrite``, ``error``,
          defaults to "overwrite"
        :param partition_spec: how to partition the dataframe before saving,
          defaults None
        :param kwargs: parameters to pass to the underlying framework
        """
        raise NotImplementedError


class MapEngine(EngineFacet):
    """The abstract base class for different map operation implementations.

    :param execution_engine: the execution engine this sql engine will run on
    """

    @abstractmethod
    def map_dataframe(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
        map_func_format_hint: Optional[str] = None,
    ) -> DataFrame:  # pragma: no cover
        """Apply a function to each partition after you partition the dataframe in a
        specified way.

        :param df: input dataframe
        :param map_func: the function to apply on every logical partition
        :param output_schema: |SchemaLikeObject| that can't be None.
          Please also understand :ref:`why we need this
          <tutorial:tutorials/beginner/interface:schema>`
        :param partition_spec: partition specification
        :param on_init: callback function when the physical partition is initializaing,
          defaults to None
        :param map_func_format_hint: the preferred data format for ``map_func``, it can
          be ``pandas``, `pyarrow`, etc, defaults to None. Certain engines can provide
          the most efficient map operations based on the hint.
        :return: the dataframe after the map operation

        .. note::

            Before implementing, you must read
            :ref:`this <tutorial:tutorials/advanced/execution_engine:map>`
            to understand what map is used for and how it should work.
        """
        raise NotImplementedError

    # @abstractmethod
    def map_bag(
        self,
        bag: Bag,
        map_func: Callable[[BagPartitionCursor, LocalBag], LocalBag],
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, Bag], Any]] = None,
    ) -> Bag:  # pragma: no cover
        """Apply a function to each partition after you partition the bag in a
        specified way.

        :param df: input dataframe
        :param map_func: the function to apply on every logical partition
        :param partition_spec: partition specification
        :param on_init: callback function when the physical partition is initializaing,
          defaults to None
        :return: the bag after the map operation
        """
        raise NotImplementedError


class ExecutionEngine(FugueEngineBase):
    """The abstract base class for execution engines.
    It is the layer that unifies core concepts of distributed computing,
    and separates the underlying computing frameworks from user's higher level logic.

    Please read |ExecutionEngineTutorial|
    to understand this most important Fugue concept

    :param conf: dict-like config, read
      :doc:`this <tutorial:tutorials/advanced/useful_config>`
      to learn Fugue specific options
    """

    def __init__(self, conf: Any):
        _conf = ParamDict(conf)
        self._conf = ParamDict({**_FUGUE_GLOBAL_CONF, **_conf})
        self._compile_conf = ParamDict()
        self._sql_engine: Optional[SQLEngine] = None
        self._map_engine: Optional[MapEngine] = None
        self._ctx_count = 0
        self._is_global = False
        self._private_lock = SerializableRLock()
        self._stop_engine_called = False

    @contextmanager
    def as_context(self) -> Iterator["ExecutionEngine"]:
        """Set this execution engine as the context engine. This function
        is thread safe and async safe.

        .. admonition:: Examples

            .. code-block:: python

                with engine.as_context():
                    transform(df, func)  # will use engine in this transformation

        """
        return self._as_context()

    @property
    def in_context(self) -> bool:
        """Whether this engine is being used as a context engine"""
        with _CONTEXT_LOCK:
            return self._ctx_count > 0

    def set_global(self) -> "ExecutionEngine":
        """Set this execution engine to be the global execution engine.

        .. note::
            Global engine is also considered as a context engine, so
            :meth:`~.ExecutionEngine.in_context` will also become true
            for the global engine.

        .. admonition:: Examples

            .. code-block:: python

                engine1.set_global():
                transform(df, func)  # will use engine1 in this transformation

                with engine2.as_context():
                    transform(df, func)  # will use engine2

                transform(df, func)  # will use engine1
        """
        _FUGUE_GLOBAL_EXECUTION_ENGINE_CONTEXT.set(self)
        return self

    @property
    def is_global(self) -> bool:
        """Whether this engine is being used as THE global engine"""
        return self._is_global

    def on_enter_context(self) -> None:  # pragma: no cover
        """The event hook when calling :func:`~.fugue.api.set_blobal_engine` or
        :func:`~.fugue.api.engine_context`, defaults to no operation
        """
        return

    def on_exit_context(self) -> None:  # pragma: no cover
        """The event hook when calling :func:`~.fugue.api.clear_blobal_engine` or
        exiting from :func:`~.fugue.api.engine_context`, defaults to no operation
        """
        return

    def stop(self) -> None:
        """Stop this execution engine, do not override
        You should customize :meth:`~.stop_engine` if necessary. This function
        ensures :meth:`~.stop_engine` to be called only once

        .. note::

            Once the engine is stopped it should not be used again
        """
        with self._private_lock:
            if not self._stop_engine_called:
                self.stop_engine()
                self._stop_engine_called = True

    def stop_engine(self) -> None:  # pragma: no cover
        """Custom logic to stop the execution engine, defaults to no operation"""
        return

    @property
    def conf(self) -> ParamDict:
        return self._conf

    @property
    def map_engine(self) -> MapEngine:
        """The :class:`~.MapEngine` currently used by this execution engine.
        You should use :meth:`~.set_map_engine` to set a new MapEngine
        instance. If not set, the default is :meth:`~.create_default_map_engine`
        """
        if self._map_engine is None:
            self._map_engine = self.create_default_map_engine()
        return self._map_engine

    @property
    def sql_engine(self) -> SQLEngine:
        """The :class:`~.SQLEngine` currently used by this execution engine.
        You should use :meth:`~.set_sql_engine` to set a new SQLEngine
        instance. If not set, the default is :meth:`~.create_default_sql_engine`
        """
        if self._sql_engine is None:
            self._sql_engine = self.create_default_sql_engine()
        return self._sql_engine

    def set_sql_engine(self, engine: SQLEngine) -> None:
        """Set :class:`~.SQLEngine` for this execution engine.
        If not set, the default is :meth:`~.create_default_sql_engine`

        :param engine: :class:`~.SQLEngine` instance
        """
        self._sql_engine = engine

    @property
    @abstractmethod
    def fs(self) -> FileSystem:  # pragma: no cover
        """File system of this engine instance"""
        raise NotImplementedError

    @abstractmethod
    def create_default_map_engine(self) -> MapEngine:  # pragma: no cover
        """Default MapEngine if user doesn't specify"""
        raise NotImplementedError

    @abstractmethod
    def create_default_sql_engine(self) -> SQLEngine:  # pragma: no cover
        """Default SQLEngine if user doesn't specify"""
        raise NotImplementedError

    @abstractmethod
    def get_current_parallelism(self) -> int:  # pragma: no cover
        """Get the current number of parallelism of this engine"""
        raise NotImplementedError

    @abstractmethod
    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        """Partition the input dataframe using ``partition_spec``.

        :param df: input dataframe
        :param partition_spec: how you want to partition the dataframe
        :return: repartitioned dataframe

        .. note::

            Before implementing please read |PartitionTutorial|
        """
        raise NotImplementedError

    @abstractmethod
    def broadcast(self, df: DataFrame) -> DataFrame:  # pragma: no cover
        """Broadcast the dataframe to all workers for a distributed computing framework

        :param df: the input dataframe
        :return: the broadcasted dataframe
        """
        raise NotImplementedError

    @abstractmethod
    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:  # pragma: no cover
        """Force materializing and caching the dataframe

        :param df: the input dataframe
        :param lazy: ``True``: first usage of the output will trigger persisting
          to happen; ``False`` (eager): persist is forced to happend immediately.
          Default to ``False``
        :param kwargs: parameter to pass to the underlying persist implementation
        :return: the persisted dataframe

        .. note::

            ``persist`` can only guarantee the persisted dataframe will be computed
            for only once. However this doesn't mean the backend really breaks up the
            execution dependency at the persisting point. Commonly, it doesn't cause
            any issue, but if your execution graph is long, it may cause expected
            problems for example, stack overflow.
        """
        raise NotImplementedError

    @abstractmethod
    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: Optional[List[str]] = None,
    ) -> DataFrame:  # pragma: no cover
        """Join two dataframes

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param how: can accept ``semi``, ``left_semi``, ``anti``, ``left_anti``,
          ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys.
        :return: the joined dataframe

        .. note::

            Please read :func:`~.fugue.dataframe.utils.get_join_schemas`
        """
        raise NotImplementedError

    @abstractmethod
    def union(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
    ) -> DataFrame:  # pragma: no cover
        """Join two dataframes

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param distinct: ``true`` for ``UNION`` (== ``UNION DISTINCT``),
          ``false`` for ``UNION ALL``
        :return: the unioned dataframe

        .. note::

            Currently, the schema of ``df1`` and ``df2`` must be identical, or
            an exception will be thrown.
        """
        raise NotImplementedError

    @abstractmethod
    def subtract(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
    ) -> DataFrame:  # pragma: no cover
        """``df1 - df2``

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param distinct: ``true`` for ``EXCEPT`` (== ``EXCEPT DISTINCT``),
          ``false`` for ``EXCEPT ALL``
        :return: the unioned dataframe

        .. note::

            Currently, the schema of ``df1`` and ``df2`` must be identical, or
            an exception will be thrown.
        """
        raise NotImplementedError

    @abstractmethod
    def intersect(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
    ) -> DataFrame:  # pragma: no cover
        """Intersect ``df1`` and ``df2``

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param distinct: ``true`` for ``INTERSECT`` (== ``INTERSECT DISTINCT``),
          ``false`` for ``INTERSECT ALL``
        :return: the unioned dataframe

        .. note::

            Currently, the schema of ``df1`` and ``df2`` must be identical, or
            an exception will be thrown.
        """
        raise NotImplementedError

    @abstractmethod
    def distinct(
        self,
        df: DataFrame,
    ) -> DataFrame:  # pragma: no cover
        """Equivalent to ``SELECT DISTINCT * FROM df``

        :param df: dataframe
        :return: [description]
        :rtype: DataFrame
        """
        pass

    @abstractmethod
    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: int = None,
        subset: List[str] = None,
    ) -> DataFrame:  # pragma: no cover
        """Drop NA recods from dataframe

        :param df: DataFrame
        :param how: 'any' or 'all'. 'any' drops rows that contain any nulls.
          'all' drops rows that contain all nulls.
        :param thresh: int, drops rows that have less than thresh non-null values
        :param subset: list of columns to operate on

        :return: DataFrame with NA records dropped
        :rtype: DataFrame
        """
        pass

    @abstractmethod
    def fillna(
        self, df: DataFrame, value: Any, subset: List[str] = None
    ) -> DataFrame:  # pragma: no cover
        """
        Fill ``NULL``, ``NAN``, ``NAT`` values in a dataframe

        :param df: DataFrame
        :param value: if scalar, fills all columns with same value.
            if dictionary, fills NA using the keys as column names and the
            values as the replacement values.
        :param subset: list of columns to operate on. ignored if value is
            a dictionary

        :return: DataFrame with NA records filled
        :rtype: DataFrame
        """
        pass

    @abstractmethod
    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:  # pragma: no cover
        """
        Sample dataframe by number of rows or by fraction

        :param df: DataFrame
        :param n: number of rows to sample, one and only one of ``n`` and ``frac``
          must be set
        :param frac: fraction [0,1] to sample, one and only one of ``n`` and ``frac``
          must be set
        :param replace: whether replacement is allowed. With replacement,
          there may be duplicated rows in the result, defaults to False
        :param seed: seed for randomness, defaults to None

        :return: sampled dataframe
        :rtype: DataFrame
        """
        pass

    @abstractmethod
    def take(
        self,
        df: DataFrame,
        n: int,
        presort: str,
        na_position: str = "last",
        partition_spec: Optional[PartitionSpec] = None,
    ) -> DataFrame:  # pragma: no cover
        """
        Get the first n rows of a DataFrame per partition. If a presort is defined,
        use the presort before applying take. presort overrides partition_spec.presort.
        The Fugue implementation of the presort follows Pandas convention of specifying
        NULLs first or NULLs last. This is different from the Spark and SQL convention
        of NULLs as the smallest value.

        :param df: DataFrame
        :param n: number of rows to return
        :param presort: presort expression similar to partition presort
        :param na_position: position of null values during the presort.
            can accept ``first`` or ``last``
        :param partition_spec: PartitionSpec to apply the take operation

        :return: n rows of DataFrame per partition
        :rtype: DataFrame
        """
        pass

    def select(
        self,
        df: DataFrame,
        cols: SelectColumns,
        where: Optional[ColumnExpr] = None,
        having: Optional[ColumnExpr] = None,
    ) -> DataFrame:
        """The functional interface for SQL select statement

        :param df: the dataframe to be operated on
        :param cols: column expressions
        :param where: ``WHERE`` condition expression, defaults to None
        :param having: ``having`` condition expression, defaults to None. It
          is used when ``cols`` contains aggregation columns, defaults to None
        :return: the select result as a dataframe

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

                # select existed and new columns
                engine.select(df, SelectColumns(col("a"),col("b"),lit(1,"another")))
                engine.select(df, SelectColumns(col("a"),(col("b")+lit(1)).alias("x")))

                # aggregation
                # SELECT COUNT(DISTINCT *) AS x FROM df
                engine.select(
                    df,
                    SelectColumns(f.count_distinct(all_cols()).alias("x")))

                # SELECT a, MAX(b+1) AS x FROM df GROUP BY a
                engine.select(
                    df,
                    SelectColumns(col("a"),f.max(col("b")+lit(1)).alias("x")))

                # SELECT a, MAX(b+1) AS x FROM df
                #   WHERE b<2 AND a>1
                #   GROUP BY a
                #   HAVING MAX(b+1)>0
                engine.select(
                    df,
                    SelectColumns(col("a"),f.max(col("b")+lit(1)).alias("x")),
                    where=(col("b")<2) & (col("a")>1),
                    having=f.max(col("b")+lit(1))>0
                )
        """
        gen = SQLExpressionGenerator(enable_cast=False)
        df_name = TempTableName()
        sql = StructuredRawSQL(
            gen.select(cols, df_name.key, where=where, having=having),
            dialect=FUGUE_SQL_DEFAULT_DIALECT,
        )
        res = self.sql_engine.select(DataFrames({df_name.key: self.to_df(df)}), sql)
        diff = gen.correct_select_schema(df.schema, cols, res.schema)
        return res if diff is None else res.alter_columns(diff)

    def filter(self, df: DataFrame, condition: ColumnExpr) -> DataFrame:
        """Filter rows by the given condition

        :param df: the dataframe to be filtered
        :param condition: (boolean) column expression
        :return: the filtered dataframe

        .. admonition:: New Since
            :class: hint

            **0.6.0**

        .. seealso::

            Please find more expression examples in :mod:`fugue.column.sql` and
            :mod:`fugue.column.functions`

        .. admonition:: Examples

            .. code-block:: python

                import fugue.column.functions as f

                engine.filter(df, (col("a")>1) & (col("b")=="x"))
                engine.filter(df, f.coalesce(col("a"),col("b"))>1)
        """
        return self.select(df, cols=SelectColumns(all_cols()), where=condition)

    def assign(self, df: DataFrame, columns: List[ColumnExpr]) -> DataFrame:
        """Update existing columns with new values and add new columns

        :param df: the dataframe to set columns
        :param columns: column expressions
        :return: the updated dataframe

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

                # assume df has schema: a:int,b:str

                # add constant column x
                engine.assign(df, lit(1,"x"))

                # change column b to be a constant integer
                engine.assign(df, lit(1,"b"))

                # add new x to be a+b
                engine.assign(df, (col("a")+col("b")).alias("x"))

                # cast column a data type to double
                engine.assign(df, col("a").cast(float))
        """
        SelectColumns(
            *columns
        ).assert_no_wildcard().assert_all_with_names().assert_no_agg()

        ck = {v: k for k, v in enumerate(df.columns)}
        cols = [col(n) for n in ck.keys()]
        for c in columns:
            if c.output_name not in ck:
                cols.append(c)
            else:
                cols[ck[c.output_name]] = c
        return self.select(df, SelectColumns(*cols))

    def aggregate(
        self,
        df: DataFrame,
        partition_spec: Optional[PartitionSpec],
        agg_cols: List[ColumnExpr],
    ):
        """Aggregate on dataframe

        :param df: the dataframe to aggregate on
        :param partition_spec: PartitionSpec to specify partition keys
        :param agg_cols: aggregation expressions
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
                engine.aggregate(
                    df,
                    partition_spec=None,
                    agg_cols=[f.max(col("b"))])

                # SELECT a, MAX(b) AS x FROM df GROUP BY a
                engine.aggregate(
                    df,
                    partition_spec=PartitionSpec(by=["a"]),
                    agg_cols=[f.max(col("b")).alias("x")])
        """
        assert_or_throw(len(agg_cols) > 0, ValueError("agg_cols can't be empty"))
        assert_or_throw(
            all(is_agg(x) for x in agg_cols),
            ValueError("all agg_cols must be aggregation functions"),
        )
        keys: List[ColumnExpr] = []
        if partition_spec is not None and len(partition_spec.partition_by) > 0:
            keys = [col(y) for y in partition_spec.partition_by]
        cols = SelectColumns(*keys, *agg_cols)
        return self.select(df, cols=cols)

    def convert_yield_dataframe(self, df: DataFrame, as_local: bool) -> DataFrame:
        """Convert a yield dataframe to a dataframe that can be used after this
        execution engine stops.

        :param df: DataFrame
        :param as_local: whether yield a local dataframe
        :return: another DataFrame that can be used after this execution engine stops

        .. note::

            By default, the output dataframe is the input dataframe. But it should be
            overridden if when an engine stops and the input dataframe will become
            invalid.

            For example, if you custom a spark engine where you start and stop the spark
            session in this engine's :meth:`~.start_engine` and :meth:`~.stop_engine`,
            then the spark dataframe will be invalid. So you may consider converting
            it to a local dataframe so it can still exist after the engine stops.
        """
        return df.as_local() if as_local else df

    def zip(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str = "inner",
        partition_spec: Optional[PartitionSpec] = None,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
        df1_name: Optional[str] = None,
        df2_name: Optional[str] = None,
    ):
        """Partition the two dataframes in the same way with ``partition_spec`` and
        zip the partitions together on the partition keys.

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param how: can accept ``inner``, ``left_outer``, ``right_outer``,
          ``full_outer``, ``cross``, defaults to ``inner``
        :param partition_spec: partition spec to partition each dataframe,
          defaults to empty.
        :type partition_spec: PartitionSpec, optional
        :param temp_path: file path to store the data (used only if the serialized data
          is larger than ``to_file_threshold``), defaults to None
        :param to_file_threshold: file byte size threshold, defaults to -1
        :param df1_name: df1's name in the zipped dataframe, defaults to None
        :param df2_name: df2's name in the zipped dataframe, defaults to None

        :return: a zipped dataframe, the metadata of the
          dataframe will indicate it's zipped

        .. note::

            * Different from join, ``df1`` and ``df2`` can have common columns that you
              will not use as partition keys.
            * If ``on`` is not specified it will also use the common columns of the two
              dataframes (if it's not a cross zip)
            * For non-cross zip, the two dataframes must have common columns, or error
              will be thrown

        .. seealso::

            For more details and examples, read |ZipComap|.
        """
        partition_spec = partition_spec or PartitionSpec()
        on = list(partition_spec.partition_by)
        how = how.lower()
        assert_or_throw(
            "semi" not in how and "anti" not in how,
            InvalidOperationError("zip does not support semi or anti joins"),
        )
        serialized_cols: Dict[str, Any] = {}
        schemas: Dict[str, Any] = {}
        if len(on) == 0:
            if how != "cross":
                on = df1.schema.extract(
                    df2.schema.names, ignore_key_mismatch=True
                ).names
        else:
            assert_or_throw(
                how != "cross",
                InvalidOperationError("can't specify keys for cross join"),
            )
        partition_spec = PartitionSpec(partition_spec, by=on)

        def update_df(df: DataFrame, name: Optional[str]) -> DataFrame:
            if name is None:
                name = f"_{len(serialized_cols)}"
            if not df.metadata.get("serialized", False):
                df = self._serialize_by_partition(
                    df,
                    partition_spec or PartitionSpec(),
                    name,
                    temp_path,
                    to_file_threshold,
                    has_name=name is not None,
                )
            for k in df.metadata["serialized_cols"].keys():
                assert_or_throw(
                    k not in serialized_cols, lambda: ValueError(f"{k} is duplicated")
                )
                serialized_cols[k] = df.metadata["serialized_cols"][k]
                schemas[k] = df.metadata["schemas"][k]
            return df

        df1 = update_df(df1, df1_name)
        df2 = update_df(df2, df2_name)
        metadata = dict(
            serialized=True,
            serialized_cols=serialized_cols,
            schemas=schemas,
            serialized_has_name=df1_name is not None or df2_name is not None,
        )
        res = self.join(df1, df2, how=how, on=on)
        res.reset_metadata(metadata)
        return res

    def zip_all(
        self,
        dfs: DataFrames,
        how: str = "inner",
        partition_spec: Optional[PartitionSpec] = None,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
    ) -> DataFrame:
        """Zip multiple dataframes together with given partition
        specifications.

        :param dfs: |DataFramesLikeObject|
        :param how: can accept ``inner``, ``left_outer``, ``right_outer``,
          ``full_outer``, ``cross``, defaults to ``inner``
        :param partition_spec: |PartitionLikeObject|, defaults to empty.
        :param temp_path: file path to store the data (used only if the serialized data
          is larger than ``to_file_threshold``), defaults to None
        :param to_file_threshold: file byte size threshold, defaults to -1

        :return: a zipped dataframe, the metadata of the
          dataframe will indicated it's zipped

        .. note::

            * Please also read :meth:`~.zip`
            * If ``dfs`` is dict like, the zipped dataframe will be dict like,
              If ``dfs`` is list like, the zipped dataframe will be list like
            * It's fine to contain only one dataframe in ``dfs``

        .. seealso::

            For more details and examples, read |ZipComap|
        """
        partition_spec = partition_spec or PartitionSpec()
        assert_or_throw(len(dfs) > 0, "can't zip 0 dataframes")
        pairs = list(dfs.items())
        has_name = dfs.has_key
        if len(dfs) == 1:
            return self._serialize_by_partition(
                pairs[0][1],
                partition_spec,
                pairs[0][0],
                temp_path,
                to_file_threshold,
                has_name=has_name,
            )
        df = self.zip(
            pairs[0][1],
            pairs[1][1],
            how=how,
            partition_spec=partition_spec,
            temp_path=temp_path,
            to_file_threshold=to_file_threshold,
            df1_name=pairs[0][0] if has_name else None,
            df2_name=pairs[1][0] if has_name else None,
        )
        for i in range(2, len(dfs)):
            df = self.zip(
                df,
                pairs[i][1],
                how=how,
                partition_spec=partition_spec,
                temp_path=temp_path,
                to_file_threshold=to_file_threshold,
                df2_name=pairs[i][0] if has_name else None,
            )
        return df

    def comap(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, DataFrames], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrames], Any]] = None,
    ):
        """Apply a function to each zipped partition on the zipped dataframe.

        :param df: input dataframe, it must be a zipped dataframe (it has to be a
          dataframe output from :meth:`~.zip` or :meth:`~.zip_all`)
        :param map_func: the function to apply on every zipped partition
        :param output_schema: |SchemaLikeObject| that can't be None.
          Please also understand :ref:`why we need this
          <tutorial:tutorials/beginner/interface:schema>`
        :param partition_spec: partition specification for processing the zipped
          zipped dataframe.
        :param on_init: callback function when the physical partition is initializaing,
          defaults to None
        :return: the dataframe after the comap operation

        .. note::

            * The input of this method must be an output of :meth:`~.zip` or
              :meth:`~.zip_all`
            * The ``partition_spec`` here is NOT related with how you zipped the
              dataframe and however you set it, will only affect the processing speed,
              actually the partition keys will be overriden to the zipped dataframe
              partition keys. You may use it in this way to improve the efficiency:
              ``PartitionSpec(algo="even", num="ROWCOUNT")``,
              this tells the execution engine to put each zipped partition into a
              physical partition so it can achieve the best possible load balance.
            * If input dataframe has keys, the dataframes you get in ``map_func`` and
              ``on_init`` will have keys, otherwise you will get list-like dataframes
            * on_init function will get a DataFrames object that has the same structure,
              but has all empty dataframes, you can use the schemas but not the data.

        .. seealso::

            For more details and examples, read |ZipComap|
        """
        assert_or_throw(df.metadata["serialized"], ValueError("df is not serilaized"))
        cs = _Comap(df, map_func, on_init)
        key_schema = df.schema - list(df.metadata["serialized_cols"].values())
        partition_spec = PartitionSpec(partition_spec, by=list(key_schema.keys()))
        return self.map_engine.map_dataframe(
            df, cs.run, output_schema, partition_spec, on_init=cs.on_init
        )

    def load_yielded(self, df: Yielded) -> DataFrame:
        """Load yielded dataframe

        :param df: the yielded dataframe
        :return: an engine compatible dataframe
        """
        if isinstance(df, PhysicalYielded):
            if df.storage_type == "file":
                return self.load_df(path=df.name)
            else:
                return self.sql_engine.load_table(table=df.name)
        else:
            return self.to_df(df.result)  # type: ignore

    @abstractmethod
    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:  # pragma: no cover
        """Load dataframe from persistent storage

        :param path: the path to the dataframe
        :param format_hint: can accept ``parquet``, ``csv``, ``json``,
          defaults to None, meaning to infer
        :param columns: list of columns or a |SchemaLikeObject|, defaults to None
        :param kwargs: parameters to pass to the underlying framework
        :return: an engine compatible dataframe

        For more details and examples, read |ZipComap|.
        """
        raise NotImplementedError

    @abstractmethod
    def save_df(
        self,
        df: DataFrame,
        path: str,
        format_hint: Any = None,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:  # pragma: no cover
        """Save dataframe to a persistent storage

        :param df: input dataframe
        :param path: output path
        :param format_hint: can accept ``parquet``, ``csv``, ``json``,
          defaults to None, meaning to infer
        :param mode: can accept ``overwrite``, ``append``, ``error``,
          defaults to "overwrite"
        :param partition_spec: how to partition the dataframe before saving,
          defaults to empty
        :param force_single: force the output as a single file, defaults to False
        :param kwargs: parameters to pass to the underlying framework

        For more details and examples, read |LoadSave|.
        """
        raise NotImplementedError

    def __copy__(self) -> "ExecutionEngine":
        return self

    def __deepcopy__(self, memo: Any) -> "ExecutionEngine":
        return self

    def _as_context(self) -> Iterator["ExecutionEngine"]:
        """Set this execution engine as the context engine. This function
        is thread safe and async safe.

        .. admonition:: Examples

            .. code-block:: python

                with engine.as_context():
                    transform(df, func)  # will use engine in this transformation

        """
        with _CONTEXT_LOCK:
            self._enter_context()
            token = _FUGUE_EXECUTION_ENGINE_CONTEXT.set(self)  # type: ignore
        try:
            yield self
        finally:
            with _CONTEXT_LOCK:
                _FUGUE_EXECUTION_ENGINE_CONTEXT.reset(token)
                self._exit_context()

    def _enter_context(self):
        self.on_enter_context()
        self._ctx_count += 1

    def _exit_context(self):
        self._ctx_count -= 1
        self.on_exit_context()
        if self._ctx_count == 0:
            self.stop()

    def _serialize_by_partition(
        self,
        df: DataFrame,
        partition_spec: PartitionSpec,
        df_name: str,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
        has_name: bool = False,
    ) -> DataFrame:
        to_file_threshold = _get_file_threshold(to_file_threshold)
        on = list(filter(lambda k: k in df.schema, partition_spec.partition_by))
        presort = list(
            filter(lambda p: p[0] in df.schema, partition_spec.presort.items())
        )
        col_name = _df_name_to_serialize_col(df_name)
        if len(on) == 0:
            partition_spec = PartitionSpec(
                partition_spec, num=1, by=[], presort=presort
            )
            output_schema = Schema(f"{col_name}:str")
        else:
            partition_spec = PartitionSpec(partition_spec, by=on, presort=presort)
            output_schema = partition_spec.get_key_schema(df.schema) + f"{col_name}:str"
        s = _PartitionSerializer(output_schema, temp_path, to_file_threshold)
        metadata = dict(
            serialized=True,
            serialized_cols={df_name: col_name},
            schemas={df_name: str(df.schema)},
            serialized_has_name=has_name,
        )
        res = self.map_engine.map_dataframe(df, s.run, output_schema, partition_spec)
        res.reset_metadata(metadata)
        return res


@fugue_annotated_param(ExecutionEngine, "e", child_can_reuse_code=True)
class ExecutionEngineParam(AnnotatedParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
    ):
        super().__init__(param)
        self._type = self.annotation

    def to_input(self, engine: Any) -> Any:
        assert_or_throw(
            isinstance(engine, self._type),
            FugueWorkflowRuntimeError(f"{engine} is not of type {self._type}"),
        )
        return engine

    def __uuid__(self) -> str:
        return to_uuid(self.code, self.annotation, self._type)


def _get_file_threshold(size: Any) -> int:
    if size is None:
        return -1
    if isinstance(size, int):
        return size
    return to_size(size)


def _df_name_to_serialize_col(name: str):
    assert_or_throw(name is not None, "Dataframe name can't be None")
    name = "__blob__" + name + "__"
    assert_or_throw(validate_triad_var_name(name), "Invalid name " + name)
    return name


class _PartitionSerializer(object):
    def __init__(
        self, output_schema: Schema, temp_path: Optional[str], to_file_threshold: int
    ):
        self.output_schema = output_schema
        self.temp_path = temp_path
        self.to_file_threshold = to_file_threshold

    def run(self, cursor: PartitionCursor, df: LocalDataFrame) -> LocalDataFrame:
        data = serialize_df(df, self.to_file_threshold, self.temp_path)
        row = cursor.key_value_array + [data]
        return ArrayDataFrame([row], self.output_schema)


class _Comap(object):
    def __init__(
        self,
        df: DataFrame,
        func: Callable,
        on_init: Optional[Callable[[int, DataFrames], Any]],
    ):
        self.schemas = df.metadata["schemas"]
        self.df_idx = [
            (df.schema.index_of_key(v), k, self.schemas[k])
            for k, v in df.metadata["serialized_cols"].items()
        ]
        self.named = df.metadata.get("serialized_has_name", False)
        self.func = func
        self._on_init = on_init

    def on_init(self, partition_no, df: DataFrame) -> None:
        if self._on_init is None:
            return
        # TODO: currently, get_output_schema only gets empty dataframes
        empty_dfs = _generate_comap_empty_dfs(self.schemas, self.named)
        self._on_init(partition_no, empty_dfs)

    def run(self, cursor: PartitionCursor, df: LocalDataFrame) -> LocalDataFrame:
        data = df.as_array(type_safe=True)
        assert_or_throw(
            len(data) == 1,
            FugueBug("each comap partition can have one and only one row"),
        )
        dfs = DataFrames(list(self._get_dfs(data[0])))
        return self.func(cursor, dfs)

    def _get_dfs(self, row: Any) -> Iterable[Any]:
        for k, name, v in self.df_idx:
            if row[k] is None:
                df: DataFrame = ArrayDataFrame([], v)
            else:
                df = deserialize_df(row[k])  # type: ignore
                assert df is not None
            if self.named:
                yield name, df
            else:
                yield df


def _generate_comap_empty_dfs(schemas: Any, named: bool) -> DataFrames:
    if named:
        return DataFrames({k: ArrayDataFrame([], v) for k, v in schemas.items()})
    else:
        return DataFrames([ArrayDataFrame([], v) for v in schemas.values()])
