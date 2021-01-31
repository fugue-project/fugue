import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from fugue.collections.partition import (
    EMPTY_PARTITION_SPEC,
    PartitionCursor,
    PartitionSpec,
)
from fugue.constants import FUGUE_DEFAULT_CONF
from fugue.dataframe import DataFrame, DataFrames
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import LocalDataFrame
from fugue.dataframe.utils import deserialize_df, serialize_df
from fugue.exceptions import FugueBug
from fugue.rpc import RPCServer, make_rpc_server
from triad.collections import ParamDict, Schema
from triad.collections.fs import FileSystem
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_size
from triad.utils.string import validate_triad_var_name
from threading import RLock

_DEFAULT_JOIN_KEYS: List[str] = []


class SQLEngine(ABC):
    """The abstract base class for different SQL execution implementations. Please read
    :ref:`this <tutorial:/tutorials/execution_engine.ipynb#sqlengine>`
    to understand the concept

    :param execution_engine: the execution engine this sql engine will run on
    """

    def __init__(self, execution_engine: "ExecutionEngine") -> None:
        self._execution_engine = execution_engine

    @property
    def execution_engine(self) -> "ExecutionEngine":
        """the execution engine this sql engine will run on"""
        return self._execution_engine

    @abstractmethod
    def select(self, dfs: DataFrames, statement: str) -> DataFrame:  # pragma: no cover
        """Execute select statement on the sql engine.

        :param dfs: a collection of dataframes that must have keys
        :param statement: the ``SELECT`` statement using the ``dfs`` keys as tables
        :return: result of the ``SELECT`` statement

        :Example:

        >>> dfs = DataFrames(a=df1, b=df2)
        >>> sql_engine.select(dfs, "SELECT * FROM a UNION SELECT * FROM b")

        :Notice:

        There can be tables that is not in ``dfs``. For example you want to select
        from hive without input DataFrames:

        >>> sql_engine.select(DataFrames(), "SELECT * FROM hive.a.table")
        """
        raise NotImplementedError


class ExecutionEngine(ABC):
    """The abstract base class for execution engines.
    It is the layer that unifies core concepts of distributed computing,
    and separates the underlying computing frameworks from user’s higher level logic.

    Please read
    :ref:`The ExecutionEngine Tutorial <tutorial:/tutorials/execution_engine.ipynb>`
    to understand this most important Fugue concept

    :param conf: dict-like config, read
      :ref:`this <tutorial:/tutorials/useful_config.ipynb>`
      to learn Fugue specific options
    """

    def __init__(self, conf: Any):
        _conf = ParamDict(conf)
        self._conf = ParamDict({**FUGUE_DEFAULT_CONF, **_conf})
        self._rpc_server = make_rpc_server(self.conf)
        self._engine_start_lock = RLock()
        self._engine_start = 0

    def start(self) -> None:
        """Start this execution engine, do not override.
        You should customize :meth:`~.start_engine` if necessary.
        """
        with self._engine_start_lock:
            if self._engine_start == 0:
                self.rpc_server.start()
                self.start_engine()
            self._engine_start += 1

    def stop(self) -> None:
        """Stop this execution engine, do not override
        You should customize :meth:`~.stop_engine` if necessary.
        """
        with self._engine_start_lock:
            if self._engine_start == 1:
                try:
                    self.stop_engine()
                finally:
                    self._engine_start -= 1
                    self.rpc_server.stop()
            else:
                self._engine_start -= 1
            self._engine_start = max(0, self._engine_start)

    def start_engine(self) -> None:  # pragma: no cover
        """Custom logic to start the execution engine, defaults to no operation"""
        return

    def stop_engine(self) -> None:  # pragma: no cover
        """Custom logic to stop the execution engine, defaults to no operation"""
        return

    @property
    def conf(self) -> ParamDict:
        """All configurations of this engine instance.

        :Notice:

        it can contain more than you providec, for example
        in :class:`~fugue_spark.execution_engine.SparkExecutionEngine`,
        the Spark session can bring in more config, they are all accessible
        using this property.
        """
        return self._conf

    @property
    def rpc_server(self) -> RPCServer:
        return self._rpc_server

    @property
    @abstractmethod
    def log(self) -> logging.Logger:  # pragma: no cover
        """Logger of this engine instance"""
        raise NotImplementedError

    @property
    @abstractmethod
    def fs(self) -> FileSystem:  # pragma: no cover
        """File system of this engine instance"""
        raise NotImplementedError

    @property
    @abstractmethod
    def default_sql_engine(self) -> SQLEngine:  # pragma: no cover
        """Default SQLEngine if user doesn't specify"""
        raise NotImplementedError

    @abstractmethod
    def to_df(
        self, data: Any, schema: Any = None, metadata: Any = None
    ) -> DataFrame:  # pragma: no cover
        """Convert a data structure to this engine compatible DataFrame

        :param data: :class:`~fugue.dataframe.dataframe.DataFrame`,
          pandas DataFramme or list or iterable of arrays or others that
          is supported by certain engine implementation
        :param schema: |SchemaLikeObject|, defaults to None
        :param metadata: |ParamsLikeObject|, defaults to None
        :return: engine compatible dataframe

        :Notice:

        There are certain conventions to follow for a new implementation:

        * if the input is already in compatible dataframe type, it should return itself
        * all other methods in the engine interface should take arbitrary dataframes and
          call this method to convert before doing anything
        """
        raise NotImplementedError

    @abstractmethod
    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        """Partition the input dataframe using ``partition_spec``.

        :param df: input dataframe
        :param partition_spec: how you want to partition the dataframe
        :return: repartitioned dataframe

        :Notice:

        Before implementing please read |PartitionTutorial|
        """
        raise NotImplementedError

    @abstractmethod
    def map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:  # pragma: no cover
        """Apply a function to each partition after you partition the data in a
        specified way.

        :param df: input dataframe
        :param map_func: the function to apply on every logical partition
        :param output_schema: |SchemaLikeObject| that can't be None.
          Please also understand :ref:`why we need this
          <tutorial:/tutorials/transformer.ipynb#why-explicit-on-output-schema?>`
        :param partition_spec: partition specification
        :param metadata: dict-like metadata object to add to the dataframe after the
          map operation, defaults to None
        :param on_init: callback function when the physical partition is initializaing,
          defaults to None
        :return: the dataframe after the map operation

        :Notice:

        Before implementing, you must read
        :ref:`this <tutorial:/tutorials/execution_engine.ipynb#map>` to understand
        what map is used for and how it should work.
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
        :param args: parameter to pass to the underlying persist implementation
        :param kwargs: parameter to pass to the underlying persist implementation
        :return: the persisted dataframe

        :Notice:

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
        on: List[str] = _DEFAULT_JOIN_KEYS,
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        """Join two dataframes

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param how: can accept ``semi``, ``left_semi``, ``anti``, ``left_anti``,
          ``inner``, ``left_outer``, ``right_outer``, ``full_outer``, ``cross``
        :param on: it can always be inferred, but if you provide, it will be
          validated against the inferred keys.
        :param metadata: dict-like object to add to the result dataframe,
          defaults to None
        :return: the joined dataframe

        :Notice:

        Please read :func:`this <fugue.dataframe.utils.get_join_schemas>`
        """
        raise NotImplementedError

    @abstractmethod
    def union(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        """Join two dataframes

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param distinct: ``true`` for ``UNION`` (== ``UNION DISTINCT``),
          ``false`` for ``UNION ALL``
        :param metadata: dict-like object to add to the result dataframe,
          defaults to None
        :return: the unioned dataframe

        :Notice:

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
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        """``df1 - df2``

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param distinct: ``true`` for ``EXCEPT`` (== ``EXCEPT DISTINCT``),
          ``false`` for ``EXCEPT ALL``
        :param metadata: dict-like object to add to the result dataframe,
          defaults to None
        :return: the unioned dataframe

        :Notice:

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
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        """Intersect ``df1`` and ``df2``

        :param df1: the first dataframe
        :param df2: the second dataframe
        :param distinct: ``true`` for ``INTERSECT`` (== ``INTERSECT DISTINCT``),
          ``false`` for ``INTERSECT ALL``
        :param metadata: dict-like object to add to the result dataframe,
          defaults to None
        :return: the unioned dataframe

        :Notice:

        Currently, the schema of ``df1`` and ``df2`` must be identical, or
        an exception will be thrown.
        """
        raise NotImplementedError

    @abstractmethod
    def distinct(
        self,
        df: DataFrame,
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        """Equivalent to ``SELECT DISTINCT * FROM df``

        :param df: dataframe
        :param metadata: dict-like object to add to the result dataframe,
          defaults to None
        :type metadata: Any, optional
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
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        """Drop NA recods from dataframe

        :param df: DataFrame
        :param how: 'any' or 'all'. 'any' drops rows that contain any nulls.
          'all' drops rows that contain all nulls.
        :param thresh: int, drops rows that have less than thresh non-null values
        :param subset: list of columns to operate on
        :param metadata: dict-like object to add to the result dataframe,
            defaults to None
        :type metadata: Any, optional

        :return: DataFrame with NA records dropped
        :rtype: DataFrame
        """
        pass

    @abstractmethod
    def fillna(
        self, df: DataFrame, value: Any, subset: List[str] = None, metadata: Any = None
    ) -> DataFrame:  # pragma: no cover
        """
        Fill ``NULL``, ``NAN``, ``NAT`` values in a dataframe

        :param df: DataFrame
        :param value: if scalar, fills all columns with same value.
            if dictionary, fills NA using the keys as column names and the
            values as the replacement values.
        :param subset: list of columns to operate on. ignored if value is
            a dictionary
        :param metadata: dict-like object to add to the result dataframe,
            defaults to None
        :type metadata: Any, optional

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
        metadata: Any = None,
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
        :param metadata: dict-like object to add to the result dataframe,
            defaults to None

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
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        metadata: Any = None,
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
        :param metadata: dict-like object to add to the result dataframe,
            defaults to None

        :return: n rows of DataFrame per partition
        :rtype: DataFrame
        """
        pass

    def zip(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str = "inner",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
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

        :Notice:

        * Different from join, ``df1`` and ``df2`` can have common columns that you will
          not use as partition keys.
        * If ``on`` is not specified it will also use the common columns of the two
          dataframes (if it's not a cross zip)
        * For non-cross zip, the two dataframes must have common columns, or error will
          be thrown

        For more details and examples, read
        :ref:`Zip & Comap <tutorial:/tutorials/execution_engine.ipynb#zip-&-comap>`.
        """
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
                    partition_spec,
                    name,
                    temp_path,
                    to_file_threshold,
                    has_name=name is not None,
                )
            for k in df.metadata["serialized_cols"].keys():
                assert_or_throw(
                    k not in serialized_cols, ValueError(f"{k} is duplicated")
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
        return self.join(df1, df2, how=how, on=on, metadata=metadata)

    def zip_all(
        self,
        dfs: DataFrames,
        how: str = "inner",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
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

        :Notice:

        * Please also read :meth:`~.zip`
        * If ``dfs`` is dict like, the zipped dataframe will be dict like,
          If ``dfs`` is list like, the zipped dataframe will be list like
        * It's fine to contain only one dataframe in ``dfs``

        For more details and examples, read
        :ref:`Zip & Comap <tutorial:/tutorials/execution_engine.ipynb#zip-&-comap>`.
        """
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
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrames], Any]] = None,
    ):
        """Apply a function to each zipped partition on the zipped dataframe.

        :param df: input dataframe, it must be a zipped dataframe (it has to be a
          dataframe output from :meth:`~.zip` or :meth:`~.zip_all`)
        :param map_func: the function to apply on every zipped partition
        :param output_schema: |SchemaLikeObject| that can't be None.
          Please also understand :ref:`why we need this
          <tutorial:/tutorials/cotransformer.ipynb#why-explicit-on-output-schema?>`
        :param partition_spec: partition specification for processing the zipped
          zipped dataframe.
        :param metadata: dict-like metadata object to add to the dataframe after the
          map operation, defaults to None
        :param on_init: callback function when the physical partition is initializaing,
          defaults to None
        :return: the dataframe after the comap operation

        :Notice:

        * The input of this method must be an output of :meth:`~.zip` or
          :meth:`~.zip_all`
        * The ``partition_spec`` here is NOT related with how you zipped the dataframe
          and however you set it, will only affect the processing speed, actually the
          partition keys will be overriden to the zipped dataframe partition keys.
          You may use it in this way to improve the efficiency:
          ``PartitionSpec(algo="even", num="ROWCOUNT")``,
          this tells the execution engine to put each zipped partition into a physical
          partition so it can achieve the best possible load balance.
        * If input dataframe has keys, the dataframes you get in ``map_func`` and
          ``on_init`` will have keys, otherwise you will get list-like dataframes
        * on_init function will get a DataFrames object that has the same structure, but
          has all empty dataframes, you can use the schemas but not the data.

        For more details and examples, read
        :ref:`Zip & Comap <tutorial:/tutorials/execution_engine.ipynb#zip-&-comap>`.
        """
        assert_or_throw(df.metadata["serialized"], ValueError("df is not serilaized"))
        cs = _Comap(df, map_func, on_init)
        key_schema = df.schema - list(df.metadata["serialized_cols"].values())
        partition_spec = PartitionSpec(partition_spec, by=list(key_schema.keys()))
        return self.map(
            df, cs.run, output_schema, partition_spec, metadata, on_init=cs.on_init
        )

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

        For more details and examples, read
        :ref:`Load & Save <tutorial:/tutorials/execution_engine.ipynb#load-&-save>`.
        """
        raise NotImplementedError

    @abstractmethod
    def save_df(
        self,
        df: DataFrame,
        path: str,
        format_hint: Any = None,
        mode: str = "overwrite",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
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

        For more details and examples, read
        :ref:`Load & Save <tutorial:/tutorials/execution_engine.ipynb#load-&-save>`.
        """
        raise NotImplementedError

    def __copy__(self) -> "ExecutionEngine":
        return self

    def __deepcopy__(self, memo: Any) -> "ExecutionEngine":
        return self

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
        return self.map(df, s.run, output_schema, partition_spec, metadata)


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
