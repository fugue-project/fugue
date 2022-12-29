import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyspark
import pyspark.sql as ps
from pyspark import StorageLevel
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, lit, row_number
from pyspark.sql.window import Window
from triad import FileSystem, IndexedOrderedDict, ParamDict, Schema
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.hash import to_uuid
from triad.utils.iter import EmptyAwareIterable
from triad.utils.pandas_like import PD_UTILS
from triad.utils.threading import RunOnce
from triad import SerializableRLock

from fugue.collections.partition import (
    PartitionCursor,
    PartitionSpec,
    parse_presort_exp,
)
from fugue.constants import KEYWORD_ROWCOUNT
from fugue.dataframe import (
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    DataFrames,
    IterableDataFrame,
    LocalDataFrame,
    LocalDataFrameIterableDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.utils import get_join_schemas
from fugue.exceptions import FugueDataFrameInitError
from fugue.execution.execution_engine import ExecutionEngine, MapEngine, SQLEngine
from fugue_spark._constants import (
    FUGUE_SPARK_CONF_USE_PANDAS_UDF,
    FUGUE_SPARK_DEFAULT_CONF,
)
from fugue_spark._utils.convert import to_schema, to_spark_schema, to_type_safe_input
from fugue_spark._utils.io import SparkIO
from fugue_spark._utils.partition import (
    even_repartition,
    hash_repartition,
    rand_repartition,
)
from fugue_spark.dataframe import SparkDataFrame

_TO_SPARK_JOIN_MAP: Dict[str, str] = {
    "inner": "inner",
    "leftouter": "left_outer",
    "rightouter": "right_outer",
    "fullouter": "outer",
    "cross": "cross",
    "semi": "left_semi",
    "anti": "left_anti",
    "leftsemi": "left_semi",
    "leftanti": "left_anti",
}


class SparkSQLEngine(SQLEngine):
    """`Spark SQL <https://spark.apache.org/sql/>`_ execution implementation.

    :param execution_engine: it must be :class:`~.SparkExecutionEngine`
    :raises ValueError: if the engine is not :class:`~.SparkExecutionEngine`
    """

    def __init__(self, execution_engine: ExecutionEngine):
        assert_or_throw(
            isinstance(execution_engine, SparkExecutionEngine),
            ValueError("SparkSQLEngine must use SparkExecutionEngine"),
        )
        super().__init__(execution_engine)

    def select(self, dfs: DataFrames, statement: List[Tuple[bool, str]]) -> DataFrame:
        _map: Dict[str, str] = {}
        for k, v in dfs.items():
            df = self.execution_engine._to_spark_df(v, create_view=True)  # type: ignore
            _map[k] = df.alias
        _sql = " ".join(_map.get(p[1], p[1]) if p[0] else p[1] for p in statement)
        return SparkDataFrame(
            self.execution_engine.spark_session.sql(_sql)  # type: ignore
        )


class SparkMapEngine(MapEngine):
    def _should_use_pandas_udf(self, schema: Schema) -> bool:
        possible = hasattr(ps.DataFrame, "mapInPandas")  # must be new version of Spark
        if pyspark.__version__ < "3":  # pragma: no cover
            possible &= self.execution_engine.conf.get(
                "spark.sql.execution.arrow.enabled", False
            )
        else:
            possible &= self.execution_engine.conf.get(
                "spark.sql.execution.arrow.pyspark.enabled", False
            )
        enabled = self.execution_engine.conf.get_or_throw(
            FUGUE_SPARK_CONF_USE_PANDAS_UDF, bool
        )
        if not possible or any(pa.types.is_nested(t) for t in schema.types):
            if enabled and not possible:  # pragma: no cover
                self.execution_engine.log.warning(
                    f"{FUGUE_SPARK_CONF_USE_PANDAS_UDF}"
                    " is enabled but the current PySpark session"
                    "did not enable Pandas UDF support"
                )
            return False
        return enabled

    def map_dataframe(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        output_schema = Schema(output_schema)
        if self._should_use_pandas_udf(output_schema):
            # pandas udf can only be used for pyspark > 3
            if len(partition_spec.partition_by) > 0 and partition_spec.algo != "even":
                return self._group_map_by_pandas_udf(
                    df,
                    map_func=map_func,
                    output_schema=output_schema,
                    partition_spec=partition_spec,
                    on_init=on_init,
                )
            elif len(partition_spec.partition_by) == 0:
                return self._map_by_pandas_udf(
                    df,
                    map_func=map_func,
                    output_schema=output_schema,
                    partition_spec=partition_spec,
                    on_init=on_init,
                )
        df = self.execution_engine.to_df(
            self.execution_engine.repartition(df, partition_spec)
        )
        mapper = _Mapper(df, map_func, output_schema, partition_spec, on_init)
        sdf = df.native.rdd.mapPartitionsWithIndex(mapper.run, True)  # type: ignore
        return self.execution_engine.to_df(sdf, output_schema)

    def _group_map_by_pandas_udf(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        presort = partition_spec.presort
        presort_keys = list(presort.keys())
        presort_asc = list(presort.values())
        output_schema = Schema(output_schema)
        input_schema = df.schema
        on_init_once: Any = (
            None
            if on_init is None
            else RunOnce(
                on_init, lambda *args, **kwargs: to_uuid(id(on_init), id(args[0]))
            )
        )

        def _udf(pdf: Any) -> pd.DataFrame:  # pragma: no cover
            if pdf.shape[0] == 0:
                return PandasDataFrame([], output_schema).as_pandas()
            if len(presort_keys) > 0:
                pdf = pdf.sort_values(presort_keys, ascending=presort_asc)
            input_df = PandasDataFrame(
                pdf.reset_index(drop=True), input_schema, pandas_df_wrapper=True
            )
            if on_init_once is not None:
                on_init_once(0, input_df)
            cursor = partition_spec.get_cursor(input_schema, 0)
            cursor.set(input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_pandas()

        df = self.execution_engine.to_df(df)

        gdf = df.native.groupBy(*partition_spec.partition_by)  # type: ignore
        sdf = gdf.applyInPandas(_udf, schema=to_spark_schema(output_schema))
        return SparkDataFrame(sdf)

    def _map_by_pandas_udf(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        df = self.execution_engine.to_df(
            self.execution_engine.repartition(df, partition_spec)
        )
        output_schema = Schema(output_schema)
        input_schema = df.schema
        on_init_once: Any = (
            None
            if on_init is None
            else RunOnce(
                on_init, lambda *args, **kwargs: to_uuid(id(on_init), id(args[0]))
            )
        )

        def _udf(
            dfs: Iterable[pd.DataFrame],
        ) -> Iterable[pd.DataFrame]:  # pragma: no cover
            def get_dfs() -> Iterable[LocalDataFrame]:
                for df in dfs:
                    if df.shape[0] > 0:
                        yield PandasDataFrame(
                            df.reset_index(drop=True),
                            input_schema,
                            pandas_df_wrapper=True,
                        )

            input_df = LocalDataFrameIterableDataFrame(get_dfs(), input_schema)
            if input_df.empty:
                return PandasDataFrame([], output_schema).as_pandas()
            if on_init_once is not None:
                on_init_once(0, input_df)
            cursor = partition_spec.get_cursor(input_schema, 0)
            cursor.set(input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            if isinstance(output_df, LocalDataFrameIterableDataFrame):
                for res in output_df.native:
                    yield res.as_pandas()
            else:
                yield output_df.as_pandas()

        df = self.execution_engine.to_df(df)
        sdf = df.native.mapInPandas(  # type: ignore
            _udf, schema=to_spark_schema(output_schema)
        )
        return SparkDataFrame(sdf)


class SparkExecutionEngine(ExecutionEngine):
    """The execution engine based on :class:`~spark:pyspark.sql.SparkSession`.

    Please read |ExecutionEngineTutorial| to understand this important Fugue concept

    :param spark_session: Spark session, defaults to None to get the Spark session by
      :meth:`~spark:pyspark.sql.SparkSession.Builder.getOrCreate`
    :param conf: |ParamsLikeObject| defaults to None, read |FugueConfig| to
      learn Fugue specific options
    """

    def __init__(self, spark_session: Optional[SparkSession] = None, conf: Any = None):
        if spark_session is None:
            spark_session = SparkSession.builder.getOrCreate()
        self._spark_session = spark_session
        cf = dict(FUGUE_SPARK_DEFAULT_CONF)
        cf.update({x[0]: x[1] for x in spark_session.sparkContext.getConf().getAll()})
        cf.update(ParamDict(conf))
        super().__init__(cf)
        self._lock = SerializableRLock()
        self._fs = FileSystem()
        self._log = logging.getLogger()
        self._broadcast_func = RunOnce(
            self._broadcast, lambda *args, **kwargs: id(args[0])
        )
        self._persist_func = RunOnce(self._persist, lambda *args, **kwargs: id(args[0]))
        self._io = SparkIO(self.spark_session, self.fs)
        self._registered_dfs: Dict[str, SparkDataFrame] = {}

    def __repr__(self) -> str:
        return "SparkExecutionEngine"

    @property
    def spark_session(self) -> SparkSession:
        """
        :return: The wrapped spark session
        :rtype: :class:`spark:pyspark.sql.SparkSession`
        """
        assert_or_throw(
            self._spark_session is not None, "SparkExecutionEngine is not started"
        )
        return self._spark_session

    @property
    def log(self) -> logging.Logger:
        return self._log

    @property
    def fs(self) -> FileSystem:
        return self._fs

    def create_default_sql_engine(self) -> SQLEngine:
        return SparkSQLEngine(self)

    def create_default_map_engine(self) -> MapEngine:
        return SparkMapEngine(self)

    def get_current_parallelism(self) -> int:
        spark = self.spark_session
        e_cores = int(spark.conf.get("spark.executor.cores", "1"))
        tc = int(spark.conf.get("spark.task.cpus", "1"))
        sc = spark._jsc.sc()
        nodes = len(list(sc.statusTracker().getExecutorInfos()))
        workers = 1 if nodes <= 1 else nodes - 1
        return max(workers * (e_cores // tc), 1)

    def to_df(self, df: Any, schema: Any = None) -> SparkDataFrame:  # noqa: C901
        """Convert a data structure to :class:`~fugue_spark.dataframe.SparkDataFrame`

        :param data: :class:`~fugue.dataframe.dataframe.DataFrame`,
          :class:`spark:pyspark.sql.DataFrame`, :class:`spark:pyspark.RDD`,
          pandas DataFrame or list or iterable of arrays
        :param schema: |SchemaLikeObject| or :class:`spark:pyspark.sql.types.StructType`
          defaults to None.
        :return: engine compatible dataframe

        .. note::

            * if the input is already :class:`~fugue_spark.dataframe.SparkDataFrame`,
              it should return itself
            * For :class:`~spark:pyspark.RDD`, list or iterable of arrays,
              ``schema`` must be specified
            * When ``schema`` is not None, a potential type cast may happen to ensure
              the dataframe's schema.
            * all other methods in the engine can take arbitrary dataframes and
              call this method to convert before doing anything
        """
        return self._to_spark_df(df, schema=schema)

    def repartition(self, df: DataFrame, partition_spec: PartitionSpec) -> DataFrame:
        def _persist_and_count(df: DataFrame) -> int:
            df = self.persist(df)
            return df.count()

        df = self._to_spark_df(df)
        num_funcs = {KEYWORD_ROWCOUNT: lambda: _persist_and_count(df)}
        num = partition_spec.get_num_partitions(**num_funcs)

        if partition_spec.algo == "hash":
            sdf = hash_repartition(
                self.spark_session, df.native, num, partition_spec.partition_by
            )
        elif partition_spec.algo == "rand":
            sdf = rand_repartition(
                self.spark_session, df.native, num, partition_spec.partition_by
            )
        elif partition_spec.algo == "even":
            df = self.persist(df)
            sdf = even_repartition(
                self.spark_session, df.native, num, partition_spec.partition_by
            )
        else:  # pragma: no cover
            raise NotImplementedError(partition_spec.algo + " is not supported")
        sorts = partition_spec.get_sorts(df.schema)
        if len(sorts) > 0:
            sdf = sdf.sortWithinPartitions(
                *sorts.keys(), ascending=list(sorts.values())
            )
        return self._to_spark_df(sdf, df.schema)

    def broadcast(self, df: DataFrame) -> SparkDataFrame:
        res = self._broadcast_func(self._to_spark_df(df))
        res.reset_metadata(df.metadata)
        return res

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> SparkDataFrame:
        res = self._persist_func(
            self._to_spark_df(df), lazy=lazy, level=kwargs.get("level", None)
        )
        res.reset_metadata(df.metadata)
        return res

    def register(self, df: DataFrame, name: str) -> SparkDataFrame:
        sdf = self._to_spark_df(df)
        sdf.native.createOrReplaceTempView(name)
        return sdf

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: Optional[List[str]] = None,
    ) -> DataFrame:
        key_schema, output_schema = get_join_schemas(df1, df2, how=how, on=on)
        how = how.lower().replace("_", "").replace(" ", "")
        assert_or_throw(
            how in _TO_SPARK_JOIN_MAP,
            ValueError(f"{how} is not supported as a join type"),
        )
        how = _TO_SPARK_JOIN_MAP[how]
        d1 = self._to_spark_df(df1).native
        d2 = self._to_spark_df(df2).native
        cols = [col(n) for n in output_schema.names]
        if how == "cross":
            res = d1.crossJoin(d2).select(*cols)
        else:
            res = d1.join(d2, on=key_schema.names, how=how).select(*cols)
        return self._to_spark_df(res, output_schema)

    def union(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
    ) -> DataFrame:
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d1 = self._to_spark_df(df1).native
        d2 = self._to_spark_df(df2).native
        d = d1.union(d2)
        if distinct:
            d = d.distinct()
        return self._to_spark_df(d, df1.schema)

    def subtract(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d1 = self._to_spark_df(df1).native
        d2 = self._to_spark_df(df2).native
        if distinct:
            d: Any = d1.subtract(d2)
        else:  # pragma: no cover
            d = d1.exceptAll(d2)
        return self._to_spark_df(d, df1.schema)

    def intersect(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d1 = self._to_spark_df(df1).native
        d2 = self._to_spark_df(df2).native
        if distinct:
            d: Any = d1.intersect(d2)
        else:  # pragma: no cover
            d = d1.intersectAll(d2)
        return self._to_spark_df(d, df1.schema)

    def distinct(self, df: DataFrame) -> DataFrame:
        d = self._to_spark_df(df).native.distinct()
        return self._to_spark_df(d, df.schema)

    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: int = None,
        subset: List[str] = None,
    ) -> DataFrame:
        d = self._to_spark_df(df).native.dropna(how=how, thresh=thresh, subset=subset)
        return self._to_spark_df(d, df.schema)

    def fillna(self, df: DataFrame, value: Any, subset: List[str] = None) -> DataFrame:
        assert_or_throw(
            (not isinstance(value, list)) and (value is not None),
            ValueError("fillna value can not be a list or None"),
        )
        if isinstance(value, dict):
            assert_or_throw(
                (None not in value.values()) and (any(value.values())),
                ValueError(
                    "fillna dict can not contain None and needs at least one value"
                ),
            )
            mapping = value
        else:
            # If subset is none, apply to all columns
            subset = subset or df.schema.names
            mapping = {col: value for col in subset}
        d = self._to_spark_df(df).native.fillna(mapping)
        return self._to_spark_df(d, df.schema)

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:
        assert_or_throw(
            (n is None and frac is not None) or (n is not None and frac is None),
            ValueError("one and only one of n and frac should be set"),
        )
        if frac is not None:
            d = self._to_spark_df(df).native.sample(
                fraction=frac, withReplacement=replace, seed=seed
            )
            return self._to_spark_df(d, df.schema)
        else:
            assert_or_throw(
                seed is None,
                NotImplementedError("seed can't be set when sampling by row count"),
            )
            assert_or_throw(
                not replace,
                NotImplementedError(
                    "replacement is not supported when sampling by row count"
                ),
            )
            temp_name = "__temp_" + str(uuid4()).split("-")[-1]
            self._to_spark_df(df).native.createOrReplaceTempView(temp_name)
            d = self.spark_session.sql(
                f"SELECT * FROM {temp_name} TABLESAMPLE({n} ROWS)"
            )
            return self._to_spark_df(d, df.schema)

    def take(
        self,
        df: DataFrame,
        n: int,
        presort: str,
        na_position: str = "last",
        partition_spec: Optional[PartitionSpec] = None,
    ) -> DataFrame:
        partition_spec = partition_spec or PartitionSpec()
        assert_or_throw(
            isinstance(n, int),
            ValueError("n needs to be an integer"),
        )
        d = self._to_spark_df(df).native
        nulls_last = bool(na_position == "last")

        if presort:
            presort = parse_presort_exp(presort)
        # Use presort over partition_spec.presort if possible
        _presort: IndexedOrderedDict = presort or partition_spec.presort

        def _presort_to_col(_col: str, _asc: bool) -> Any:
            if nulls_last:
                if _asc:
                    return col(_col).asc_nulls_last()
                else:
                    return col(_col).desc_nulls_last()
            else:
                if _asc:
                    return col(_col).asc_nulls_first()
                else:
                    return col(_col).desc_nulls_first()

        # If no partition
        if len(partition_spec.partition_by) == 0:
            if len(_presort.keys()) > 0:
                d = d.orderBy(
                    [_presort_to_col(_col, _presort[_col]) for _col in _presort.keys()]
                )
            d = d.limit(n)

        # If partition exists
        else:
            w = Window.partitionBy([col(x) for x in partition_spec.partition_by])

            if len(_presort.keys()) > 0:
                w = w.orderBy(
                    [_presort_to_col(_col, _presort[_col]) for _col in _presort.keys()]
                )
            else:
                # row_number() still needs an orderBy
                w = w.orderBy(lit(1))

            d = (
                d.select(col("*"), row_number().over(w).alias("__row_number__"))
                .filter(col("__row_number__") <= n)
                .drop("__row_number__")
            )

        return self._to_spark_df(d, df.schema)

    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:
        return self._io.load_df(
            uri=path, format_hint=format_hint, columns=columns, **kwargs
        )

    def save_df(
        self,
        df: DataFrame,
        path: str,
        format_hint: Any = None,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:
        partition_spec = partition_spec or PartitionSpec()
        df = self._to_spark_df(df)
        self._io.save_df(
            df,
            uri=path,
            format_hint=format_hint,
            mode=mode,
            partition_spec=partition_spec,
            force_single=force_single,
            **kwargs,
        )

    def _broadcast(self, df: SparkDataFrame) -> SparkDataFrame:
        sdf = broadcast(df.native)
        return SparkDataFrame(sdf, df.schema)

    def _persist(self, df: SparkDataFrame, lazy: bool, level: Any) -> SparkDataFrame:
        if level is None:
            level = StorageLevel.MEMORY_AND_DISK
        if isinstance(level, str) and level in StorageLevel.__dict__:
            level = StorageLevel.__dict__[level]
        if isinstance(level, StorageLevel):
            df.native.persist()
            if not lazy:
                ct = df.count()
                self.log.info("Persist dataframe with %s, count %i", level, ct)
            return df
        raise ValueError(f"{level} is not supported persist type")  # pragma: no cover

    def _to_spark_df(  # noqa: C901
        self, df: Any, schema: Any = None, create_view: bool = False
    ) -> SparkDataFrame:
        def _to_df() -> SparkDataFrame:
            if isinstance(df, DataFrame):
                assert_or_throw(
                    schema is None,
                    ValueError("schema must be None when df is a DataFrame"),
                )
                if isinstance(df, SparkDataFrame):
                    return df
                if isinstance(df, ArrowDataFrame):
                    sdf = self.spark_session.createDataFrame(
                        df.as_array(), to_spark_schema(df.schema)
                    )
                    return SparkDataFrame(sdf, df.schema)
                if isinstance(df, (ArrayDataFrame, IterableDataFrame)):
                    adf = ArrowDataFrame(df.as_array(type_safe=False), df.schema)
                    sdf = self.spark_session.createDataFrame(
                        adf.as_array(), to_spark_schema(df.schema)
                    )
                    return SparkDataFrame(sdf, df.schema)
                if any(pa.types.is_struct(t) for t in df.schema.types):
                    sdf = self.spark_session.createDataFrame(
                        df.as_array(type_safe=True), to_spark_schema(df.schema)
                    )
                else:
                    sdf = self.spark_session.createDataFrame(
                        df.as_pandas(), to_spark_schema(df.schema)
                    )
                return SparkDataFrame(sdf, df.schema)
            if isinstance(df, ps.DataFrame):
                return SparkDataFrame(df, None if schema is None else to_schema(schema))
            if isinstance(df, RDD):
                assert_arg_not_none(schema, "schema")
                sdf = self.spark_session.createDataFrame(df, to_spark_schema(schema))
                return SparkDataFrame(sdf, to_schema(schema))
            if isinstance(df, pd.DataFrame):
                if PD_UTILS.empty(df):
                    temp_schema = to_spark_schema(PD_UTILS.to_schema(df))
                    sdf = self.spark_session.createDataFrame([], temp_schema)
                else:
                    sdf = self.spark_session.createDataFrame(df)
                return SparkDataFrame(sdf, schema)

            # use arrow dataframe here to handle nulls in int cols
            assert_or_throw(
                schema is not None, FugueDataFrameInitError("schema can't be None")
            )
            adf = ArrowDataFrame(df, to_schema(schema))
            map_pos = [i for i, t in enumerate(adf.schema.types) if pa.types.is_map(t)]
            if len(map_pos) == 0:
                sdf = self.spark_session.createDataFrame(
                    adf.as_array(), to_spark_schema(adf.schema)
                )
            else:

                def to_dict(rows: Iterable[List[Any]]) -> Iterable[List[Any]]:
                    for row in rows:
                        for p in map_pos:
                            row[p] = dict(row[p])
                        yield row

                sdf = self.spark_session.createDataFrame(
                    to_dict(adf.as_array_iterable()), to_spark_schema(adf.schema)
                )
            return SparkDataFrame(sdf, adf.schema)

        res = _to_df()
        if res is not df and isinstance(df, DataFrame) and df.has_metadata:
            res.reset_metadata(df.metadata)

        if create_view:
            with self._lock:
                if res.alias not in self._registered_dfs:
                    res.native.createOrReplaceTempView(res.alias)
                    self._registered_dfs[res.alias] = res

        return res


class _Mapper(object):  # pragma: no cover
    # pytest can't identify the coverage, but this part is fully tested
    def __init__(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]],
    ):
        super().__init__()
        self.schema = df.schema
        self.output_schema = Schema(output_schema)
        self.partition_spec = partition_spec
        self.map_func = map_func
        self.on_init = on_init

    def run(self, no: int, rows: Iterable[ps.Row]) -> Iterable[Any]:
        df = IterableDataFrame(to_type_safe_input(rows, self.schema), self.schema)
        if df.empty:  # pragma: no cover
            return
        cursor = self.partition_spec.get_cursor(self.schema, no)
        if self.on_init is not None:
            self.on_init(no, df)
        if self.partition_spec.empty:
            partitions: Iterable[Tuple[int, int, EmptyAwareIterable]] = [
                (0, 0, df.native)
            ]
        else:
            partitioner = self.partition_spec.get_partitioner(self.schema)
            partitions = partitioner.partition(df.native)
        for pn, sn, sub in partitions:
            cursor.set(sub.peek(), pn, sn)
            sub_df = IterableDataFrame(sub, self.schema)
            res = self.map_func(cursor, sub_df)
            for r in res.as_array_iterable(type_safe=True):
                yield r
