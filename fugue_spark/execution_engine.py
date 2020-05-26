import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pyspark.sql as ps
from fs.base import FS as FileSystem
from fs.osfs import OSFS
from fugue.collections.partition import PartitionCursor, PartitionSpec
from fugue.dataframe import (
    DataFrame,
    DataFrames,
    IterableDataFrame,
    LocalDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.utils import get_join_schemas
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    ExecutionEngine,
    SQLEngine,
)
from fugue_spark.dataframe import SparkDataFrame
from fugue_spark.utils.convert import to_schema, to_spark_schema
from pyspark import StorageLevel
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, lit
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.iter import EmptyAwareIterable
from triad.utils.threading import RunOnce

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

_NO_PARTITION_SERIALIZATION_KEY = "__no_partition_serialization_key__"


class SparkSQLEngine(SQLEngine):
    def __init__(self, execution_engine: ExecutionEngine) -> None:
        assert_or_throw(
            isinstance(execution_engine, SparkExecutionEngine),
            ValueError("SparkSQLEngine must use SparkExecutionEngine"),
        )
        return super().__init__(execution_engine)

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        for k, v in dfs.items():
            self.execution_engine.register(v, k)  # type: ignore
        return SparkDataFrame(
            self.execution_engine.spark_session.sql(statement)  # type: ignore
        )


class SparkExecutionEngine(ExecutionEngine):
    def __init__(self, spark_session: SparkSession, conf: Any = None):
        self._spark_session = spark_session
        cf = {x[0]: x[1] for x in spark_session.sparkContext.getConf().getAll()}
        cf.update(ParamDict(conf))
        super().__init__(cf)
        self._fs = OSFS("/")  # TODO: this is not right
        self._log = logging.getLogger()
        self._default_sql_engine = SparkSQLEngine(self)
        self._broadcast_func = RunOnce(
            self._broadcast, lambda *args, **kwargs: id(args[0])
        )
        self._persist_func = RunOnce(self._persist, lambda *args, **kwargs: id(args[0]))
        self._register_func = RunOnce(
            self._register, lambda *args, **kwargs: id(args[0])
        )

    def __repr__(self) -> str:
        return "SparkExecutionEngine"

    @property
    def spark_session(self) -> SparkSession:
        return self._spark_session

    @property
    def log(self) -> logging.Logger:
        return self._log

    @property
    def fs(self) -> FileSystem:
        return self._fs

    @property
    def default_sql_engine(self) -> SQLEngine:
        return self._default_sql_engine

    def stop(self) -> None:
        # TODO: we need a conf to control whether to stop session
        # self.spark_session.stop()
        pass

    def to_df(
        self, df: Any, schema: Any = None, metadata: Any = None
    ) -> SparkDataFrame:
        if isinstance(df, DataFrame):
            assert_or_throw(
                schema is None and metadata is None,
                ValueError("schema and metadata must be None when df is a DataFrame"),
            )
            if isinstance(df, SparkDataFrame):
                return df
            sdf = self.spark_session.createDataFrame(
                df.as_pandas(), to_spark_schema(df.schema)
            )
            return SparkDataFrame(sdf, df.schema, metadata)
        if isinstance(df, ps.DataFrame):
            return SparkDataFrame(
                df, None if schema is None else to_schema(schema), metadata
            )
        if isinstance(df, RDD):
            assert_arg_not_none(schema, "schema")
            sdf = self.spark_session.createDataFrame(df, to_spark_schema(schema))
            return SparkDataFrame(sdf, to_schema(schema), metadata)
        pdf = PandasDataFrame(df, to_schema(schema))
        sdf = self.spark_session.createDataFrame(
            pdf.as_pandas(), to_spark_schema(pdf.schema)
        )
        return SparkDataFrame(sdf, pdf.schema, metadata)

    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        df = self.to_df(df)
        sdf = df.native
        if len(partition_spec.partition_by) > 0:
            sdf = sdf.repartition(*partition_spec.partition_by)
        sorts = partition_spec.get_sorts(df.schema)
        if len(sorts) > 0:
            sdf = sdf.sortWithinPartitions(
                *sorts.keys(), ascending=list(sorts.values())
            )
        return self.to_df(sdf, df.schema, df.metadata)

    def map(
        self,
        df: DataFrame,
        mapFunc: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        df = self.to_df(self.repartition(df, partition_spec))
        mapper = _Mapper(df, mapFunc, partition_spec, on_init)
        sdf = df.native.rdd.mapPartitionsWithIndex(mapper.run, True)
        return self.to_df(sdf, output_schema, metadata)

    def serialize_by_partition(
        self,
        df: DataFrame,
        partition_spec: PartitionSpec,
        df_name: str,
        temp_path: Optional[str] = None,
        to_file_threshold: int = -1,
    ) -> DataFrame:
        if any(k in df.schema for k in partition_spec.partition_by):
            return super().serialize_by_partition(
                df=df,
                partition_spec=partition_spec,
                df_name=df_name,
                temp_path=temp_path,
                to_file_threshold=to_file_threshold,
            )
        presort = list(
            filter(lambda p: p[0] in df.schema, partition_spec.presort.items())
        )
        sdf = self.to_df(df).native
        sdf = sdf.withColumn(_NO_PARTITION_SERIALIZATION_KEY, lit(0))
        tdf = self.repartition(
            self.to_df(sdf),
            PartitionSpec(
                partition_spec, by=[_NO_PARTITION_SERIALIZATION_KEY], presort=presort
            ),
        ).drop([_NO_PARTITION_SERIALIZATION_KEY])
        return super().serialize_by_partition(
            df=tdf,
            partition_spec=PartitionSpec(),
            df_name=df_name,
            temp_path=temp_path,
            to_file_threshold=to_file_threshold,
        )

    def broadcast(self, df: DataFrame) -> DataFrame:
        return self._broadcast_func(self.to_df(df))

    def persist(self, df: DataFrame, level: Any = None) -> DataFrame:
        return self._persist_func(self.to_df(df), level)

    def register(self, df: DataFrame, name: str) -> DataFrame:
        return self._register_func(self.to_df(df), name)

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: List[str] = _DEFAULT_JOIN_KEYS,
        metadata: Any = None,
    ) -> DataFrame:
        key_schema, output_schema = get_join_schemas(df1, df2, how=how, on=on)
        how = how.lower().replace("_", "").replace(" ", "")
        assert_or_throw(
            how in _TO_SPARK_JOIN_MAP,
            ValueError(f"{how} is not supported as a join type"),
        )
        how = _TO_SPARK_JOIN_MAP[how]
        d1 = self.to_df(df1).native
        d2 = self.to_df(df2).native
        cols = [col(n) for n in output_schema.names]
        if how == "cross":
            res = d1.crossJoin(d2).select(*cols)
        else:
            res = d1.join(d2, on=key_schema.names, how=how).select(*cols)
        return self.to_df(res, output_schema, metadata)

    def _broadcast(self, df: SparkDataFrame) -> SparkDataFrame:
        sdf = broadcast(df.native)
        return SparkDataFrame(sdf, df.schema, df.metadata)

    def _persist(self, df: SparkDataFrame, level: Any) -> SparkDataFrame:
        if level is None:
            level = StorageLevel.MEMORY_AND_DISK
        if isinstance(level, str) and level in StorageLevel.__dict__:
            level = StorageLevel.__dict__[level]
        if isinstance(level, StorageLevel):
            df.native.persist()
            self.log.info(f"Persist dataframe with {level}, count {df.count()}")
            return df
        raise NotImplementedError(f"{level} is not supported persist type")

    def _register(self, df: SparkDataFrame, name: str) -> SparkDataFrame:
        df.native.createOrReplaceTempView(name)
        return df


class _Mapper(object):
    def __init__(
        self,
        df: DataFrame,
        mapFunc: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]],
    ):
        super().__init__()
        self.schema = df.schema
        self.metadata = df.metadata
        self.partition_spec = partition_spec
        self.mapFunc = mapFunc
        self.on_init = on_init

    def run(self, no: int, rows: Iterable[ps.Row]) -> Iterable[Any]:
        df = IterableDataFrame(rows, self.schema, self.metadata)
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
            sub_df._metadata = self.metadata
            res = self.mapFunc(cursor, sub_df)
            for r in res.as_array_iterable(type_safe=True):
                yield r
