import logging
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Union

import duckdb
import pyarrow as pa
from duckdb import DuckDBPyConnection
from fugue import ArrowDataFrame, NativeExecutionEngine
from fugue.collections.partition import (
    EMPTY_PARTITION_SPEC,
    PartitionCursor,
    PartitionSpec,
)
from fugue.dataframe import (
    DataFrame,
    DataFrames,
    LocalBoundedDataFrame,
    LocalDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.utils import get_join_schemas
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    ExecutionEngine,
    SQLEngine,
)
from triad.collections.fs import FileSystem
from triad.utils.assertion import assert_or_throw

from fugue_duckdb._utils import get_temp_df_name
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_duckdb._io import DuckDBIO


class DuckDBEngine(SQLEngine):
    """DuckDB SQL backend implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def __init__(self, execution_engine: ExecutionEngine) -> None:
        super().__init__(execution_engine)
        self._cache: Dict[str, int] = {}

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        if isinstance(self.execution_engine, DuckExecutionEngine):
            return self._duck_select(dfs, statement)
        return self._other_select(dfs, statement)

    def _duck_select(self, dfs: DataFrames, statement: str) -> DataFrame:
        for k, v in dfs.items():
            tdf: Any = self.execution_engine.to_df(v)
            if k not in self._cache or self._cache[k] != id(tdf.native):
                tdf.native.create_view(k, replace=True)
                self._cache[k] = id(tdf.native)
        result = self.execution_engine.connection.query(statement)  # type: ignore
        return DuckDataFrame(result)

    def _other_select(self, dfs: DataFrames, statement: str) -> DataFrame:
        conn = duckdb.connect()
        try:
            for k, v in dfs.items():
                conn.from_arrow_table(v.as_arrow()).create_view(k)
            return ArrowDataFrame(conn.execute(statement).arrow())
        finally:
            conn.close()


class DuckExecutionEngine(ExecutionEngine):
    """The execution engine using DuckDB.
    Please read |ExecutionEngineTutorial| to understand this important Fugue concept
    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    """

    def __init__(
        self, conf: Any = None, connection: Optional[DuckDBPyConnection] = None
    ):
        super().__init__(conf)
        self._native_engine = NativeExecutionEngine(conf)
        self._con = connection or duckdb.connect()
        self._external_con = connection is not None
        self._context_lock = RLock()

    def stop(self) -> None:
        if not self._external_con:
            self._con.close()

    def __repr__(self) -> str:
        return "DuckExecutionEngine"

    @property
    def connection(self) -> DuckDBPyConnection:
        return self._con

    @property
    def log(self) -> logging.Logger:
        return self._native_engine.log

    @property
    def fs(self) -> FileSystem:
        return self._native_engine.fs

    @property
    def default_sql_engine(self) -> SQLEngine:
        return DuckDBEngine(self)

    def to_df(self, df: Any, schema: Any = None, metadata: Any = None) -> DuckDataFrame:
        if isinstance(df, DataFrame):
            assert_or_throw(
                schema is None and metadata is None,
                ValueError("schema and metadata must be None when df is a DataFrame"),
            )
            if isinstance(df, DuckDataFrame):
                return df

            if isinstance(df, PandasDataFrame) and all(
                not pa.types.is_nested(f.type) for f in df.schema.fields
            ):
                rdf = DuckDataFrame(
                    self.connection.df(df.as_pandas()), metadata=dict(df.metadata)
                )
            else:
                rdf = DuckDataFrame(
                    self.connection.from_arrow_table(df.as_arrow()),
                    metadata=dict(df.metadata),
                )
            return rdf
        tdf = ArrowDataFrame(df, schema)
        return DuckDataFrame(self.connection.from_arrow_table(tdf.native), metadata)

    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        self.log.warning("%s doesn't respect repartition", self)
        return df

    def map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        return self._native_engine.map(
            df=df,
            map_func=map_func,
            output_schema=output_schema,
            partition_spec=partition_spec,
            metadata=metadata,
            on_init=on_init,
        )

    def broadcast(self, df: DataFrame) -> DataFrame:
        return self.to_df(df)

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        if isinstance(df, DuckDataFrame):
            # materialize
            return ArrowDataFrame(df.native.arrow(), metadata=df.metadata)
        return self.to_df(df)

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: List[str] = _DEFAULT_JOIN_KEYS,
        metadata: Any = None,
    ) -> DataFrame:
        key_schema, output_schema = get_join_schemas(df1, df2, how=how, on=on)
        t1, t2, t3 = (
            get_temp_df_name(),
            get_temp_df_name(),
            get_temp_df_name(),
        )
        on_fields = " AND ".join(f"{t1}.{k}={t2}.{k}" for k in key_schema)
        join_type = self._how_to_join(how)
        if how.lower() == "cross":
            select_fields = ",".join(
                f"{t1}.{k}" if k in df1.schema else f"{t2}.{k}"
                for k in output_schema.names
            )
            sql = f"SELECT {select_fields} FROM {t1} {join_type} {t2}"
        elif how.lower() == "right_outer":
            select_fields = ",".join(
                f"{t2}.{k}" if k in df2.schema else f"{t1}.{k}"
                for k in output_schema.names
            )
            sql = (
                f"SELECT {select_fields} FROM {t2} LEFT OUTER JOIN {t1} ON {on_fields}"
            )
        elif how.lower() == "full_outer":
            select_fields = ",".join(
                f"COALESCE({t1}.{k},{t2}.{k}) AS {k}" if k in key_schema else k
                for k in output_schema.names
            )
            sql = f"SELECT {select_fields} FROM {t1} {join_type} {t2} ON {on_fields}"
        elif how.lower() in ["semi", "left_semi"]:
            keys = ",".join(key_schema.names)
            on_fields = " AND ".join(f"{t1}.{k}={t3}.{k}" for k in key_schema)
            sql = (
                f"SELECT {t1}.* FROM {t1} INNER JOIN (SELECT DISTINCT {keys} "
                f"FROM {t2}) AS {t3} ON {on_fields}"
            )
        elif how.lower() in ["anti", "left_anti"]:
            keys = ",".join(key_schema.names)
            on_fields = " AND ".join(f"{t1}.{k}={t3}.{k}" for k in key_schema)
            sql = (
                f"SELECT {t1}.* FROM {t1} LEFT OUTER JOIN "
                f"(SELECT DISTINCT {keys}, 1 AS __contain__ FROM {t2}) AS {t3} "
                f"ON {on_fields} WHERE {t3}.__contain__ IS NULL"
            )
        else:
            select_fields = ",".join(
                f"{t1}.{k}" if k in df1.schema else f"{t2}.{k}"
                for k in output_schema.names
            )
            sql = f"SELECT {select_fields} FROM {t1} {join_type} {t2} ON {on_fields}"
        return self._sql(sql, {t1: df1, t2: df2}, metadata=metadata)

    def _how_to_join(self, how: str):
        return how.upper().replace("_", " ") + " JOIN"

    def union(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
        metadata: Any = None,
    ) -> DataFrame:
        assert_or_throw(
            df1.schema == df2.schema, ValueError(f"{df1.schema} != {df2.schema}")
        )
        if distinct:
            t1, t2 = get_temp_df_name(), get_temp_df_name()
            sql = f"SELECT * FROM {t1} UNION SELECT * FROM {t2}"
            return self._sql(sql, {t1: df1, t2: df2}, metadata=metadata)
        return DuckDataFrame(
            self.to_df(df1).native.union(self.to_df(df2).native), metadata=metadata
        )

    def subtract(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        if distinct:
            t1, t2 = get_temp_df_name(), get_temp_df_name()
            sql = f"SELECT * FROM {t1} EXCEPT SELECT * FROM {t2}"
            return self._sql(sql, {t1: df1, t2: df2}, metadata=metadata)
        return DuckDataFrame(
            self.to_df(df1).native.except_(self.to_df(df2).native), metadata=metadata
        )

    def intersect(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
        metadata: Any = None,
    ) -> DataFrame:
        if distinct:
            t1, t2 = get_temp_df_name(), get_temp_df_name()
            sql = f"SELECT * FROM {t1} INTERSECT SELECT * FROM {t2}"
            return self._sql(sql, {t1: df1, t2: df2}, metadata=metadata)
        return DuckDataFrame(
            self.to_df(df1).native.intersect(self.to_df(df2).native), metadata=metadata
        )

    def distinct(
        self,
        df: DataFrame,
        metadata: Any = None,
    ) -> DataFrame:
        rel = self.to_df(df).native.distinct()
        return DuckDataFrame(rel, metadata=metadata)

    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: int = None,
        subset: List[str] = None,
        metadata: Any = None,
    ) -> DataFrame:
        return self._native_engine.dropna(df, how, thresh, subset, metadata)

    def fillna(
        self,
        df: DataFrame,
        value: Any,
        subset: List[str] = None,
        metadata: Any = None,
    ) -> DataFrame:
        return self._native_engine.fillna(df, value, subset, metadata)

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
        metadata: Any = None,
    ) -> DataFrame:
        return self._native_engine.sample(df, n, frac, replace, seed, metadata)

    def take(
        self,
        df: DataFrame,
        n: int,
        presort: str,
        na_position: str = "last",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        metadata: Any = None,
    ) -> DataFrame:
        return self._native_engine.take(
            df, n, presort, na_position, partition_spec, metadata
        )

    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> LocalBoundedDataFrame:
        dio = DuckDBIO(self.fs, self.connection)
        return dio.load_df(path, format_hint, columns, **kwargs)

    def save_df(
        self,
        df: DataFrame,
        path: str,
        format_hint: Any = None,
        mode: str = "overwrite",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:
        dio = DuckDBIO(self.fs, self.connection)
        dio.save_df(self.to_df(df), path, format_hint, mode, **kwargs)

    def _sql(
        self, sql: str, dfs: Dict[str, DataFrame], metadata: Any = None
    ) -> DuckDataFrame:
        with self._context_lock:
            df = self.sql_engine.select(DataFrames(dfs), sql)
            return DuckDataFrame(df.native, metadata=metadata)  # type: ignore
