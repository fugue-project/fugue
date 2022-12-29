import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import duckdb
import pyarrow as pa
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from triad import SerializableRLock
from triad.collections.fs import FileSystem
from triad.utils.assertion import assert_or_throw

from fugue import (
    ArrowDataFrame,
    ExecutionEngine,
    MapEngine,
    NativeExecutionEngine,
    PandasMapEngine,
    SQLEngine,
)
from fugue._utils.sql import get_temp_tb_name, parse_sql
from fugue.collections.partition import PartitionSpec, parse_presort_exp
from fugue.dataframe import (
    DataFrame,
    DataFrames,
    LocalBoundedDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.utils import get_join_schemas

from ._io import DuckDBIO
from ._utils import (
    encode_column_name,
    encode_column_names,
    encode_schema_names,
    encode_value_to_expr,
)
from .dataframe import DuckDataFrame

_FUGUE_DUCKDB_PRAGMA_CONFIG_PREFIX = "fugue.duckdb.pragma."


class DuckDBEngine(SQLEngine):
    """DuckDB SQL backend implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def select(self, dfs: DataFrames, statement: List[Tuple[bool, str]]) -> DataFrame:
        if isinstance(self.execution_engine, DuckExecutionEngine):
            return self._duck_select(dfs, statement)
        else:
            _dfs, _sql = self.encode(dfs, statement)
            return self._other_select(_dfs, _sql)

    def _duck_select(
        self, dfs: DataFrames, statement: List[Tuple[bool, str]]
    ) -> DataFrame:
        name_map: Dict[str, str] = {}
        for k, v in dfs.items():
            tdf: DuckDataFrame = self.execution_engine._to_duck_df(  # type: ignore
                v, create_view=True
            )
            name_map[k] = tdf.alias
        query = " ".join(name_map.get(p[1], p[1]) if p[0] else p[1] for p in statement)
        result = self.execution_engine.connection.query(query)  # type: ignore
        return DuckDataFrame(result)

    def _other_select(self, dfs: DataFrames, statement: str) -> DataFrame:
        conn = duckdb.connect()
        try:
            for k, v in dfs.items():
                duckdb.arrow(v.as_arrow(), connection=conn).create_view(k)
            return ArrowDataFrame(conn.execute(statement).arrow())
        finally:
            conn.close()


class DuckExecutionEngine(ExecutionEngine):
    """The execution engine using DuckDB.
    Please read |ExecutionEngineTutorial| to understand this important Fugue concept

    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    :param connection: DuckDB connection
    """

    def __init__(
        self, conf: Any = None, connection: Optional[DuckDBPyConnection] = None
    ):
        super().__init__(conf)
        self._native_engine = NativeExecutionEngine(conf)
        self._con = connection or duckdb.connect()
        self._external_con = connection is not None
        self._context_lock = SerializableRLock()
        self._registered_dfs: Dict[str, DuckDataFrame] = {}

        try:
            for pg in list(self._get_pragmas()):  # transactional
                self._con.execute(pg)
        except Exception:
            self.stop()
            raise

    def _get_pragmas(self) -> Iterable[str]:
        for k, v in self.conf.items():
            if k.startswith(_FUGUE_DUCKDB_PRAGMA_CONFIG_PREFIX):
                name = k[len(_FUGUE_DUCKDB_PRAGMA_CONFIG_PREFIX) :]
                assert_or_throw(
                    name.isidentifier(), ValueError(f"{name} is not a valid pragma key")
                )
                value = encode_value_to_expr(v)
                yield f"PRAGMA {name}={value};"

    def stop(self) -> None:
        if not self._external_con:
            self._con.close()
        super().stop()

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

    def create_default_sql_engine(self) -> SQLEngine:
        return DuckDBEngine(self)

    def create_default_map_engine(self) -> MapEngine:
        return PandasMapEngine(self._native_engine)

    def get_current_parallelism(self) -> int:
        return 1

    def to_df(self, df: Any, schema: Any = None) -> DataFrame:
        return self._to_duck_df(df, schema=schema)

    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        self.log.warning("%s doesn't respect repartition", self)
        return df

    def broadcast(self, df: DataFrame) -> DataFrame:
        return df

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        # TODO: we should create DuckDB table, but it has bugs, so can't use by 0.3.1
        if isinstance(df, DuckDataFrame):
            # materialize
            res: DataFrame = ArrowDataFrame(df.native.arrow())
        else:
            res = self.to_df(df)
        res.reset_metadata(df.metadata)
        return res

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: Optional[List[str]] = None,
    ) -> DataFrame:
        key_schema, output_schema = get_join_schemas(df1, df2, how=how, on=on)
        t1, t2, t3 = (
            get_temp_tb_name(),
            get_temp_tb_name(),
            get_temp_tb_name(),
        )
        on_fields = " AND ".join(
            f"{t1}.{encode_column_name(k)}={t2}.{encode_column_name(k)}"
            for k in key_schema
        )
        join_type = self._how_to_join(how)
        if how.lower() == "cross":
            select_fields = ",".join(
                f"{t1}.{encode_column_name(k)}"
                if k in df1.schema
                else f"{t2}.{encode_column_name(k)}"
                for k in output_schema.names
            )
            sql = f"SELECT {select_fields} FROM {t1} {join_type} {t2}"
        elif how.lower() == "right_outer":
            select_fields = ",".join(
                f"{t2}.{encode_column_name(k)}"
                if k in df2.schema
                else f"{t1}.{encode_column_name(k)}"
                for k in output_schema.names
            )
            sql = (
                f"SELECT {select_fields} FROM {t2} LEFT OUTER JOIN {t1} ON {on_fields}"
            )
        elif how.lower() == "full_outer":
            select_fields = ",".join(
                f"COALESCE({t1}.{encode_column_name(k)},{t2}.{encode_column_name(k)}) "
                f"AS {encode_column_name(k)}"
                if k in key_schema
                else encode_column_name(k)
                for k in output_schema.names
            )
            sql = f"SELECT {select_fields} FROM {t1} {join_type} {t2} ON {on_fields}"
        elif how.lower() in ["semi", "left_semi"]:
            keys = ",".join(encode_schema_names(key_schema))
            on_fields = " AND ".join(
                f"{t1}.{encode_column_name(k)}={t3}.{encode_column_name(k)}"
                for k in key_schema
            )
            sql = (
                f"SELECT {t1}.* FROM {t1} INNER JOIN (SELECT DISTINCT {keys} "
                f"FROM {t2}) AS {t3} ON {on_fields}"
            )
        elif how.lower() in ["anti", "left_anti"]:
            keys = ",".join(encode_schema_names(key_schema))
            on_fields = " AND ".join(
                f"{t1}.{encode_column_name(k)}={t3}.{encode_column_name(k)}"
                for k in key_schema
            )
            sql = (
                f"SELECT {t1}.* FROM {t1} LEFT OUTER JOIN "
                f"(SELECT DISTINCT {keys}, 1 AS __contain__ FROM {t2}) AS {t3} "
                f"ON {on_fields} WHERE {t3}.__contain__ IS NULL"
            )
        else:
            select_fields = ",".join(
                f"{t1}.{encode_column_name(k)}"
                if k in df1.schema
                else f"{t2}.{encode_column_name(k)}"
                for k in output_schema.names
            )
            sql = f"SELECT {select_fields} FROM {t1} {join_type} {t2} ON {on_fields}"
        return self._sql(sql, {t1.key: df1, t2.key: df2})

    def _how_to_join(self, how: str):
        return how.upper().replace("_", " ") + " JOIN"

    def union(self, df1: DataFrame, df2: DataFrame, distinct: bool = True) -> DataFrame:
        assert_or_throw(
            df1.schema == df2.schema, ValueError(f"{df1.schema} != {df2.schema}")
        )
        if distinct:
            t1, t2 = get_temp_tb_name(), get_temp_tb_name()
            sql = f"SELECT * FROM {t1} UNION SELECT * FROM {t2}"
            return self._sql(sql, {t1.key: df1, t2.key: df2})
        return DuckDataFrame(
            self._to_duck_df(df1).native.union(self._to_duck_df(df2).native)
        )

    def subtract(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:  # pragma: no cover
        if distinct:
            t1, t2 = get_temp_tb_name(), get_temp_tb_name()
            sql = f"SELECT * FROM {t1} EXCEPT SELECT * FROM {t2}"
            return self._sql(sql, {t1.key: df1, t2.key: df2})
        return DuckDataFrame(
            self._to_duck_df(df1).native.except_(self._to_duck_df(df2).native)
        )

    def intersect(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        if distinct:
            t1, t2 = get_temp_tb_name(), get_temp_tb_name()
            sql = f"SELECT * FROM {t1} INTERSECT DISTINCT SELECT * FROM {t2}"
            return self._sql(sql, {t1.key: df1, t2.key: df2})
        raise NotImplementedError(
            "DuckDB doesn't have consist behavior on INTERSECT ALL,"
            " so Fugue doesn't support it"
        )

    def distinct(self, df: DataFrame) -> DataFrame:
        rel = self._to_duck_df(df).native.distinct()
        return DuckDataFrame(rel)

    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: int = None,
        subset: List[str] = None,
    ) -> DataFrame:
        schema = df.schema
        if subset is not None:
            schema = schema.extract(subset)
        if how == "all":
            thr = 0
        elif how == "any":
            thr = thresh or len(schema)
        else:  # pragma: no cover
            raise ValueError(f"{how} is not one of any and all")
        cw = [
            f"CASE WHEN {encode_column_name(f)} IS NULL THEN 0 ELSE 1 END"
            for f in schema.names
        ]
        expr = " + ".join(cw) + f" >= {thr}"
        return DuckDataFrame(self._to_duck_df(df).native.filter(expr))

    def fillna(self, df: DataFrame, value: Any, subset: List[str] = None) -> DataFrame:
        def _build_value_dict(names: List[str]) -> Dict[str, str]:
            if not isinstance(value, dict):
                v = encode_value_to_expr(value)
                return {n: v for n in names}
            else:
                return {n: encode_value_to_expr(value[n]) for n in names}

        names = list(df.schema.names)
        if isinstance(value, dict):
            # subset should be ignored
            names = list(value.keys())
        elif subset is not None:
            names = list(df.schema.extract(subset).names)
        vd = _build_value_dict(names)
        assert_or_throw(
            all(v != "NULL" for v in vd.values()),
            ValueError("fillna value can not be None or contain None"),
        )
        cols = [
            f"COALESCE({encode_column_name(f)}, {vd[f]}) AS {encode_column_name(f)}"
            if f in names
            else encode_column_name(f)
            for f in df.schema.names
        ]
        return DuckDataFrame(self._to_duck_df(df).native.project(", ".join(cols)))

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:
        assert_or_throw(
            (n is None and frac is not None and frac >= 0.0)
            or (frac is None and n is not None and n >= 0),
            ValueError(
                f"one and only one of n and frac should be non-negative, {n}, {frac}"
            ),
        )
        tb = get_temp_tb_name()
        if frac is not None:
            sql = f"SELECT * FROM {tb} USING SAMPLE bernoulli({frac*100} PERCENT)"
        else:
            sql = f"SELECT * FROM {tb} USING SAMPLE reservoir({n} ROWS)"
        if seed is not None:
            sql += f" REPEATABLE ({seed})"
        return self._sql(sql, {tb.key: df})

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

        if presort is not None and presort != "":
            _presort = parse_presort_exp(presort)
        else:
            _presort = partition_spec.presort
        tb = get_temp_tb_name()

        if len(_presort) == 0:
            if len(partition_spec.partition_by) == 0:
                return DuckDataFrame(self._to_duck_df(df).native.limit(n))
            cols = ", ".join(encode_schema_names(df.schema))
            pcols = ", ".join(encode_column_names(partition_spec.partition_by))
            sql = (
                f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pcols}) "
                f"AS __fugue_take_param FROM {tb}"
            )
            sql = f"SELECT {cols} FROM ({sql}) WHERE __fugue_take_param<={n}"
            return self._sql(sql, {tb.key: df})

        sorts: List[str] = []
        for k, v in _presort.items():
            s = encode_column_name(k)
            if not v:
                s += " DESC"
            s += " NULLS FIRST" if na_position == "first" else " NULLS LAST"
            sorts.append(s)
        sort_expr = "ORDER BY " + ", ".join(sorts)

        if len(partition_spec.partition_by) == 0:
            sql = f"SELECT * FROM {tb} {sort_expr} LIMIT {n}"
            return self._sql(sql, {tb.key: df})

        cols = ", ".join(encode_schema_names(df.schema))
        pcols = ", ".join(encode_column_names(partition_spec.partition_by))
        sql = (
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pcols} {sort_expr}) "
            f"AS __fugue_take_param FROM {tb}"
        )
        sql = f"SELECT {cols} FROM ({sql}) WHERE __fugue_take_param<={n}"
        return self._sql(sql, {tb.key: df})

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
        partition_spec: Optional[PartitionSpec] = None,
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:
        partition_spec = partition_spec or PartitionSpec()
        if not partition_spec.empty and not force_single:
            kwargs["partition_cols"] = partition_spec.partition_by
        dio = DuckDBIO(self.fs, self.connection)
        dio.save_df(self._to_duck_df(df), path, format_hint, mode, **kwargs)

    def convert_yield_dataframe(self, df: DataFrame, as_local: bool) -> DataFrame:
        if as_local:
            return df.as_local()
        return df.as_local() if not self.in_context and not self._external_con else df

    def _sql(self, sql: str, dfs: Dict[str, DataFrame]) -> DuckDataFrame:
        with self._context_lock:
            df = self.sql_engine.select(DataFrames(dfs), list(parse_sql(sql)))
            return DuckDataFrame(df.native)  # type: ignore

    def _to_duck_df(
        self, df: Any, schema: Any = None, create_view: bool = False
    ) -> DuckDataFrame:
        def _gen_duck() -> DuckDataFrame:
            if isinstance(df, DuckDBPyRelation):
                assert_or_throw(
                    schema is None,
                    ValueError("schema must be None when df is a DuckDBPyRelation"),
                )
                return DuckDataFrame(df)
            if isinstance(df, DataFrame):
                assert_or_throw(
                    schema is None,
                    ValueError("schema must be None when df is a DataFrame"),
                )
                if isinstance(df, DuckDataFrame):
                    return df

                if isinstance(df, PandasDataFrame) and all(
                    not pa.types.is_nested(f.type) for f in df.schema.fields
                ):
                    rdf = DuckDataFrame(self.connection.from_df(df.as_pandas()))
                else:
                    rdf = DuckDataFrame(
                        duckdb.arrow(df.as_arrow(), connection=self.connection)
                    )
                rdf.reset_metadata(df.metadata if df.has_metadata else None)
                return rdf
            tdf = ArrowDataFrame(df, schema)
            return DuckDataFrame(duckdb.arrow(tdf.native, connection=self.connection))

        res = _gen_duck()
        if create_view:
            with self._context_lock:
                if res.alias not in self._registered_dfs:
                    res.native.create_view(res.alias, replace=True)
                    # must hold the reference of the df so the id will not be reused
                    self._registered_dfs[res.alias] = res
        return res
