import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from triad import Schema, SerializableRLock
from triad.collections.fs import FileSystem
from triad.utils.assertion import assert_or_throw
from triad.utils.pandas_like import PandasUtils
from triad.utils.schema import quote_name

from ..collections.partition import PartitionCursor, PartitionSpec, parse_presort_exp
from ..collections.sql import StructuredRawSQL, TempTableName
from ..dataframe import (
    AnyDataFrame,
    ArrowDataFrame,
    DataFrame,
    DataFrames,
    LocalBoundedDataFrame,
    LocalDataFrame,
    PandasDataFrame,
    as_fugue_df,
)
from ..dataframe.api import as_arrow
from ..dataframe.utils import get_join_schemas
from ..execution import ExecutionEngine, MapEngine, SQLEngine
from ..execution.factory import parse_execution_engine
from ._io import DuckDBIO
from ._utils import (
    encode_column_name,
    encode_column_names,
    encode_schema_names,
    encode_value_to_expr,
)
from .dataframe import DuckDataFrame

_FUGUE_DUCKDB_PRAGMA_CONFIG_PREFIX = "fugue.duckdb.pragma."
_FUGUE_DUCKDB_EXTENSIONS = "fugue.duckdb.extensions"


class DuckDBEngine(SQLEngine):
    """DuckDB SQL backend implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    @property
    def dialect(self) -> Optional[str]:
        return "duckdb"

    def to_df(self, df: AnyDataFrame, schema: Any = None) -> DataFrame:
        return _to_duck_df(self.execution_engine, df, schema=schema)  # type: ignore

    def select(self, dfs: DataFrames, statement: StructuredRawSQL) -> DataFrame:
        if isinstance(self.execution_engine, NativeExecutionEngine):
            return self._duck_select(dfs, statement)
        else:
            _dfs, _sql = self.encode(dfs, statement)
            return self._other_select(_dfs, _sql)

    def table_exists(self, table: str) -> bool:
        return self._get_table(table) is not None

    def save_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs: Any,
    ) -> None:
        if isinstance(self.execution_engine, NativeExecutionEngine):
            con = self.execution_engine.connection
            tdf: DuckDataFrame = _to_duck_df(
                self.execution_engine, df, create_view=False  # type: ignore
            )
            et = self._get_table(table)
            if et is not None:
                if mode == "overwrite":
                    tp = "VIEW" if et["table_type"] == "VIEW" else "TABLE"
                    tn = encode_column_name(et["table_name"])
                    con.query(f"DROP {tp} {tn}")
                else:
                    raise Exception(f"{table} exists")
            tdf.native.create(table)
        else:  # pragma: no cover
            raise NotImplementedError(
                "save_table can only be used with NativeExecutionEngine"
            )

    def load_table(self, table: str, **kwargs: Any) -> DataFrame:
        if isinstance(self.execution_engine, NativeExecutionEngine):
            return DuckDataFrame(self.execution_engine.connection.table(table))
        else:  # pragma: no cover
            raise NotImplementedError(
                "load_table can only be used with NativeExecutionEngine"
            )

    @property
    def is_distributed(self) -> bool:
        return False

    def _duck_select(self, dfs: DataFrames, statement: StructuredRawSQL) -> DataFrame:
        name_map: Dict[str, str] = {}
        for k, v in dfs.items():
            tdf: DuckDataFrame = _to_duck_df(
                self.execution_engine, v, create_view=True  # type: ignore
            )
            name_map[k] = tdf.alias
        query = statement.construct(name_map, dialect=self.dialect, log=self.log)
        result = self.execution_engine.connection.query(query)  # type: ignore
        return DuckDataFrame(result)

    def _other_select(self, dfs: DataFrames, statement: str) -> DataFrame:
        with duckdb.connect() as conn:
            for k, v in dfs.items():
                duckdb.arrow(v.as_arrow(), connection=conn).create_view(k)
            return ArrowDataFrame(conn.execute(statement).arrow())

    def _get_table(self, table: str) -> Optional[Dict[str, Any]]:
        if isinstance(self.execution_engine, NativeExecutionEngine):
            # TODO: this is over simplified
            con = self.execution_engine.connection
            qt = quote_name(table, "'")
            if not qt.startswith("'"):
                qt = "'" + qt + "'"
            tables = (
                con.query(
                    "SELECT table_catalog,table_schema,table_name,table_type"
                    f" FROM information_schema.tables WHERE table_name={qt}"
                )
                .to_df()
                .to_dict("records")
            )
            return None if len(tables) == 0 else tables[0]
        else:  # pragma: no cover
            raise NotImplementedError(
                "table_exists can only be used with NativeExecutionEngine"
            )


class PandasMapEngine(MapEngine):
    @property
    def is_distributed(self) -> bool:
        return False

    def map_dataframe(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
        map_func_format_hint: Optional[str] = None,
    ) -> DataFrame:
        # if partition_spec.num_partitions != "0":
        #     self.log.warning(
        #         "%s doesn't respect num_partitions %s",
        #         self,
        #         partition_spec.num_partitions,
        #     )
        is_coarse = partition_spec.algo == "coarse"
        presort = partition_spec.get_sorts(df.schema, with_partition_keys=is_coarse)
        presort_keys = list(presort.keys())
        presort_asc = list(presort.values())
        output_schema = Schema(output_schema)
        cursor = partition_spec.get_cursor(df.schema, 0)
        if on_init is not None:
            on_init(0, df)
        if (
            len(partition_spec.partition_by) == 0 or partition_spec.algo == "coarse"
        ):  # no partition
            if len(partition_spec.presort) > 0:
                pdf = (
                    df.as_pandas()
                    .sort_values(presort_keys, ascending=presort_asc)
                    .reset_index(drop=True)
                )
                input_df = PandasDataFrame(pdf, df.schema, pandas_df_wrapper=True)
                cursor.set(lambda: input_df.peek_array(), cursor.partition_no + 1, 0)
                output_df = map_func(cursor, input_df)
            else:
                df = df.as_local()
                cursor.set(lambda: df.peek_array(), 0, 0)
                output_df = map_func(cursor, df)
            if (
                isinstance(output_df, PandasDataFrame)
                and output_df.schema != output_schema
            ):
                output_df = PandasDataFrame(output_df.native, output_schema)
            assert_or_throw(
                output_df.schema == output_schema,
                lambda: f"map output {output_df.schema} "
                f"mismatches given {output_schema}",
            )
            return self.to_df(output_df)  # type: ignore

        def _map(pdf: pd.DataFrame) -> pd.DataFrame:
            if len(partition_spec.presort) > 0:
                pdf = pdf.sort_values(presort_keys, ascending=presort_asc).reset_index(
                    drop=True
                )
            input_df = PandasDataFrame(pdf, df.schema, pandas_df_wrapper=True)
            cursor.set(lambda: input_df.peek_array(), cursor.partition_no + 1, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_pandas()

        result = self.execution_engine.pl_utils.safe_groupby_apply(  # type: ignore
            df.as_pandas(), partition_spec.partition_by, _map
        )
        return PandasDataFrame(result, output_schema)


class NativeExecutionEngine(ExecutionEngine):
    """The execution engine using DuckD and Pandas
    Please read |ExecutionEngineTutorial| to understand this important Fugue concept

    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    :param connection: DuckDB connection
    """

    def __init__(
        self, conf: Any = None, connection: Optional[DuckDBPyConnection] = None
    ):
        super().__init__(conf)
        self._fs = FileSystem()
        self._log = logging.getLogger()
        self._con = connection or duckdb.connect()
        self._external_con = connection is not None
        self._context_lock = SerializableRLock()
        self._registered_dfs: Dict[str, DuckDataFrame] = {}

        try:
            for pg in list(self._get_pragmas()):  # transactional
                self._con.execute(pg)

            for ext in self.conf.get(_FUGUE_DUCKDB_EXTENSIONS, "").split(","):
                _ext = ext.strip()
                if _ext != "":
                    self._con.install_extension(_ext)
                    self._con.load_extension(_ext)
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

    def stop_engine(self) -> None:
        if not self._external_con:
            self._con.close()

    def __repr__(self) -> str:
        return "NativeExecutionEngine"

    @property
    def connection(self) -> DuckDBPyConnection:
        return self._con

    @property
    def pl_utils(self) -> PandasUtils:
        """Pandas-like dataframe utils"""
        return PandasUtils()

    @property
    def log(self) -> logging.Logger:
        return self._log

    @property
    def is_distributed(self) -> bool:
        return False

    @property
    def fs(self) -> FileSystem:
        return self._fs

    def create_default_sql_engine(self) -> SQLEngine:
        return DuckDBEngine(self)

    def create_default_map_engine(self) -> MapEngine:
        return PandasMapEngine(self)

    def get_current_parallelism(self) -> int:
        return 1

    def to_df(self, df: Any, schema: Any = None) -> DataFrame:
        return _to_native_execution_engine_df(df, schema)

    def repartition(self, df: DataFrame, partition_spec: PartitionSpec) -> DataFrame:
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
        if df.has_metadata:
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
            TempTableName(),
            TempTableName(),
            TempTableName(),
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
            t1, t2 = TempTableName(), TempTableName()
            sql = f"SELECT * FROM {t1} UNION SELECT * FROM {t2}"
            return self._sql(sql, {t1.key: df1, t2.key: df2})
        return DuckDataFrame(
            _to_duck_df(self, df1).native.union(_to_duck_df(self, df2).native)
        )

    def subtract(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:  # pragma: no cover
        if distinct:
            t1, t2 = TempTableName(), TempTableName()
            sql = f"SELECT * FROM {t1} EXCEPT SELECT * FROM {t2}"
            return self._sql(sql, {t1.key: df1, t2.key: df2})
        return DuckDataFrame(
            _to_duck_df(self, df1).native.except_(_to_duck_df(self, df2).native)
        )

    def intersect(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        if distinct:
            t1, t2 = TempTableName(), TempTableName()
            sql = f"SELECT * FROM {t1} INTERSECT DISTINCT SELECT * FROM {t2}"
            return self._sql(sql, {t1.key: df1, t2.key: df2})
        raise NotImplementedError(
            "DuckDB doesn't have consist behavior on INTERSECT ALL,"
            " so Fugue doesn't support it"
        )

    def distinct(self, df: DataFrame) -> DataFrame:
        rel = _to_duck_df(self, df).native.distinct()
        return DuckDataFrame(rel)

    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: int = None,
        subset: List[str] = None,
    ) -> DataFrame:
        if isinstance(df, PandasDataFrame):
            return self._pd_dropna(df, how, thresh, subset)
        return self._duck_dropna(df, how, thresh, subset)

    def fillna(self, df: DataFrame, value: Any, subset: List[str] = None) -> DataFrame:
        if isinstance(df, PandasDataFrame):
            return self._pd_fillna(df, value, subset)
        return self._duck_fillna(df, value, subset)

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:
        if isinstance(df, PandasDataFrame):
            return self._pd_sample(df, n, frac, replace, seed)
        return self._duck_sample(df, n, frac, replace, seed)

    def take(
        self,
        df: DataFrame,
        n: int,
        presort: str,
        na_position: str = "last",
        partition_spec: Optional[PartitionSpec] = None,
    ) -> DataFrame:
        if isinstance(df, PandasDataFrame):
            return self._pd_take(df, n, presort, na_position, partition_spec)
        return self._duck_take(df, n, presort, na_position, partition_spec)

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
        dio.save_df(_to_duck_df(self, df), path, format_hint, mode, **kwargs)

    def convert_yield_dataframe(self, df: DataFrame, as_local: bool) -> DataFrame:
        if as_local:
            return df.as_local()
        # self._ctx_count <= 1 means it is either not in a context or in the top
        # leve context where the engine was constructed
        return df.as_local() if self._ctx_count <= 1 and not self._external_con else df

    def _sql(self, sql: str, dfs: Dict[str, DataFrame]) -> DuckDataFrame:
        with self._context_lock:
            df = self.sql_engine.select(
                DataFrames(dfs), StructuredRawSQL.from_expr(sql, dialect=None)
            )
            return DuckDataFrame(df.native)  # type: ignore

    def _duck_dropna(
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
        return DuckDataFrame(_to_duck_df(self, df).native.filter(expr))

    def _duck_fillna(
        self, df: DataFrame, value: Any, subset: List[str] = None
    ) -> DataFrame:
        def _build_value_dict(names: List[str]) -> Dict[str, str]:
            if not isinstance(value, dict):
                v = encode_value_to_expr(value)
                return {n: v for n in names}
            else:
                return {n: encode_value_to_expr(value[n]) for n in names}

        names = df.columns
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
            for f in df.columns
        ]
        return DuckDataFrame(_to_duck_df(self, df).native.project(", ".join(cols)))

    def _duck_sample(
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
        tb = TempTableName()
        if frac is not None:
            sql = f"SELECT * FROM {tb} USING SAMPLE bernoulli({frac*100} PERCENT)"
        else:
            sql = f"SELECT * FROM {tb} USING SAMPLE reservoir({n} ROWS)"
        if seed is not None:
            sql += f" REPEATABLE ({seed})"
        return self._sql(sql, {tb.key: df})

    def _duck_take(
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
        tb = TempTableName()

        if len(_presort) == 0:
            if len(partition_spec.partition_by) == 0:
                return DuckDataFrame(_to_duck_df(self, df).native.limit(n))
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

    def _pd_dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: List[str] = None,
    ) -> DataFrame:
        kwargs: Dict[str, Any] = dict(axis=0, subset=subset, inplace=False)
        if thresh is None:
            kwargs["how"] = how
        else:
            kwargs["thresh"] = thresh
        d = df.as_pandas().dropna(**kwargs)
        return PandasDataFrame(d.reset_index(drop=True), df.schema)

    def _pd_fillna(
        self,
        df: DataFrame,
        value: Any,
        subset: List[str] = None,
    ) -> DataFrame:
        assert_or_throw(
            (not isinstance(value, list)) and (value is not None),
            ValueError("fillna value can not None or a list"),
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
            subset = subset or df.columns
            mapping = {col: value for col in subset}
        d = df.as_pandas().fillna(mapping, inplace=False)
        return PandasDataFrame(d.reset_index(drop=True), df.schema)

    def _pd_sample(
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
        d = df.as_pandas().sample(n=n, frac=frac, replace=replace, random_state=seed)
        return PandasDataFrame(d.reset_index(drop=True), df.schema)

    def _pd_take(
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
        d = df.as_pandas()

        # Use presort over partition_spec.presort if possible
        if presort is not None and presort != "":
            _presort = parse_presort_exp(presort)
        else:
            _presort = partition_spec.presort

        if len(_presort.keys()) > 0:
            d = d.sort_values(
                list(_presort.keys()),
                ascending=list(_presort.values()),
                na_position=na_position,
            )

        if len(partition_spec.partition_by) == 0:
            d = d.head(n)
        else:
            d = d.groupby(by=partition_spec.partition_by, dropna=False).head(n)

        return PandasDataFrame(
            d.reset_index(drop=True), df.schema, pandas_df_wrapper=True
        )


def _to_duck_df(
    engine: NativeExecutionEngine,
    df: Any,
    schema: Any = None,
    create_view: bool = False,
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
            rdf = DuckDataFrame(
                duckdb.arrow(df.as_arrow(), connection=engine.connection)
            )
            rdf.reset_metadata(df.metadata if df.has_metadata else None)
            return rdf
        adf = (
            as_arrow(df)
            if schema is None
            else _to_native_execution_engine_df(df, schema).as_arrow()
        )
        return DuckDataFrame(duckdb.arrow(adf, connection=engine.connection))

    res = _gen_duck()
    if create_view:
        with engine._context_lock:
            if res.alias not in engine._registered_dfs:
                res.native.create_view(res.alias, replace=True)
                # must hold the reference of the df so the id will not be reused
                engine._registered_dfs[res.alias] = res
    return res


def _to_native_execution_engine_df(df: AnyDataFrame, schema: Any = None) -> DataFrame:
    fdf = as_fugue_df(df) if schema is None else as_fugue_df(df, schema=schema)
    return fdf.as_local_bounded()


@parse_execution_engine.candidate(
    lambda engine, conf, **kwargs: engine is None
    or (isinstance(engine, str) and engine == "")
)
def _parse_native_execution_engine(
    engine: Any, conf: Any, **kwargs: Any
) -> NativeExecutionEngine:
    return NativeExecutionEngine(conf=conf)
