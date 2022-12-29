import itertools
from typing import Any, Dict, List, Optional, Tuple

import ibis
from ibis import BaseBackend
from triad.utils.assertion import assert_or_throw

from fugue.collections.partition import (
    PartitionSpec,
    parse_presort_exp,
)
from fugue.dataframe import DataFrame, DataFrames
from fugue.dataframe.utils import get_join_schemas
from fugue.execution.execution_engine import ExecutionEngine, SQLEngine

from ._compat import IbisTable
from .dataframe import IbisDataFrame

_JOIN_RIGHT_SUFFIX = "_ibis_y__"
_GEN_TABLE_NAMES = (f"_fugue_temp_table_{i:d}" for i in itertools.count())


class IbisSQLEngine(SQLEngine):
    """Ibis SQL backend base implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def __init__(self, execution_engine: ExecutionEngine) -> None:
        assert_or_throw(
            isinstance(execution_engine, IbisExecutionEngine)
            and isinstance(
                execution_engine.backend, ibis.backends.base.sql.BaseSQLBackend
            )
        )
        super().__init__(execution_engine)
        self._ibis_engine: IbisExecutionEngine = execution_engine  # type: ignore

    def select(self, dfs: DataFrames, statement: List[Tuple[bool, str]]) -> DataFrame:
        return self._ibis_engine._to_ibis_dataframe(
            self._ibis_engine._raw_select(" ".join(x[1] for x in statement), dfs)
        )


class IbisExecutionEngine(ExecutionEngine):
    """The base execution engine using Ibis.
    Please read |ExecutionEngineTutorial| to understand this important Fugue concept

    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    """

    def create_default_sql_engine(self) -> SQLEngine:
        return IbisSQLEngine(self)

    def get_current_parallelism(self) -> int:
        return 1

    @property
    def backend(self) -> BaseBackend:  # pragma: no cover
        raise NotImplementedError

    def encode_column_name(self, name: str) -> str:  # pragma: no cover
        raise NotImplementedError

    def get_temp_table_name(self) -> str:
        return next(_GEN_TABLE_NAMES)

    def _to_ibis_dataframe(
        self, df: Any, schema: Any = None
    ) -> IbisDataFrame:  # pragma: no cover
        raise NotImplementedError

    def _compile_sql(self, df: IbisDataFrame) -> str:
        return str(df.native.compile())

    def to_df(self, df: Any, schema: Any = None) -> DataFrame:
        return self._to_ibis_dataframe(df, schema=schema)

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: Optional[List[str]] = None,
    ) -> DataFrame:
        _df1 = self._to_ibis_dataframe(df1)
        _df2 = self._to_ibis_dataframe(df2)
        key_schema, end_schema = get_join_schemas(_df1, _df2, how=how, on=on)
        on_fields = [_df1.native[k] == _df2.native[k] for k in key_schema]
        if how.lower() == "cross":
            tb = _df1.native.cross_join(_df2.native, suffixes=("", _JOIN_RIGHT_SUFFIX))
        elif how.lower() == "right_outer":
            tb = _df2.native.left_join(
                _df1.native, on_fields, suffixes=("", _JOIN_RIGHT_SUFFIX)
            )
        elif how.lower() == "left_outer":
            tb = _df1.native.left_join(
                _df2.native, on_fields, suffixes=("", _JOIN_RIGHT_SUFFIX)
            )
        elif how.lower() == "full_outer":
            tb = _df1.native.outer_join(
                _df2.native, on_fields, suffixes=("", _JOIN_RIGHT_SUFFIX)
            )
            cols: List[Any] = []
            for k in end_schema.names:
                if k not in key_schema:
                    cols.append(k)
                else:
                    cols.append(
                        ibis.coalesce(tb[k], tb[k + _JOIN_RIGHT_SUFFIX]).name(k)
                    )
            tb = tb[cols]
        elif how.lower() in ["semi", "left_semi"]:
            tb = _df1.native.semi_join(
                _df2.native, on_fields, suffixes=("", _JOIN_RIGHT_SUFFIX)
            )
        elif how.lower() in ["anti", "left_anti"]:
            tb = _df1.native.anti_join(
                _df2.native, on_fields, suffixes=("", _JOIN_RIGHT_SUFFIX)
            )
        else:
            tb = _df1.native.inner_join(
                _df2.native, on_fields, suffixes=("", _JOIN_RIGHT_SUFFIX)
            )
        return self._to_ibis_dataframe(tb[end_schema.names], schema=end_schema)

    def union(self, df1: DataFrame, df2: DataFrame, distinct: bool = True) -> DataFrame:
        assert_or_throw(
            df1.schema == df2.schema, ValueError(f"{df1.schema} != {df2.schema}")
        )
        _df1 = self._to_ibis_dataframe(df1)
        _df2 = self._to_ibis_dataframe(df2)
        tb = _df1.native.union(_df2.native, distinct=distinct)
        return self._to_ibis_dataframe(tb, _df1.schema)

    def subtract(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        _df1 = self._to_ibis_dataframe(df1)
        _df2 = self._to_ibis_dataframe(df2)
        tb = _df1.native.difference(_df2.native, distinct=distinct)
        return self._to_ibis_dataframe(tb, _df1.schema)

    def intersect(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        _df1 = self._to_ibis_dataframe(df1)
        _df2 = self._to_ibis_dataframe(df2)
        tb = _df1.native.intersect(_df2.native, distinct=distinct)
        return self._to_ibis_dataframe(tb, _df1.schema)

    def distinct(self, df: DataFrame) -> DataFrame:
        _df = self._to_ibis_dataframe(df)
        tb = _df.native.distinct()
        return self._to_ibis_dataframe(tb, _df.schema)

    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: int = None,
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        schema = df.schema
        if subset is not None:
            schema = schema.extract(subset)
        _df = self._to_ibis_dataframe(df)
        if thresh is None:
            tb = _df.native.dropna(subset=subset, how=how)
            return self._to_ibis_dataframe(tb, _df.schema)
        assert_or_throw(
            how == "any", ValueError("when thresh is set, how must be 'any'")
        )
        sm = None
        for col in schema.names:
            expr = _df.native[col].isnull().ifelse(0, 1)
            if sm is None:
                sm = expr
            else:
                sm = sm + expr
        tb = _df.native.filter(sm >= ibis.literal(thresh))
        return self._to_ibis_dataframe(tb, _df.schema)

    def fillna(self, df: DataFrame, value: Any, subset: List[str] = None) -> DataFrame:
        def _build_value_dict(names: List[str]) -> Dict[str, str]:
            if not isinstance(value, dict):
                return {n: value for n in names}
            else:
                return {n: value[n] for n in names}

        names = list(df.schema.names)
        if isinstance(value, dict):
            # subset should be ignored
            names = list(value.keys())
        elif subset is not None:
            names = list(df.schema.extract(subset).names)
        vd = _build_value_dict(names)
        assert_or_throw(
            all(v is not None for v in vd.values()),
            ValueError("fillna value can not be None or contain None"),
        )
        tb = self._to_ibis_dataframe(df).native
        cols = [
            ibis.coalesce(tb[f], ibis.literal(vd[f])).name(f) if f in names else tb[f]
            for f in df.schema.names
        ]
        return self._to_ibis_dataframe(tb[cols], schema=df.schema)

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
        tbn = "_temp"
        idf = self._to_ibis_dataframe(df)

        if len(_presort) == 0:
            if len(partition_spec.partition_by) == 0:
                return self._to_ibis_dataframe(idf.native.head(n), schema=df.schema)
            pcols = ", ".join(
                self.encode_column_name(x) for x in partition_spec.partition_by
            )
            sql = (
                f"SELECT * FROM ("
                f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pcols}) "
                f"AS __fugue_take_param FROM {tbn}"
                f") WHERE __fugue_take_param<={n}"
            )
            tb = self._raw_select(sql, {tbn: idf})
            return self._to_ibis_dataframe(tb[df.schema.names], schema=df.schema)

        sorts: List[str] = []
        for k, v in _presort.items():
            s = self.encode_column_name(k)
            s += " ASC" if v else " DESC"
            s += " NULLS FIRST" if na_position == "first" else " NULLS LAST"
            sorts.append(s)
        sort_expr = "ORDER BY " + ", ".join(sorts)

        if len(partition_spec.partition_by) == 0:
            sql = f"SELECT * FROM {tbn} {sort_expr} LIMIT {n}"
            tb = self._raw_select(sql, {tbn: idf})
            return self._to_ibis_dataframe(tb[df.schema.names], schema=df.schema)

        pcols = ", ".join(
            self.encode_column_name(x) for x in partition_spec.partition_by
        )
        sql = (
            f"SELECT * FROM ("
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pcols} {sort_expr}) "
            f"AS __fugue_take_param FROM {tbn}"
            f") WHERE __fugue_take_param<={n}"
        )
        tb = self._raw_select(sql, {tbn: idf})
        return self._to_ibis_dataframe(tb[df.schema.names], schema=df.schema)

    def _raw_select(self, statement: str, dfs: Dict[str, Any]) -> IbisTable:
        cte: List[str] = []
        for k, v in dfs.items():
            idf = self._to_ibis_dataframe(v)
            cte.append(k + " AS (" + self._compile_sql(idf) + ")")
        if len(cte) > 0:
            sql = "WITH " + ",\n".join(cte) + "\n" + statement
        else:
            sql = statement
        return self.backend.sql(sql)
