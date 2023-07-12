import itertools
import logging
from abc import abstractmethod
from typing import Any, Callable, Dict, List, Optional, Type

import ibis
from ibis import BaseBackend
from triad import FileSystem, assert_or_throw

from fugue import StructuredRawSQL
from fugue.bag import Bag, LocalBag
from fugue.collections.partition import (
    BagPartitionCursor,
    PartitionCursor,
    PartitionSpec,
    parse_presort_exp,
)
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.dataframe.utils import get_join_schemas
from fugue.execution.execution_engine import ExecutionEngine, MapEngine, SQLEngine

from ._compat import IbisTable
from ._utils import to_ibis_schema
from .dataframe import IbisDataFrame

_JOIN_RIGHT_SUFFIX = "_ibis_y__"
_GEN_TABLE_NAMES = (f"_fugue_temp_table_{i:d}" for i in itertools.count())


class IbisSQLEngine(SQLEngine):
    """Ibis SQL backend base implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    @property
    @abstractmethod
    def backend(self) -> BaseBackend:  # pragma: no cover
        raise NotImplementedError

    def select(self, dfs: DataFrames, statement: StructuredRawSQL) -> DataFrame:
        return self.to_df(
            self.query_to_table(
                statement.construct(dialect=self.dialect, log=self.log),
                dfs,
            )
        )

    def query_to_table(self, statement: str, dfs: Dict[str, Any]) -> IbisTable:
        cte: List[str] = []
        for k, v in dfs.items():
            idf = self.to_df(v)
            cte.append(k + " AS (" + idf.to_sql() + ")")  # type: ignore
        if len(cte) > 0:
            sql = "WITH " + ",\n".join(cte) + "\n" + statement
        else:
            sql = statement
        return self.backend.sql(sql)

    @abstractmethod
    def encode_column_name(self, name: str) -> str:  # pragma: no cover
        raise NotImplementedError

    def get_temp_table_name(self) -> str:
        return next(_GEN_TABLE_NAMES)

    @abstractmethod
    def persist(
        self, df: DataFrame, lazy: bool = False, **kwargs: Any
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: Optional[List[str]] = None,
    ) -> DataFrame:
        _df1 = self.to_df(df1)
        _df2 = self.to_df(df2)
        key_schema, end_schema = get_join_schemas(_df1, _df2, how=how, on=on)
        on_fields = [_df1.native[k] == _df2.native[k] for k in key_schema]
        if ibis.__version__ < "6":
            suffixes: Dict[str, Any] = dict(suffixes=("", _JOIN_RIGHT_SUFFIX))
        else:  # pragma: no cover
            # breaking change in ibis 6.0
            suffixes = dict(lname="", rname=_JOIN_RIGHT_SUFFIX)
        if how.lower() == "cross":
            tb = _df1.native.cross_join(_df2.native, **suffixes)
        elif how.lower() == "right_outer":
            tb = _df2.native.left_join(_df1.native, on_fields, **suffixes)
        elif how.lower() == "left_outer":
            tb = _df1.native.left_join(_df2.native, on_fields, **suffixes)
        elif how.lower() == "full_outer":
            tb = _df1.native.outer_join(_df2.native, on_fields, **suffixes)
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
            tb = _df1.native.semi_join(_df2.native, on_fields, **suffixes)
        elif how.lower() in ["anti", "left_anti"]:
            tb = _df1.native.anti_join(_df2.native, on_fields, **suffixes)
        else:
            tb = _df1.native.inner_join(_df2.native, on_fields, **suffixes)
        return self.to_df(tb[end_schema.names], schema=end_schema)

    def union(self, df1: DataFrame, df2: DataFrame, distinct: bool = True) -> DataFrame:
        _df1 = self.to_df(df1)
        _df2 = self.to_df(df2)
        tb = _df1.native.union(_df2.native, distinct=distinct)
        return self.to_df(tb, df1.schema)

    def subtract(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        _df1 = self.to_df(df1)
        _df2 = self.to_df(df2)
        tb = _df1.native.difference(_df2.native, distinct=distinct)
        return self.to_df(tb, df1.schema)

    def intersect(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        _df1 = self.to_df(df1)
        _df2 = self.to_df(df2)
        tb = _df1.native.intersect(_df2.native, distinct=distinct)
        return self.to_df(tb, df1.schema)

    def distinct(self, df: DataFrame) -> DataFrame:
        _df = self.to_df(df)
        tb = _df.native.distinct()
        return self.to_df(tb, df.schema)

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
        _df = self.to_df(df)
        if thresh is None:
            tb = _df.native.dropna(subset=subset, how=how)
            return self.to_df(tb, df.schema)
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
        return self.to_df(tb, df.schema)

    def fillna(self, df: DataFrame, value: Any, subset: List[str] = None) -> DataFrame:
        def _build_value_dict(names: List[str]) -> Dict[str, str]:
            if not isinstance(value, dict):
                return {n: value for n in names}
            else:
                return {n: value[n] for n in names}

        names = df.columns
        if isinstance(value, dict):
            # subset should be ignored
            names = list(value.keys())
        elif subset is not None:
            st = set(names)
            assert_or_throw(
                st.issuperset(subset),
                ValueError(f"{names} is not a superset of {subset}"),
            )
            names = subset
        vd = _build_value_dict(names)
        assert_or_throw(
            all(v is not None for v in vd.values()),
            ValueError("fillna value can not be None or contain None"),
        )
        tb = self.to_df(df).native
        cols = [
            ibis.coalesce(tb[f], ibis.literal(vd[f])).name(f) if f in names else tb[f]
            for f in df.columns
        ]
        return self.to_df(tb[cols], schema=df.schema)

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
        idf = self.to_df(df)

        if len(_presort) == 0:
            if len(partition_spec.partition_by) == 0:
                return self.to_df(idf.native.head(n), schema=df.schema)
            pcols = ", ".join(
                self.encode_column_name(x) for x in partition_spec.partition_by
            )
            sql = (
                f"SELECT * FROM ("
                f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pcols}) "
                f"AS __fugue_take_param FROM {tbn}"
                f") WHERE __fugue_take_param<={n}"
            )
            tb = self.query_to_table(sql, {tbn: idf})
            return self.to_df(tb[df.columns], schema=df.schema)

        sorts: List[str] = []
        for k, v in _presort.items():
            s = self.encode_column_name(k)
            s += " ASC" if v else " DESC"
            s += " NULLS FIRST" if na_position == "first" else " NULLS LAST"
            sorts.append(s)
        sort_expr = "ORDER BY " + ", ".join(sorts)

        if len(partition_spec.partition_by) == 0:
            sql = f"SELECT * FROM {tbn} {sort_expr} LIMIT {n}"
            tb = self.query_to_table(sql, {tbn: idf})
            return self.to_df(tb[df.columns], schema=df.schema)

        pcols = ", ".join(
            self.encode_column_name(x) for x in partition_spec.partition_by
        )
        sql = (
            f"SELECT * FROM ("
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {pcols} {sort_expr}) "
            f"AS __fugue_take_param FROM {tbn}"
            f") WHERE __fugue_take_param<={n}"
        )
        tb = self.query_to_table(sql, {tbn: idf})
        return self.to_df(tb[df.columns], schema=df.schema)

    def table_exists(self, table: str) -> bool:
        return table in self.backend.list_tables()

    def save_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs: Any,
    ) -> None:
        if mode == "overwrite":
            self.backend.drop_table(table, force=True)
        if isinstance(df, IbisDataFrame):
            self.backend.create_table(table, df.native)
        else:
            self.backend.create_table(
                table, df.as_pandas(), schema=to_ibis_schema(df.schema)
            )

    def load_table(self, table: str, **kwargs: Any) -> DataFrame:
        return self.to_df(self.backend.table(table))


class IbisMapEngine(MapEngine):
    """IbisExecutionEngine's MapEngine, it is a wrapper of the map engine
    of :meth:`~.IbisExecutionEngine.non_ibis_engine`

    :param execution_engine: the execution engine this map engine will run on
    """

    @property
    def is_distributed(self) -> bool:
        return self._ibis_engine.non_ibis_engine.map_engine.is_distributed

    def __init__(self, execution_engine: ExecutionEngine) -> None:
        super().__init__(execution_engine)
        self._ibis_engine: IbisExecutionEngine = execution_engine  # type: ignore

    @property
    def execution_engine_constraint(self) -> Type[ExecutionEngine]:
        return IbisExecutionEngine

    def map_dataframe(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
        map_func_format_hint: Optional[str] = None,
    ) -> DataFrame:
        _df = self._ibis_engine._to_non_ibis_dataframe(df)
        return self._ibis_engine.non_ibis_engine.map_engine.map_dataframe(
            _df,
            map_func=map_func,
            output_schema=output_schema,
            partition_spec=partition_spec,
            on_init=on_init,
            map_func_format_hint=map_func_format_hint,
        )

    def map_bag(
        self,
        bag: Bag,
        map_func: Callable[[BagPartitionCursor, LocalBag], LocalBag],
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, Bag], Any]] = None,
    ) -> Bag:  # pragma: no cover
        return self._ibis_engine.non_ibis_engine.map_engine.map_bag(
            bag, map_func, partition_spec, on_init
        )


class IbisExecutionEngine(ExecutionEngine):
    """The base execution engine using Ibis.
    Please read |ExecutionEngineTutorial| to understand this important Fugue concept

    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    """

    def __init__(self, conf: Any):
        super().__init__(conf)
        self._non_ibis_engine = self.create_non_ibis_execution_engine()

    @abstractmethod
    def create_non_ibis_execution_engine(self) -> ExecutionEngine:
        """Create the execution engine that handles operations beyond SQL"""
        raise NotImplementedError  # pragma: no cover

    def is_non_ibis(self, ds: Any) -> bool:
        return not isinstance(ds, (IbisDataFrame, IbisTable))

    @property
    def non_ibis_engine(self) -> ExecutionEngine:
        return self._non_ibis_engine

    @property
    def ibis_sql_engine(self) -> IbisSQLEngine:
        return self.sql_engine  # type: ignore

    def create_default_map_engine(self) -> MapEngine:
        return IbisMapEngine(self)

    @property
    def log(self) -> logging.Logger:
        return self.non_ibis_engine.log

    @property
    def fs(self) -> FileSystem:
        return self.non_ibis_engine.fs

    def get_current_parallelism(self) -> int:
        return self.non_ibis_engine.get_current_parallelism()

    def to_df(self, df: Any, schema: Any = None) -> DataFrame:
        if self.is_non_ibis(df):
            return self._to_non_ibis_dataframe(df, schema)
        return self._to_ibis_dataframe(df, schema=schema)

    def repartition(self, df: DataFrame, partition_spec: PartitionSpec) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.repartition(df, partition_spec=partition_spec)
        self.log.warning("%s doesn't respect repartition", self)
        return df

    def broadcast(self, df: DataFrame) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.broadcast(df)
        return df

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.persist(df, lazy=lazy, **kwargs)
        return self.ibis_sql_engine.persist(df, lazy=lazy, **kwargs)

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: Optional[List[str]] = None,
    ) -> DataFrame:
        if self.is_non_ibis(df1) and self.is_non_ibis(df2):
            return self.non_ibis_engine.join(df1, df2, how=how, on=on)
        return self.ibis_sql_engine.join(df1, df2, how=how, on=on)

    def union(self, df1: DataFrame, df2: DataFrame, distinct: bool = True) -> DataFrame:
        if self.is_non_ibis(df1) and self.is_non_ibis(df2):
            return self.non_ibis_engine.union(df1, df2, distinct=distinct)
        return self.ibis_sql_engine.union(df1, df2, distinct=distinct)

    def subtract(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        if self.is_non_ibis(df1) and self.is_non_ibis(df2):
            return self.non_ibis_engine.subtract(df1, df2, distinct=distinct)
        return self.ibis_sql_engine.subtract(df1, df2, distinct=distinct)

    def intersect(
        self, df1: DataFrame, df2: DataFrame, distinct: bool = True
    ) -> DataFrame:
        if self.is_non_ibis(df1) and self.is_non_ibis(df2):
            return self.non_ibis_engine.intersect(df1, df2, distinct=distinct)
        return self.ibis_sql_engine.intersect(df1, df2, distinct=distinct)

    def distinct(self, df: DataFrame) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.distinct(df)
        return self.ibis_sql_engine.distinct(df)

    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: int = None,
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.dropna(
                df, how=how, thresh=thresh, subset=subset
            )
        return self.ibis_sql_engine.dropna(df, how=how, thresh=thresh, subset=subset)

    def fillna(self, df: DataFrame, value: Any, subset: List[str] = None) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.fillna(df, value=value, subset=subset)
        return self.ibis_sql_engine.fillna(df, value=value, subset=subset)

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.sample(
                df,
                n=n,
                frac=frac,
                replace=replace,
                seed=seed,
            )
        return self.ibis_sql_engine.sample(
            df,
            n=n,
            frac=frac,
            replace=replace,
            seed=seed,
        )

    def take(
        self,
        df: DataFrame,
        n: int,
        presort: str,
        na_position: str = "last",
        partition_spec: Optional[PartitionSpec] = None,
    ) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.take(
                df,
                n=n,
                presort=presort,
                na_position=na_position,
                partition_spec=partition_spec,
            )
        return self.ibis_sql_engine.take(
            df,
            n=n,
            presort=presort,
            na_position=na_position,
            partition_spec=partition_spec,
        )

    def _to_ibis_dataframe(self, df: Any, schema: Any = None) -> DataFrame:
        """Create ``IbisDataFrame`` from the dataframe like input

        :param df: dataframe like object
        :param schema: dataframe schema, defaults to None
        :return: the IbisDataFrame
        """
        return self.sql_engine.to_df(df, schema=schema)

    def _to_non_ibis_dataframe(
        self, df: Any, schema: Any = None
    ) -> DataFrame:  # pragma: no cover
        """Create ``DataFrame`` for map operations
        from the dataframe like input

        :param df: dataframe like object
        :param schema: dataframe schema, defaults to None
        :return: the DataFrame
        """
        return self.non_ibis_engine.to_df(df, schema)
