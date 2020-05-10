import logging
from typing import Any, Callable, Iterable, List

import pandas as pd
import pyarrow as pa
from fs.base import FS as FileSystem
from fs.osfs import OSFS
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    DataFrames,
    IterableDataFrame,
    LocalBoundedDataFrame,
    PandasDataFrame,
    to_local_bounded_df,
)
from fugue.dataframe.utils import get_join_schemas, to_local_df
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    SQLEngine,
    ExecutionEngine,
)
from sqlalchemy import create_engine
from triad.collections import Schema
from triad.utils.pandas_like import PD_UTILS


class SqliteEngine(SQLEngine):
    def __init__(self, execution_engine: ExecutionEngine) -> None:
        return super().__init__(execution_engine)

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        sql_engine = create_engine("sqlite:///:memory:")
        for k, v in dfs.items():
            v.as_pandas().to_sql(k, sql_engine, if_exists="replace", index=False)
        df = pd.read_sql_query(statement, sql_engine)
        return PandasDataFrame(df)


class NaiveExecutionEngine(ExecutionEngine):
    def __init__(self, conf: Any = None):
        super().__init__(conf)
        self._fs = OSFS("/")
        self._log = logging.getLogger()
        self._default_sql_engine = SqliteEngine(self)

    def __repr__(self) -> str:
        return "NaiveExecutionEngine"

    @property
    def log(self) -> logging.Logger:
        return self._log

    @property
    def fs(self) -> FileSystem:
        return self._fs

    @property
    def default_sql_engine(self) -> SQLEngine:
        return self._default_sql_engine

    def stop(self) -> None:  # pragma: no cover
        return

    def to_df(
        self, df: Any, schema: Any = None, metadata: Any = None
    ) -> LocalBoundedDataFrame:
        return to_local_bounded_df(df, schema, metadata)

    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        self.log.warning(f"{self} doesn't respect repartition")
        return df

    def map_partitions(
        self,
        df: DataFrame,
        mapFunc: Callable[[int, Iterable[Any]], Iterable[Any]],
        output_schema: Any,
        partition_spec: PartitionSpec,
    ) -> DataFrame:
        if partition_spec.num_partitions != "0":
            self.log.warning(
                f"{self} doesn't respect num_partitions {partition_spec.num_partitions}"
            )
        if len(partition_spec.partition_by) == 0:  # no partition
            df = to_local_df(df)
            return IterableDataFrame(
                mapFunc(0, df.as_array_iterable(type_safe=True)), output_schema
            )
        presort = partition_spec.presort
        presort_keys = list(presort.keys())
        presort_asc = list(presort.values())
        output_schema = Schema(output_schema)
        names = output_schema.names

        def _map(pdf: Any) -> pd.DataFrame:
            if len(presort_keys) > 0:
                pdf = pdf.sort_values(presort_keys, ascending=presort_asc)
            data = list(
                mapFunc(
                    0, PD_UTILS.as_array_iterable(pdf, type_safe=True, null_safe=False)
                )
            )
            return pd.DataFrame(data, columns=names)

        result = PD_UTILS.safe_groupby_apply(
            df.as_pandas(), partition_spec.partition_by, _map
        )
        return PandasDataFrame(result, output_schema)

    # TODO: remove this
    def _map_partitions(
        self,
        df: DataFrame,
        mapFunc: Callable[[int, Iterable[Any]], Iterable[Any]],
        output_schema: Any,
        partition_spec: PartitionSpec,
    ) -> DataFrame:  # pragma: no cover
        df = to_local_df(df)
        if partition_spec.num_partitions != "0":
            self.log.warning(
                f"{self} doesn't respect num_partitions {partition_spec.num_partitions}"
            )
        partitioner = partition_spec.get_partitioner(df.schema)
        if len(partition_spec.partition_by) == 0:  # no partition
            return IterableDataFrame(
                mapFunc(0, df.as_array_iterable(type_safe=True)), output_schema
            )
        pdf = df.as_pandas()
        sorts = partition_spec.get_sorts(df.schema)
        if len(sorts) > 0:
            pdf = pdf.sort_values(
                list(sorts.keys()), ascending=list(sorts.values()), na_position="first"
            ).reset_index(drop=True)
        df = PandasDataFrame(pdf, df.schema)

        def get_rows() -> Iterable[Any]:
            for _, _, sub in partitioner.partition(
                df.as_array_iterable(type_safe=True)
            ):
                for r in mapFunc(0, sub):
                    yield r

        return ArrayDataFrame(get_rows(), output_schema)

    def broadcast(self, df: DataFrame) -> DataFrame:
        return self.to_df(df)

    def persist(self, df: DataFrame, level: Any = None) -> DataFrame:
        return self.to_df(df)

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        keys: List[str] = _DEFAULT_JOIN_KEYS,
    ) -> DataFrame:
        key_schema, output_schema = get_join_schemas(df1, df2, how=how, keys=keys)
        how = how.lower().replace("_", "").replace(" ", "")
        if how == "cross":
            d1 = df1.as_pandas()
            d2 = df2.as_pandas()
            d1["__cross_join_index__"] = 1
            d2["__cross_join_index__"] = 1
            d = d1.merge(d2, on=("__cross_join_index__")).drop(
                "__cross_join_index__", axis=1
            )
            return PandasDataFrame(d.reset_index(drop=True), output_schema)
        if how in ["semi", "leftsemi"]:
            d1 = df1.as_pandas()
            d2 = df2.as_pandas()[key_schema.names]
            d = d1.merge(d2, on=key_schema.names, how="inner")
            return PandasDataFrame(d.reset_index(drop=True), output_schema)
        if how in ["anti", "leftanti"]:
            d1 = df1.as_pandas()
            d2 = df2.as_pandas()[key_schema.names]
            d2["__anti_join_dummy__"] = 1.0
            d = d1.merge(d2, on=key_schema.names, how="left")
            d = d[d.iloc[:, -1].isnull()]
            return PandasDataFrame(
                d.drop(["__anti_join_dummy__"], axis=1).reset_index(drop=True),
                output_schema,
            )
        fix_left, fix_right = False, False
        if how in ["leftouter"]:
            how = "left"
            self._validate_outer_joinable(df2.schema, key_schema)
            fix_right = True
        if how in ["rightouter"]:
            how = "right"
            self._validate_outer_joinable(df1.schema, key_schema)
            fix_left = True
        if how in ["fullouter"]:
            how = "outer"
            self._validate_outer_joinable(df1.schema, key_schema)
            self._validate_outer_joinable(df2.schema, key_schema)
            fix_left, fix_right = True, True
        d1 = df1.as_pandas()
        d2 = df2.as_pandas()
        d = d1.merge(d2, on=key_schema.names, how=how)
        if fix_left:
            d = self._fix_nan(
                d, output_schema, df1.schema.exclude(list(df2.schema.keys())).keys()
            )
        if fix_right:
            d = self._fix_nan(
                d, output_schema, df2.schema.exclude(list(df1.schema.keys())).keys()
            )
        return PandasDataFrame(d.reset_index(drop=True), output_schema)

    def _validate_outer_joinable(self, schema: Schema, key_schema: Schema) -> None:
        # TODO: this is to prevent wrong behavior of pandas, we may not need it
        # s = schema - key_schema
        # if any(pa.types.is_boolean(v) or pa.types.is_integer(v) for v in s.types):
        #    raise NotImplementedError(
        #        f"{schema} excluding {key_schema} is not outer joinable"
        #    )
        return

    def _fix_nan(
        self, df: pd.DataFrame, schema: Schema, keys: Iterable[str]
    ) -> pd.DataFrame:
        for key in keys:
            if pa.types.is_floating(schema[key].type):
                continue
            df[key] = df[key].where(pd.notna(df[key]), None)
        return df
