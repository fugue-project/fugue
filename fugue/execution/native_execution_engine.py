import logging
from typing import Any, Callable, Iterable, List, Optional, Union

import pandas as pd
import pyarrow as pa
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
    to_local_bounded_df,
)
from fugue.dataframe.utils import get_join_schemas, to_local_df
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    ExecutionEngine,
    SQLEngine,
)
from fugue.utils.io import load_df, save_df
from sqlalchemy import create_engine
from triad.collections import Schema
from triad.collections.dict import ParamDict
from triad.collections.fs import FileSystem
from triad.utils.assertion import assert_or_throw
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


class NativeExecutionEngine(ExecutionEngine):
    def __init__(self, conf: Any = None):
        super().__init__(conf)
        self._fs = FileSystem()
        self._log = logging.getLogger()
        self._default_sql_engine = SqliteEngine(self)

    def __repr__(self) -> str:
        return "NativeExecutionEngine"

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

    def map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        if partition_spec.num_partitions != "0":
            self.log.warning(
                f"{self} doesn't respect num_partitions {partition_spec.num_partitions}"
            )
        cursor = partition_spec.get_cursor(df.schema, 0)
        if on_init is not None:
            on_init(0, df)
        if len(partition_spec.partition_by) == 0:  # no partition
            df = to_local_df(df)
            cursor.set(df.peek_array(), 0, 0)
            output_df = map_func(cursor, df)
            assert_or_throw(
                output_df.schema == output_schema,
                f"map output {output_df.schema} mismatches given {output_schema}",
            )
            output_df._metadata = ParamDict(metadata, deep=True)
            output_df._metadata.set_readonly()
            return output_df
        presort = partition_spec.presort
        presort_keys = list(presort.keys())
        presort_asc = list(presort.values())
        output_schema = Schema(output_schema)

        def _map(pdf: pd.DataFrame) -> pd.DataFrame:
            if len(presort_keys) > 0:
                pdf = pdf.sort_values(presort_keys, ascending=presort_asc)
            input_df = PandasDataFrame(
                pdf.reset_index(drop=True), df.schema, pandas_df_wrapper=True
            )
            cursor.set(input_df.peek_array(), cursor.partition_no + 1, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_pandas()

        result = PD_UTILS.safe_groupby_apply(
            df.as_pandas(), partition_spec.partition_by, _map
        )
        return PandasDataFrame(result, output_schema, metadata)

    def broadcast(self, df: DataFrame) -> DataFrame:
        return self.to_df(df)

    def persist(self, df: DataFrame, level: Any = None) -> DataFrame:
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
        how = how.lower().replace("_", "").replace(" ", "")
        if how == "cross":
            d1 = df1.as_pandas()
            d2 = df2.as_pandas()
            d1["__cross_join_index__"] = 1
            d2["__cross_join_index__"] = 1
            d = d1.merge(d2, on=("__cross_join_index__")).drop(
                "__cross_join_index__", axis=1
            )
            return PandasDataFrame(d.reset_index(drop=True), output_schema, metadata)
        if how in ["semi", "leftsemi"]:
            d1 = df1.as_pandas()
            d2 = df2.as_pandas()[key_schema.names]
            d = d1.merge(d2, on=key_schema.names, how="inner")
            return PandasDataFrame(d.reset_index(drop=True), output_schema, metadata)
        if how in ["anti", "leftanti"]:
            d1 = df1.as_pandas()
            d2 = df2.as_pandas()[key_schema.names]
            d2["__anti_join_dummy__"] = 1.0
            d = d1.merge(d2, on=key_schema.names, how="left")
            d = d[d.iloc[:, -1].isnull()]
            return PandasDataFrame(
                d.drop(["__anti_join_dummy__"], axis=1).reset_index(drop=True),
                output_schema,
                metadata,
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
        return PandasDataFrame(d.reset_index(drop=True), output_schema, metadata)

    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> LocalBoundedDataFrame:
        return self.to_df(
            load_df(
                path, format_hint=format_hint, columns=columns, fs=self.fs, **kwargs
            )
        )

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
        if not partition_spec.empty:
            self.log.warning(  # pragma: no cover
                f"partition_spec is not respected in {self}.save_df"
            )
        df = self.to_df(df)
        save_df(df, path, format_hint=format_hint, mode=mode, fs=self.fs, **kwargs)

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
