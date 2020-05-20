import logging
from typing import Any, Callable, Iterable, List

import modin.pandas as pd
import pandas
import pyarrow as pa
from fs.base import FS as FileSystem
from fs.osfs import OSFS
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame, IterableDataFrame
from fugue.dataframe.pandas_dataframes import PandasDataFrame
from fugue.dataframe.utils import get_join_schemas, to_local_df
from fugue.execution import SqliteEngine
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    ExecutionEngine,
    SQLEngine,
)
from fugue_modin.dataframe import ModinDataFrame
from triad.collections import ParamDict, Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.pandas_like import as_array_iterable, safe_groupby_apply


class ModinExecutionEngine(ExecutionEngine):
    def __init__(self, conf: Any = None):
        self._conf = ParamDict(conf)
        self._fs = OSFS("/")
        self._log = logging.getLogger()
        self._default_sql_engine = SqliteEngine(self)

    def __repr__(self) -> str:
        return "ModinExecutionEngine"

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
    ) -> ModinDataFrame:
        if isinstance(df, DataFrame):
            assert_or_throw(
                schema is None and metadata is None,
                ValueError("schema and metadata must be None when df is a DataFrame"),
            )
            if isinstance(df, ModinDataFrame):
                return df
            if isinstance(df, PandasDataFrame):
                return ModinDataFrame(df.native, df.schema, df.metadata)
            return ModinDataFrame(df.as_array(type_safe=True), df.schema, df.metadata)
        return ModinDataFrame(df, schema, metadata)

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
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        if partition_spec.num_partitions != "0":
            self.log.warning(
                f"{self} doesn't respect num_partitions {partition_spec.num_partitions}"
            )
        if len(partition_spec.partition_by) == 0:  # no partition
            # TODO: is there a better way?
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
                mapFunc(0, as_array_iterable(pdf, type_safe=True, null_safe=False))
            )
            return pandas.DataFrame(data, columns=names)

        df = self.to_df(df)
        result = safe_groupby_apply(df.native, partition_spec.partition_by, _map)
        return ModinDataFrame(result, output_schema)

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
            d1 = self.to_df(df1).native
            d2 = self.to_df(df2).native
            d1["__cross_join_index__"] = 1
            d2["__cross_join_index__"] = 1
            d = d1.merge(d2, on=("__cross_join_index__")).drop(
                "__cross_join_index__", axis=1
            )
            return ModinDataFrame(d.reset_index(drop=True), output_schema)
        if how in ["semi", "leftsemi"]:
            d1 = self.to_df(df1).native
            d2 = self.to_df(df2).native[key_schema.names]
            d = d1.merge(d2, on=key_schema.names, how="inner")
            return ModinDataFrame(d.reset_index(drop=True), output_schema)
        if how in ["anti", "leftanti"]:
            d1 = self.to_df(df1).native
            d2 = self.to_df(df2).native[key_schema.names]
            if d1.empty or d2.empty:
                return df1
            d2["__anti_join_dummy__"] = 1.0
            d = d1.merge(d2, on=key_schema.names, how="left")
            d = d[d["__anti_join_dummy__"].isnull()]
            return ModinDataFrame(
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
        d1 = self.to_df(df1).native
        d2 = self.to_df(df2).native
        d = d1.merge(d2, on=key_schema.names, how=how)
        if fix_left:
            d = self._fix_nan(
                d, output_schema, df1.schema.exclude(list(df2.schema.keys())).keys()
            )
        if fix_right:
            d = self._fix_nan(
                d, output_schema, df2.schema.exclude(list(df1.schema.keys())).keys()
            )
        return ModinDataFrame(d.reset_index(drop=True), output_schema)

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
        if df.empty:
            return df
        for key in keys:
            if pa.types.is_floating(schema[key].type):
                continue
            df[key] = df[key].where(pd.notna(df[key]), None)
        return df
