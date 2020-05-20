import logging
from typing import Any, Callable, Iterable, List

import dask.dataframe as pd
import pandas
import pyarrow as pa
from fs.base import FS as FileSystem
from fs.osfs import OSFS
from fugue.collections.partition import PartitionSpec
from fugue.constants import KEYWORD_CORECOUNT, KEYWORD_ROWCOUNT
from fugue.dataframe import DataFrame
from fugue.dataframe.pandas_dataframes import PandasDataFrame
from fugue.dataframe.utils import get_join_schemas
from fugue.execution import SqliteEngine
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    ExecutionEngine,
    SQLEngine,
)
from fugue_dask.dataframe import DEFAULT_CONFIG, DaskDataFrame
from fugue_dask.utils import DASK_UTILS
from triad.collections import Schema
from triad.utils.assertion import assert_or_throw
from triad.collections.dict import ParamDict


class DaskExecutionEngine(ExecutionEngine):
    def __init__(self, conf: Any = None):
        p = ParamDict(DEFAULT_CONFIG)
        p.update(ParamDict(conf))
        super().__init__(p)
        self._fs = OSFS("/")
        self._log = logging.getLogger()
        self._default_sql_engine = SqliteEngine(self)

    def __repr__(self) -> str:
        return "DaskExecutionEngine"

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

    def to_df(self, df: Any, schema: Any = None, metadata: Any = None) -> DaskDataFrame:
        default_partitions = self.conf.get_or_throw(
            "fugue.dask.dataframe.default.partitions", int
        )
        if isinstance(df, DataFrame):
            assert_or_throw(
                schema is None and metadata is None,
                ValueError("schema and metadata must be None when df is a DataFrame"),
            )
            if isinstance(df, DaskDataFrame):
                return df
            if isinstance(df, PandasDataFrame):
                return DaskDataFrame(
                    df.native, df.schema, df.metadata, num_partitions=default_partitions
                )
            return DaskDataFrame(
                df.as_array(type_safe=True),
                df.schema,
                df.metadata,
                num_partitions=default_partitions,
            )
        return DaskDataFrame(df, schema, metadata, num_partitions=default_partitions)

    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DaskDataFrame:
        df = self.to_df(df)
        if partition_spec.empty:
            return df
        if len(partition_spec.partition_by) > 0:
            return df
        p = partition_spec.get_num_partitions(
            **{
                KEYWORD_ROWCOUNT: lambda: df.persist().count(),  # type: ignore
                KEYWORD_CORECOUNT: lambda: 2,  # TODO: remove this hard code
            }
        )
        if p > 0:
            return DaskDataFrame(
                df.native.repartition(npartitions=p),
                schema=df.schema,
                metadata=df.metadata,
                type_safe=False,
            )
        return df

    def map_partitions(
        self,
        df: DataFrame,
        mapFunc: Callable[[int, Iterable[Any]], Iterable[Any]],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
    ) -> DataFrame:
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
                    0,
                    DASK_UTILS.as_array_iterable(pdf, type_safe=True, null_safe=False),
                )
            )
            return pandas.DataFrame(data, columns=names)

        df = self.to_df(df)
        if len(partition_spec.partition_by) == 0:
            pdf = self.repartition(df, partition_spec)
            result = pdf.native.map_partitions(_map, meta=output_schema.pandas_dtype)
        else:
            df = self.repartition(df, PartitionSpec(num=partition_spec.num_partitions))
            result = DASK_UTILS.safe_groupby_apply(
                df.native,
                partition_spec.partition_by,
                _map,
                meta=output_schema.pandas_dtype,
            )
        return DaskDataFrame(result, output_schema, metadata)

    def broadcast(self, df: DataFrame) -> DataFrame:
        return self.to_df(df)

    def persist(self, df: DataFrame, level: Any = None) -> DataFrame:
        return self.to_df(df).persist()

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
            return DaskDataFrame(d.reset_index(drop=True), output_schema, metadata)
        if how in ["semi", "leftsemi"]:
            d1 = self.to_df(df1).native
            d2 = self.to_df(df2).native[key_schema.names]
            d = d1.merge(d2, on=key_schema.names, how="inner")
            return DaskDataFrame(d.reset_index(drop=True), output_schema, metadata)
        if how in ["anti", "leftanti"]:
            d1 = self.to_df(df1).native
            d2 = self.to_df(df2).native[key_schema.names]
            if DASK_UTILS.empty(d1) or DASK_UTILS.empty(d2):
                return df1
            d2["__anti_join_dummy__"] = 1.0
            d = d1.merge(d2, on=key_schema.names, how="left")
            d = d[d["__anti_join_dummy__"].isnull()]
            return DaskDataFrame(
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
        return DaskDataFrame(d.reset_index(drop=True), output_schema, metadata)

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
        if DASK_UTILS.empty(df):
            return df
        for key in keys:
            if pa.types.is_floating(schema[key].type):
                continue
            df[key] = df[key].where(df[key].notnull(), None)
        return df
