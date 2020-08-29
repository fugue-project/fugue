import logging
from typing import Any, Callable, Iterable, List, Optional, Union

import dask.dataframe as pd
import pyarrow as pa
from fugue.collections.partition import (
    EMPTY_PARTITION_SPEC,
    PartitionCursor,
    PartitionSpec,
)
from fugue.constants import KEYWORD_CORECOUNT, KEYWORD_ROWCOUNT
from fugue.dataframe import DataFrame, LocalDataFrame, PandasDataFrame, DataFrames
from fugue.dataframe.utils import get_join_schemas
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    ExecutionEngine,
    SQLEngine,
)
from fugue._utils.io import load_df, save_df
from fugue_dask.dataframe import DEFAULT_CONFIG, DaskDataFrame
from fugue_dask._utils import DASK_UTILS
from triad.collections import Schema
from triad.collections.dict import ParamDict
from triad.collections.fs import FileSystem
from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid
from triad.utils.threading import RunOnce
from qpd_dask import run_sql_on_dask


class QPDDaskEngine(SQLEngine):
    """QPD execution implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def __init__(self, execution_engine: ExecutionEngine) -> None:
        return super().__init__(execution_engine)

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        dask_dfs = {
            k: self.execution_engine.to_df(v).native  # type: ignore
            for k, v in dfs.items()
        }
        df = run_sql_on_dask(statement, dask_dfs)
        return DaskDataFrame(df)


class DaskExecutionEngine(ExecutionEngine):
    """The execution engine based on `Dask <https://docs.dask.org/>`_.

    Please read |ExecutionEngineTutorial| to understand this important Fugue concept

    :param conf: |ParamsLikeObject| defaults to None, read |FugueConfig| to
      learn Fugue specific options

    :Notice:

    You should setup Dask single machine or distributed environment in the
    :doc:`common <dask:setup>` way. Before initializing :class:`~.DaskExecutionEngine`
    """

    def __init__(self, conf: Any = None):
        p = ParamDict(DEFAULT_CONFIG)
        p.update(ParamDict(conf))
        super().__init__(p)
        self._fs = FileSystem()
        self._log = logging.getLogger()
        self._default_sql_engine = QPDDaskEngine(self)

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
        """It does nothing"""
        return

    def to_df(self, df: Any, schema: Any = None, metadata: Any = None) -> DaskDataFrame:
        """Convert a data structure to :class:`~fugue_dask.dataframe.DaskDataFrame`

        :param data: :class:`~fugue.dataframe.dataframe.DataFrame`,
          :class:`dask:dask.dataframe.DataFrame`,
          pandas DataFrame or list or iterable of arrays
        :param schema: |SchemaLikeObject|, defaults to None.
        :param metadata: |ParamsLikeObject|, defaults to None
        :return: engine compatible dataframe

        :Notice:

        * if the input is already :class:`~fugue_dask.dataframe.DaskDataFrame`,
          it should return itself
        * For list or iterable of arrays, ``schema`` must be specified
        * When ``schema`` is not None, a potential type cast may happen to ensure
          the dataframe's schema.
        * all other methods in the engine can take arbitrary dataframes and
          call this method to convert before doing anything
        """
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

    def map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        presort = partition_spec.presort
        presort_keys = list(presort.keys())
        presort_asc = list(presort.values())
        output_schema = Schema(output_schema)
        input_schema = df.schema
        on_init_once: Any = (
            None
            if on_init is None
            else RunOnce(
                on_init, lambda *args, **kwargs: to_uuid(id(on_init), id(args[0]))
            )
        )

        def _map(pdf: Any) -> pd.DataFrame:
            if pdf.shape[0] == 0:
                return PandasDataFrame([], output_schema).as_pandas()
            if len(presort_keys) > 0:
                pdf = pdf.sort_values(presort_keys, ascending=presort_asc)
            input_df = PandasDataFrame(
                pdf.reset_index(drop=True), input_schema, pandas_df_wrapper=True
            )
            if on_init_once is not None:
                on_init_once(0, input_df)
            cursor = partition_spec.get_cursor(input_schema, 0)
            cursor.set(input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_pandas()

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

    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DaskDataFrame:
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
        df = self.to_df(df).as_local()
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
        if DASK_UTILS.empty(df):
            return df
        for key in keys:
            if pa.types.is_floating(schema[key].type):
                continue
            df[key] = df[key].where(df[key].notnull(), None)
        return df
