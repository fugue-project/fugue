import logging
import os
from typing import Any, Callable, List, Optional, Union

import dask.dataframe as dd
from fugue.collections.partition import (
    EMPTY_PARTITION_SPEC,
    PartitionCursor,
    PartitionSpec,
    parse_presort_exp,
)
from fugue.constants import (
    FUGUE_CONF_SQL_IGNORE_CASE,
    KEYWORD_CORECOUNT,
    KEYWORD_ROWCOUNT,
)
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame, PandasDataFrame
from fugue.dataframe.utils import get_join_schemas
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    ExecutionEngine,
    SQLEngine,
)
from fugue.execution.native_execution_engine import NativeExecutionEngine
from qpd_dask import run_sql_on_dask
from triad.collections import Schema
from triad.collections.dict import IndexedOrderedDict, ParamDict
from triad.collections.fs import FileSystem
from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid
from triad.utils.threading import RunOnce

from fugue_dask._constants import (
    FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS,
    FUGUE_DASK_DEFAULT_CONF,
)
from fugue_dask._io import load_df, save_df
from fugue_dask._utils import DaskUtils
from fugue_dask.dataframe import DaskDataFrame


class QPDDaskEngine(SQLEngine):
    """QPD execution implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def __init__(self, execution_engine: ExecutionEngine):
        assert_or_throw(
            isinstance(execution_engine, DaskExecutionEngine),
            lambda: f"{self} must be used with DaskExecutionEngine",
        )
        super().__init__(execution_engine)

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        dask_dfs = {
            k: self.execution_engine.to_df(v).native  # type: ignore
            for k, v in dfs.items()
        }
        df = run_sql_on_dask(
            statement,
            dask_dfs,
            ignore_case=self.execution_engine.compile_conf.get(
                FUGUE_CONF_SQL_IGNORE_CASE, False
            ),
        )
        return DaskDataFrame(df)


class DaskExecutionEngine(ExecutionEngine):
    """The execution engine based on `Dask <https://docs.dask.org/>`_.

    Please read |ExecutionEngineTutorial| to understand this important Fugue concept

    :param conf: |ParamsLikeObject| defaults to None, read |FugueConfig| to
      learn Fugue specific options

    .. note::

        You should setup Dask single machine or distributed environment in the
        :doc:`common <dask:setup>` way.
        Before initializing :class:`~.DaskExecutionEngine`
    """

    def __init__(self, conf: Any = None):
        p = ParamDict(FUGUE_DASK_DEFAULT_CONF)
        p.update(ParamDict(conf))
        super().__init__(p)
        self._fs = FileSystem()
        self._log = logging.getLogger()
        self._native = NativeExecutionEngine(conf=conf)

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
        return QPDDaskEngine(self)

    @property
    def pl_utils(self) -> DaskUtils:
        """Pandas-like dataframe utils"""
        return DaskUtils()

    def to_df(self, df: Any, schema: Any = None, metadata: Any = None) -> DaskDataFrame:
        """Convert a data structure to :class:`~fugue_dask.dataframe.DaskDataFrame`

        :param data: :class:`~fugue.dataframe.dataframe.DataFrame`,
          :class:`dask:dask.dataframe.DataFrame`,
          pandas DataFrame or list or iterable of arrays
        :param schema: |SchemaLikeObject|, defaults to None.
        :param metadata: |ParamsLikeObject|, defaults to None
        :return: engine compatible dataframe

        .. note::

            * if the input is already :class:`~fugue_dask.dataframe.DaskDataFrame`,
              it should return itself
            * For list or iterable of arrays, ``schema`` must be specified
            * When ``schema`` is not None, a potential type cast may happen to ensure
              the dataframe's schema.
            * all other methods in the engine can take arbitrary dataframes and
              call this method to convert before doing anything
        """
        default_partitions = self.conf.get_or_throw(
            FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS, int
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

        def _map(pdf: Any) -> dd.DataFrame:
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
            result = self.pl_utils.safe_groupby_apply(
                df.native,
                partition_spec.partition_by,
                _map,
                meta=output_schema.pandas_dtype,
            )
        return DaskDataFrame(result, output_schema, metadata)

    def broadcast(self, df: DataFrame) -> DataFrame:
        return self.to_df(df)

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
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
        d = self.pl_utils.join(
            self.to_df(df1).native,
            self.to_df(df2).native,
            join_type=how,
            on=key_schema.names,
        )
        return DaskDataFrame(d, output_schema, metadata)

    def union(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
        metadata: Any = None,
    ) -> DataFrame:
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d = self.pl_utils.union(
            self.to_df(df1).native, self.to_df(df2).native, unique=distinct
        )
        return DaskDataFrame(d, df1.schema, metadata)

    def subtract(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
        metadata: Any = None,
    ) -> DataFrame:
        assert_or_throw(
            distinct, NotImplementedError("EXCEPT ALL for DaskExecutionEngine")
        )
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d = self.pl_utils.except_df(
            self.to_df(df1).native, self.to_df(df2).native, unique=distinct
        )
        return DaskDataFrame(d, df1.schema, metadata)

    def intersect(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
        metadata: Any = None,
    ) -> DataFrame:
        assert_or_throw(
            distinct, NotImplementedError("INTERSECT ALL for DaskExecutionEngine")
        )
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d = self.pl_utils.intersect(
            self.to_df(df1).native, self.to_df(df2).native, unique=distinct
        )
        return DaskDataFrame(d, df1.schema, metadata)

    def distinct(
        self,
        df: DataFrame,
        metadata: Any = None,
    ) -> DataFrame:
        d = self.pl_utils.drop_duplicates(self.to_df(df).native)
        return DaskDataFrame(d, df.schema, metadata)

    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: int = None,
        subset: List[str] = None,
        metadata: Any = None,
    ) -> DataFrame:
        d = self.to_df(df).native.dropna(how=how, thresh=thresh, subset=subset)
        return DaskDataFrame(d, df.schema, metadata)

    def fillna(
        self,
        df: DataFrame,
        value: Any,
        subset: List[str] = None,
        metadata: Any = None,
    ) -> DataFrame:
        assert_or_throw(
            (not isinstance(value, list)) and (value is not None),
            ValueError("fillna value can not be a list or None"),
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
            subset = subset or df.schema.names
            mapping = {col: value for col in subset}
        d = self.to_df(df).native.fillna(mapping)
        return DaskDataFrame(d, df.schema, metadata)

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
        metadata: Any = None,
    ) -> DataFrame:
        assert_or_throw(
            (n is None and frac is not None) or (n is not None and frac is None),
            ValueError("one and only one of n and frac should be set"),
        )
        # TODO: dask does not support sample by number of rows
        d = self.to_df(df).native.sample(
            n=n, frac=frac, replace=replace, random_state=seed
        )
        return DaskDataFrame(d, df.schema, metadata)

    def take(
        self,
        df: DataFrame,
        n: int,
        presort: str,
        na_position: str = "last",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        metadata: Any = None,
    ) -> DataFrame:
        assert_or_throw(
            isinstance(n, int),
            ValueError("n needs to be an integer"),
        )
        d = self.to_df(df).native
        meta = [(d[x].name, d[x].dtype) for x in d.columns]

        if presort:
            presort = parse_presort_exp(presort)
        # Use presort over partition_spec.presort if possible
        _presort: IndexedOrderedDict = presort or partition_spec.presort

        def _partition_take(partition, n, presort):
            if len(presort.keys()) > 0:
                partition = partition.sort_values(
                    list(presort.keys()),
                    ascending=list(presort.values()),
                    na_position=na_position,
                )
            return partition.head(n)

        if len(partition_spec.partition_by) == 0:
            if len(_presort.keys()) == 0:
                d = d.head(n)
            else:
                # Use the default partition
                d = (
                    d.map_partitions(_partition_take, n, _presort, meta=meta)
                    .reset_index(drop=True)
                    .compute()
                )
                # compute() brings this to Pandas so we can use pandas
                d = d.sort_values(
                    list(_presort.keys()),
                    ascending=list(_presort.values()),
                    na_position=na_position,
                ).head(n)

        else:
            d = (
                d.groupby(partition_spec.partition_by, dropna=False)
                .apply(_partition_take, n=n, presort=_presort, meta=meta)
                .reset_index(drop=True)
            )

        return DaskDataFrame(d, df.schema, metadata)

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
        if force_single:
            self._native.save_df(
                df,
                path=path,
                format_hint=format_hint,
                mode=mode,
                partition_spec=partition_spec,
                **kwargs,
            )
        else:
            if not partition_spec.empty:
                self.log.warning(  # pragma: no cover
                    "partition_spec is not respected in %s.save_df", self
                )
            self.fs.makedirs(os.path.dirname(path), recreate=True)
            df = self.to_df(df)
            save_df(df, path, format_hint=format_hint, mode=mode, fs=self.fs, **kwargs)
