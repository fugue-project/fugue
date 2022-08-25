import hashlib
import pickle
from typing import Any, Callable, List, Optional

import pandas as pd
import pyarrow as pa
from fugue import (
    ArrowDataFrame,
    DataFrame,
    LocalDataFrame,
    PandasDataFrame,
    PartitionCursor,
    PartitionSpec,
)
from fugue.constants import KEYWORD_ROWCOUNT
from fugue.dataframe.arrow_dataframe import _build_empty_arrow
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_duckdb.execution_engine import DuckExecutionEngine
from qpd_pandas.engine import PandasUtils
from triad import Schema, assert_or_throw, to_uuid
from triad.utils.threading import RunOnce

from ._constants import FUGUE_RAY_CONF_SHUFFLE_PARTITIONS
from ._ray_utils import add_partition_key
from .dateframe import RayDataFrame

_RAY_PARTITION_KEY = "__ray_partition_key__"


class RayExecutionEngine(DuckExecutionEngine):
    """A hybrid engine of Ray and DuckDB as Phase 1 of Fugue Ray integration.
    Most operations will be done by DuckDB, but for ``map``, it will use Ray.

    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    :param connection: DuckDB connection
    """

    def to_df(self, df: Any, schema: Any = None, metadata: Any = None) -> DataFrame:
        return self._to_ray_df(df, schema=schema, metadata=metadata)

    def repartition(self, df: DataFrame, partition_spec: PartitionSpec) -> DataFrame:
        def _persist_and_count(df: RayDataFrame) -> int:
            self.persist(df)
            return df.count()

        rdf = self._to_ray_df(df)

        num_funcs = {KEYWORD_ROWCOUNT: lambda: _persist_and_count(rdf)}
        num = partition_spec.get_num_partitions(**num_funcs)

        if partition_spec.algo in ["hash", "even"]:
            pdf = rdf.native.repartition(num)
        elif partition_spec.algo == "rand":
            pdf = rdf.native.repartition(num, shuffle=True)
        else:  # pragma: no cover
            raise NotImplementedError(partition_spec.algo + " is not supported")
        return RayDataFrame(
            pdf, schema=rdf.schema, metadata=df.metadata, internal_schema=True
        )

    def broadcast(self, df: DataFrame) -> DataFrame:
        return df

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        df = self._to_auto_df(df)
        if isinstance(df, RayDataFrame):
            return df.persist(**kwargs)
        return df

    def convert_yield_dataframe(self, df: DataFrame, as_local: bool) -> DataFrame:
        if isinstance(df, RayDataFrame):
            return df if not as_local else df.as_local()
        return super().convert_yield_dataframe(df, as_local)

    def map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        if len(partition_spec.partition_by) == 0:
            return self._map(
                df=df,
                map_func=map_func,
                output_schema=output_schema,
                partition_spec=partition_spec,
                metadata=metadata,
                on_init=on_init,
            )
        else:
            return self._group_map_hash(
                df=df,
                map_func=map_func,
                output_schema=output_schema,
                partition_spec=partition_spec,
                metadata=metadata,
                on_init=on_init,
            )

    def _group_map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        presort = partition_spec.presort
        presort_tuples = [
            (k, "ascending" if v else "descending") for k, v in presort.items()
        ]
        output_schema = Schema(output_schema)
        input_schema = df.schema
        on_init_once: Any = (
            None
            if on_init is None
            else RunOnce(
                on_init, lambda *args, **kwargs: to_uuid(id(on_init), id(args[0]))
            )
        )

        def _udf(adf: pa.Table) -> pa.Table:  # pragma: no cover
            if adf.shape[0] == 0:
                return _build_empty_arrow(output_schema)
            adf = adf.remove_column(len(input_schema))  # remove partition key
            if len(presort_tuples) > 0:
                adf = adf.sort_by(presort_tuples)
            input_df = ArrowDataFrame(adf)
            if on_init_once is not None:
                on_init_once(0, input_df)
            cursor = partition_spec.get_cursor(input_schema, 0)
            cursor.set(input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_arrow()

        _df = self._to_ray_df(df)
        if partition_spec.num_partitions != "0":
            _df = self.repartition(_df, partition_spec)  # type: ignore
        else:
            n = self.conf.get(FUGUE_RAY_CONF_SHUFFLE_PARTITIONS, -1)
            if n > 1:
                _df = self.repartition(_df, PartitionSpec(num=n))  # type: ignore
        rdf, _ = add_partition_key(
            _df.native,
            keys=partition_spec.partition_by,
            input_schema=input_schema,
            output_key=_RAY_PARTITION_KEY,
        )

        gdf = rdf.groupby(_RAY_PARTITION_KEY)
        sdf = gdf.map_groups(_udf, batch_format="pyarrow")
        return RayDataFrame(
            sdf, schema=output_schema, metadata=metadata, internal_schema=True
        )

    def _group_map_hash(
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

        def _to_hash(x: Any, num: int) -> int:
            return int.from_bytes(hashlib.md5(pickle.dumps(x)).digest(), "big") % num

        def _get_blocks(df: pd.DataFrame, keys: List[str], num: int) -> pd.DataFrame:
            _s = df[keys].apply(lambda x: _to_hash(tuple(x), num), axis=1)
            return pd.DataFrame(
                [
                    [k, pickle.dumps(v)]
                    for k, v in df.groupby(_s, dropna=False, as_index=False)
                ],
                columns=["key", "value"],
            )

        def _map_shard(df: pa.Table) -> pa.Table:  # pragma: no cover
            if df.shape[0] == 0:
                return _build_empty_arrow(output_schema)
            pdf = pd.concat(
                pickle.loads(x) for x in df.column(1).combine_chunks().tolist()
            )
            cursor = partition_spec.get_cursor(input_schema, 0)

            def _map(pdf: pd.DataFrame) -> pd.DataFrame:
                if len(presort_keys) > 0:
                    pdf = pdf.sort_values(presort_keys, ascending=presort_asc)
                input_df = PandasDataFrame(
                    pdf.reset_index(drop=True), input_schema, pandas_df_wrapper=True
                )
                if on_init_once is not None:
                    on_init_once(0, input_df)
                cursor.set(input_df.peek_array(), cursor.partition_no + 1, 0)
                output_df = map_func(cursor, input_df)
                return output_df.as_pandas()

            return pa.Table.from_pandas(
                PandasUtils().safe_groupby_apply(
                    pdf, partition_spec.partition_by, _map
                ),
                schema=output_schema.pa_schema,
                nthreads=1,
                safe=False,
            )

        _df = self._to_ray_df(df)
        if partition_spec.num_partitions != "0":
            _df = self.repartition(_df, partition_spec)  # type: ignore
        else:
            n = self.conf.get(FUGUE_RAY_CONF_SHUFFLE_PARTITIONS, -1)
            if n > 1:
                _df = self.repartition(_df, PartitionSpec(num=n))  # type: ignore

        gdf = _df.native.map_batches(
            lambda df: _get_blocks(
                df, keys=partition_spec.partition_by, num=_df.num_partitions
            ),
            batch_format="pandas",
        ).groupby("key")
        sdf = gdf.map_groups(_map_shard, batch_format="pyarrow")
        return RayDataFrame(
            sdf, schema=output_schema, metadata=metadata, internal_schema=True
        )

    def _map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        output_schema = Schema(output_schema)
        input_schema = df.schema
        on_init_once: Any = (
            None
            if on_init is None
            else RunOnce(
                on_init, lambda *args, **kwargs: to_uuid(id(on_init), id(args[0]))
            )
        )

        def _udf(adf: pa.Table) -> pa.Table:  # pragma: no cover
            if adf.shape[0] == 0:
                return _build_empty_arrow(output_schema)
            input_df = ArrowDataFrame(adf)
            if on_init_once is not None:
                on_init_once(0, input_df)
            cursor = partition_spec.get_cursor(input_schema, 0)
            cursor.set(input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_arrow()

        rdf = self._to_ray_df(df)
        if not partition_spec.empty:
            rdf = self.repartition(rdf, partition_spec=partition_spec)  # type: ignore
        sdf = rdf.native.map_batches(_udf, batch_format="pyarrow")
        return RayDataFrame(
            sdf, schema=output_schema, metadata=metadata, internal_schema=True
        )

    def _to_ray_df(
        self, df: Any, schema: Any = None, metadata: Any = None
    ) -> RayDataFrame:
        # TODO: remove this in phase 2
        res = self._to_auto_df(df, schema, metadata=metadata)
        if not isinstance(res, RayDataFrame):
            return RayDataFrame(res, metadata=metadata)
        return res

    def _to_auto_df(
        self, df: Any, schema: Any = None, metadata: Any = None
    ) -> DataFrame:
        # TODO: remove this in phase 2
        if isinstance(df, (DuckDataFrame, RayDataFrame)):
            assert_or_throw(
                schema is None and metadata is None,
                ValueError("schema and metadata must be None when df is a DataFrame"),
            )
            return df
        return RayDataFrame(df, schema, metadata=metadata)
