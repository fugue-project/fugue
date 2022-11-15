from typing import Any, Callable, Dict, List, Optional, Union

import pyarrow as pa
from duckdb import DuckDBPyConnection
from triad import Schema, assert_or_throw, to_uuid
from triad.utils.threading import RunOnce

from fugue import (
    ArrowDataFrame,
    DataFrame,
    LocalDataFrame,
    MapEngine,
    PartitionCursor,
    PartitionSpec,
)
from fugue.collections.partition import EMPTY_PARTITION_SPEC
from fugue.constants import KEYWORD_ROWCOUNT
from fugue.dataframe.arrow_dataframe import _build_empty_arrow
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_duckdb.execution_engine import DuckExecutionEngine

from ._constants import FUGUE_RAY_CONF_SHUFFLE_PARTITIONS
from ._utils.dataframe import add_partition_key
from ._utils.io import RayIO
from .dataframe import RayDataFrame

_RAY_PARTITION_KEY = "__ray_partition_key__"


class RayMapEngine(MapEngine):
    def map_dataframe(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        if len(partition_spec.partition_by) == 0:
            return self._map(
                df=df,
                map_func=map_func,
                output_schema=output_schema,
                partition_spec=partition_spec,
                on_init=on_init,
            )
        else:
            return self._group_map(
                df=df,
                map_func=map_func,
                output_schema=output_schema,
                partition_spec=partition_spec,
                on_init=on_init,
            )

    def _group_map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
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
                if pa.__version__ < "7":  # pragma: no cover
                    idx = pa.compute.sort_indices(
                        adf, options=pa.compute.SortOptions(presort_tuples)
                    )
                    adf = adf.take(idx)
                else:
                    adf = adf.sort_by(presort_tuples)
            input_df = ArrowDataFrame(adf)
            if on_init_once is not None:
                on_init_once(0, input_df)
            cursor = partition_spec.get_cursor(input_schema, 0)
            cursor.set(input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_arrow()

        _df = self.execution_engine._to_ray_df(df)  # type: ignore
        if partition_spec.num_partitions != "0":
            _df = self.execution_engine.repartition(_df, partition_spec)  # type: ignore
        else:
            n = self.execution_engine.conf.get(FUGUE_RAY_CONF_SHUFFLE_PARTITIONS, -1)
            if n > 1:
                _df = self.execution_engine.repartition(  # type: ignore
                    _df, PartitionSpec(num=n)
                )
        rdf, _ = add_partition_key(
            _df.native,
            keys=partition_spec.partition_by,
            input_schema=input_schema,
            output_key=_RAY_PARTITION_KEY,
        )

        gdf = rdf.groupby(_RAY_PARTITION_KEY)
        sdf = gdf.map_groups(
            _udf,
            batch_format="pyarrow",
            **self.execution_engine._get_remote_args(),  # type: ignore
        )
        return RayDataFrame(sdf, schema=output_schema, internal_schema=True)

    def _map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
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

        rdf = self.execution_engine._to_ray_df(df)  # type: ignore
        if not partition_spec.empty:
            rdf = self.execution_engine.repartition(  # type: ignore
                rdf, partition_spec=partition_spec
            )
        sdf = rdf.native.map_batches(
            _udf,
            batch_format="pyarrow",
            **self.execution_engine._get_remote_args(),  # type: ignore
        )
        return RayDataFrame(sdf, schema=output_schema, internal_schema=True)


class RayExecutionEngine(DuckExecutionEngine):
    """A hybrid engine of Ray and DuckDB as Phase 1 of Fugue Ray integration.
    Most operations will be done by DuckDB, but for ``map``, it will use Ray.

    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    :param connection: DuckDB connection
    """

    def __init__(
        self, conf: Any = None, connection: Optional[DuckDBPyConnection] = None
    ):
        super().__init__(conf, connection)
        self._io = RayIO(self)

    def create_default_map_engine(self) -> MapEngine:
        return RayMapEngine(self)

    def to_df(self, df: Any, schema: Any = None) -> DataFrame:
        return self._to_ray_df(df, schema=schema)

    def repartition(self, df: DataFrame, partition_spec: PartitionSpec) -> DataFrame:
        def _persist_and_count(df: RayDataFrame) -> int:
            self.persist(df)
            return df.count()

        rdf = self._to_ray_df(df)

        num_funcs = {KEYWORD_ROWCOUNT: lambda: _persist_and_count(rdf)}
        num = partition_spec.get_num_partitions(**num_funcs)

        if partition_spec.algo in ["hash", "even"]:
            pdf = rdf.native
            if num > 0:
                pdf = pdf.repartition(num)
        elif partition_spec.algo == "rand":
            pdf = rdf.native
            if num > 0:
                pdf = pdf.repartition(num, shuffle=True)
        else:  # pragma: no cover
            raise NotImplementedError(partition_spec.algo + " is not supported")
        return RayDataFrame(pdf, schema=rdf.schema, internal_schema=True)

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

    def load_df(  # type:ignore
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:
        return self._io.load_df(
            uri=path, format_hint=format_hint, columns=columns, **kwargs
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
        df = self._to_ray_df(df)
        self._io.save_df(
            df,
            uri=path,
            format_hint=format_hint,
            mode=mode,
            partition_spec=partition_spec,
            force_single=force_single,
            **kwargs,
        )

    def _to_ray_df(self, df: Any, schema: Any = None) -> RayDataFrame:
        # TODO: remove this in phase 2
        res = self._to_auto_df(df, schema)
        if not isinstance(res, RayDataFrame):
            return RayDataFrame(res)
        return res

    def _to_auto_df(self, df: Any, schema: Any = None) -> DataFrame:
        # TODO: remove this in phase 2
        if isinstance(df, (DuckDataFrame, RayDataFrame)):
            assert_or_throw(
                schema is None,
                ValueError("schema must be None when df is a DataFrame"),
            )
            return df
        return RayDataFrame(df, schema)

    def _get_remote_args(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {}

        for k, v in self.conf.items():
            if k.startswith("fugue.ray.remote."):
                key = k.split(".", 3)[-1]
                res[key] = v

        return res
