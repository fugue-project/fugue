from typing import Any, Callable, Dict, List, Optional, Type, Union

import pyarrow as pa
import ray
from duckdb import DuckDBPyConnection
from triad import Schema, assert_or_throw, to_uuid
from triad.utils.threading import RunOnce

from fugue import (
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    LocalDataFrame,
    MapEngine,
    PartitionCursor,
    PartitionSpec,
)
from fugue.constants import KEYWORD_PARALLELISM, KEYWORD_ROWCOUNT
from fugue.dataframe.arrow_dataframe import _build_empty_arrow
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_duckdb.execution_engine import DuckExecutionEngine

from ._constants import FUGUE_RAY_DEFAULT_BATCH_SIZE, FUGUE_RAY_ZERO_COPY
from ._utils.cluster import get_default_partitions, get_default_shuffle_partitions
from ._utils.dataframe import add_coarse_partition_key, add_partition_key
from ._utils.io import RayIO
from .dataframe import RayDataFrame

_RAY_PARTITION_KEY = "__ray_partition_key__"


class RayMapEngine(MapEngine):
    @property
    def execution_engine_constraint(self) -> Type[ExecutionEngine]:
        return RayExecutionEngine

    @property
    def is_distributed(self) -> bool:
        return True

    def map_dataframe(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
        map_func_format_hint: Optional[str] = None,
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
        output_schema = Schema(output_schema)
        input_schema = df.schema
        presort = partition_spec.get_sorts(
            input_schema, with_partition_keys=partition_spec.algo == "coarse"
        )
        presort_tuples = [
            (k, "ascending" if v else "descending") for k, v in presort.items()
        ]
        cursor = partition_spec.get_cursor(input_schema, 0)
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
            if len(partition_spec.presort) > 0:
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
            cursor.set(lambda: input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_arrow()

        _df: RayDataFrame = self.execution_engine._to_ray_df(df)  # type: ignore
        if partition_spec.num_partitions != "0":
            _df = self.execution_engine.repartition(_df, partition_spec)  # type: ignore
        else:
            n = get_default_shuffle_partitions(self.execution_engine)
            if n > 0 and n != _df.num_partitions:
                # if n==0 or same as the current dataframe partitions
                # then no repartition will be done by fugue
                # otherwise, repartition the dataset
                _df = self.execution_engine.repartition(  # type: ignore
                    _df, PartitionSpec(num=n)
                )
        if partition_spec.algo != "coarse":
            rdf, _ = add_partition_key(
                _df.native,
                keys=partition_spec.partition_by,
                input_schema=input_schema,
                output_key=_RAY_PARTITION_KEY,
            )
        else:
            rdf = add_coarse_partition_key(
                _df.native,
                keys=partition_spec.partition_by,
                output_key=_RAY_PARTITION_KEY,
                bucket=_df.num_partitions,
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
        cursor = partition_spec.get_cursor(input_schema, 0)
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
            cursor.set(lambda: input_df.peek_array(), 0, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_arrow()

        rdf = self.execution_engine._to_ray_df(df)  # type: ignore
        if not partition_spec.empty:
            rdf = self.execution_engine.repartition(  # type: ignore
                rdf, partition_spec=partition_spec
            )
        elif rdf.num_partitions <= 1:
            n = get_default_partitions(self.execution_engine)
            if n > 0 and n != rdf.num_partitions:
                # if n==0 or same as the current dataframe partitions
                # then no repartition will be done by fugue
                # otherwise, repartition the dataset
                rdf = self.execution_engine.repartition(  # type: ignore
                    rdf, PartitionSpec(num=n)
                )
        mb_args: Dict[str, Any] = {}
        if FUGUE_RAY_DEFAULT_BATCH_SIZE in self.conf:
            mb_args["batch_size"] = self.conf.get_or_throw(
                FUGUE_RAY_DEFAULT_BATCH_SIZE, int
            )
        if ray.__version__ >= "2.3":
            mb_args["zero_copy_batch"] = self.conf.get(FUGUE_RAY_ZERO_COPY, True)
        sdf = rdf.native.map_batches(
            _udf,
            batch_format="pyarrow",
            **mb_args,
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
        if not ray.is_initialized():  # pragma: no cover
            ray.init()

        super().__init__(conf, connection)
        self._io = RayIO(self)

    def __repr__(self) -> str:
        return "RayExecutionEngine"

    @property
    def is_distributed(self) -> bool:
        return True

    def create_default_map_engine(self) -> MapEngine:
        return RayMapEngine(self)

    def get_current_parallelism(self) -> int:
        res = ray.cluster_resources()
        n = res.get("CPU", 0)
        if n == 0:  # pragma: no cover
            res.get("cpu", 0)
        return int(n)

    def to_df(self, df: Any, schema: Any = None) -> DataFrame:
        return self._to_ray_df(df, schema=schema)

    def repartition(self, df: DataFrame, partition_spec: PartitionSpec) -> DataFrame:
        def _persist_and_count(df: RayDataFrame) -> int:
            self.persist(df)
            return df.count()

        rdf = self._to_ray_df(df)

        num_funcs = {
            KEYWORD_ROWCOUNT: lambda: _persist_and_count(rdf),
            KEYWORD_PARALLELISM: lambda: self.get_current_parallelism(),
        }
        num = partition_spec.get_num_partitions(**num_funcs)
        pdf = rdf.native

        if num > 0:
            if partition_spec.algo in ["hash", "even", "coarse"]:
                pdf = pdf.repartition(num)
            elif partition_spec.algo == "rand":
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
        return df  # pragma: no cover

    def convert_yield_dataframe(self, df: DataFrame, as_local: bool) -> DataFrame:
        if isinstance(df, RayDataFrame):
            return df if not as_local else df.as_local()
        return super().convert_yield_dataframe(df, as_local)

    def union(self, df1: DataFrame, df2: DataFrame, distinct: bool = True) -> DataFrame:
        if distinct:
            return super().union(df1, df2, distinct)
        assert_or_throw(
            df1.schema == df2.schema, ValueError(f"{df1.schema} != {df2.schema}")
        )
        tdf1 = self._to_ray_df(df1)
        tdf2 = self._to_ray_df(df2)
        return RayDataFrame(tdf1.native.union(tdf2.native), df1.schema)

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
        partition_spec: Optional[PartitionSpec] = None,
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:
        partition_spec = partition_spec or PartitionSpec()
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
