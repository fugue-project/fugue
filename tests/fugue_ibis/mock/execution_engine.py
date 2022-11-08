import logging
from typing import Any, Callable, Iterable, List, Optional, Union

import ibis
import pyarrow as pa
from fugue import (
    ArrowDataFrame,
    DataFrame,
    LocalDataFrame,
    NativeExecutionEngine,
    PartitionCursor,
    PartitionSpec,
)
from fugue.collections.partition import EMPTY_PARTITION_SPEC
from fugue_ibis import IbisDataFrame, IbisExecutionEngine, IbisTable
from triad import FileSystem, assert_or_throw

from .dataframe import MockDuckDataFrame


class MockDuckExecutionEngine(IbisExecutionEngine):
    def __init__(self, conf: Any):
        super().__init__(conf)
        self._backend = ibis.duckdb.connect()
        self._native_engine = NativeExecutionEngine(conf)

    @property
    def backend(self) -> ibis.BaseBackend:
        return self._backend

    def encode_column_name(self, name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _to_ibis_dataframe(
        self, df: Any, schema: Any = None, metadata: Any = None
    ) -> IbisDataFrame:
        if isinstance(df, MockDuckDataFrame):
            return df
        if isinstance(df, DataFrame):
            return self._register_df(
                df.as_arrow(),
                schema=schema if schema is not None else df.schema,
                metadata=metadata if metadata is not None else df.metadata,
            )
        if isinstance(df, pa.Table):
            return self._register_df(df, schema=schema, metadata=metadata)
        if isinstance(df, IbisTable):
            return MockDuckDataFrame(df, schema=schema, metadata=metadata)
        if isinstance(df, Iterable):
            adf = ArrowDataFrame(df, schema)
            return self._register_df(adf.native, schema=schema, metadata=metadata)
        raise NotImplementedError

    def __repr__(self) -> str:
        return "MockDuckExecutionEngine"

    @property
    def log(self) -> logging.Logger:
        return self._native_engine.log

    @property
    def fs(self) -> FileSystem:
        return self._native_engine.fs

    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        self.log.warning("%s doesn't respect repartition", self)
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
        return self._native_engine.map_engine.map_dataframe(
            df=df,
            map_func=map_func,
            output_schema=output_schema,
            partition_spec=partition_spec,
            metadata=metadata,
            on_init=on_init,
        )

    def broadcast(self, df: DataFrame) -> DataFrame:
        return df

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        if isinstance(df, MockDuckDataFrame):
            return ArrowDataFrame(df.as_arrow(), metadata=df.metadata)
        return self.to_df(df)

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
            (n is None and frac is not None and frac >= 0.0)
            or (frac is None and n is not None and n >= 0),
            ValueError(
                f"one and only one of n and frac should be non-negative, {n}, {frac}"
            ),
        )
        tn = self.get_temp_table_name()
        if frac is not None:
            sql = f"SELECT * FROM {tn} USING SAMPLE bernoulli({frac*100} PERCENT)"
        else:
            sql = f"SELECT * FROM {tn} USING SAMPLE reservoir({n} ROWS)"
        if seed is not None:
            sql += f" REPEATABLE ({seed})"
        idf = self._to_ibis_dataframe(df)
        return self._to_ibis_dataframe(idf.native.alias(tn).sql(sql), metadata=metadata)

    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:
        return self._native_engine.load_df(path, format_hint, columns, **kwargs)

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
        return self._native_engine.save_df(
            df, path, format_hint, mode, partition_spec, force_single, **kwargs
        )

    def _register_df(
        self,
        df: pa.Table,
        name: Optional[str] = None,
        schema: Any = None,
        metadata: Any = None,
    ) -> MockDuckDataFrame:
        tb = self.backend.register(df, name)
        return MockDuckDataFrame(tb, metadata=metadata)
