from typing import Any, Iterable, List, Optional, Union

import ibis
import pyarrow as pa
from triad import assert_or_throw

from fugue import (
    ArrowDataFrame,
    DataFrame,
    ExecutionEngine,
    NativeExecutionEngine,
    PartitionSpec,
)
from fugue_ibis import IbisDataFrame, IbisExecutionEngine, IbisTable

from .dataframe import MockDuckDataFrame


class MockDuckExecutionEngine(IbisExecutionEngine):
    def __init__(self, conf: Any, force_is_ibis: bool = False):
        super().__init__(conf)
        self._backend = ibis.duckdb.connect()
        self._force_is_ibis = force_is_ibis

    @property
    def dialect(self) -> str:
        return "duckdb"

    def create_non_ibis_execution_engine(self) -> ExecutionEngine:
        return NativeExecutionEngine(self.conf)

    def is_non_ibis(self, ds: Any) -> bool:
        if self._force_is_ibis:
            return False
        return super().is_non_ibis(ds)

    @property
    def backend(self) -> ibis.BaseBackend:
        return self._backend

    def encode_column_name(self, name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _to_ibis_dataframe(self, df: Any, schema: Any = None) -> IbisDataFrame:
        if isinstance(df, MockDuckDataFrame):
            return df
        if isinstance(df, DataFrame):
            return self._register_df(
                df.as_arrow(), schema=schema if schema is not None else df.schema
            )
        if isinstance(df, pa.Table):
            return self._register_df(df, schema=schema)
        if isinstance(df, IbisTable):
            return MockDuckDataFrame(df, schema=schema)
        if isinstance(df, Iterable):
            adf = ArrowDataFrame(df, schema)
            return self._register_df(adf.native, schema=schema)
        raise NotImplementedError

    def _to_non_ibis_dataframe(self, df: Any, schema: Any = None) -> DataFrame:
        return self.non_ibis_engine.to_df(df, schema)

    def __repr__(self) -> str:
        return "MockDuckExecutionEngine"

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.persist(df, lazy=lazy, **kwargs)

        if isinstance(df, MockDuckDataFrame):
            res = ArrowDataFrame(df.as_arrow())
        else:
            res = self.to_df(df)
        res.reset_metadata(df.metadata)
        return res

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:
        if self.is_non_ibis(df):
            return self.non_ibis_engine.sample(
                df, n=n, frac=frac, replace=replace, seed=seed
            )

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
        return self._to_ibis_dataframe(idf.native.alias(tn).sql(sql))

    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:
        return self.non_ibis_engine.load_df(path, format_hint, columns, **kwargs)

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
        _df = self._to_non_ibis_dataframe(df)
        return self.non_ibis_engine.save_df(
            _df, path, format_hint, mode, partition_spec, force_single, **kwargs
        )

    def _register_df(
        self, df: pa.Table, name: Optional[str] = None, schema: Any = None
    ) -> MockDuckDataFrame:
        tb = self.backend.register(df, name)
        return MockDuckDataFrame(tb)
