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
    SQLEngine,
)
from fugue_ibis import IbisDataFrame, IbisExecutionEngine, IbisSQLEngine, IbisTable

from .dataframe import MockDuckDataFrame


class MockDuckSQLEngine(IbisSQLEngine):
    def __init__(self, execution_engine: ExecutionEngine) -> None:
        super().__init__(execution_engine)
        self._backend = ibis.duckdb.connect()

    @property
    def backend(self) -> ibis.BaseBackend:
        return self._backend

    @property
    def dialect(self) -> str:
        return "duckdb"

    @property
    def is_distributed(self) -> bool:
        return False

    def encode_column_name(self, name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def to_df(self, df: Any, schema: Any = None) -> IbisDataFrame:
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

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
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
        idf = self.to_df(df)
        _res = f"WITH {tn} AS ({idf.native.compile()}) " + sql
        return self.to_df(self.backend.sql(_res))

    def _register_df(
        self, df: pa.Table, name: Optional[str] = None, schema: Any = None
    ) -> MockDuckDataFrame:
        tb = self.backend.register(df, name)
        return MockDuckDataFrame(tb)


class MockDuckExecutionEngine(IbisExecutionEngine):
    def __init__(self, conf: Any, force_is_ibis: bool = False):
        super().__init__(conf)

        self._force_is_ibis = force_is_ibis

    @property
    def is_distributed(self) -> bool:
        return False

    def create_non_ibis_execution_engine(self) -> ExecutionEngine:
        return NativeExecutionEngine(self.conf)

    def create_default_sql_engine(self) -> SQLEngine:
        return MockDuckSQLEngine(self)

    def is_non_ibis(self, ds: Any) -> bool:
        if self._force_is_ibis:
            return False
        return super().is_non_ibis(ds)

    def __repr__(self) -> str:
        return "MockDuckExecutionEngine"

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
