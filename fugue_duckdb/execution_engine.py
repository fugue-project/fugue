import duckdb
from fugue import (
    DataFrame,
    DataFrames,
    NativeExecutionEngine,
    PandasDataFrame,
    SQLEngine,
)


class DuckDBEngine(SQLEngine):
    """DuckDB SQL backend implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        conn = duckdb.connect()
        try:
            for k, v in dfs.items():
                conn.register(k, v.as_pandas())
            return PandasDataFrame(conn.execute(statement).fetchdf())
        finally:
            conn.close()


class DuckExeuctionEngine(NativeExecutionEngine):
    """:class:`~fugue.execution.native_execution_engine.NativeExecutionEngine`
    with :class:`~fugue_duckdb.execution_engine.DuckDBEngine` as SQL backend

    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    """

    @property
    def default_sql_engine(self) -> SQLEngine:
        return DuckDBEngine(self)
