import pytest

ibis = pytest.importorskip("ibis")
from fugue_ibis import IbisEngine
from fugue_test.ibis_suite import IbisTests

from fugue_duckdb import DuckExeuctionEngine, DuckDBIbisEngine


class DuckDBIbisTests(IbisTests.Tests):
    def make_engine(self):
        e = DuckExeuctionEngine(dict(test=True))
        return e

    def make_ibis_engine(self) -> IbisEngine:
        return DuckDBIbisEngine(self._engine)
