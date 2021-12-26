import pytest

ibis = pytest.importorskip("ibis")
from fugue import NativeExecutionEngine, QPDPandasEngine
from fugue_ibis import IbisEngine
from fugue_ibis.execution.pandas_backend import PandasIbisEngine
from fugue_test.ibis_suite import IbisTests


class PandasIbisTests(IbisTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(QPDPandasEngine(e))
        return e

    def make_ibis_engine(self) -> IbisEngine:
        return PandasIbisEngine(self.engine)
