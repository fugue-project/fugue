import pytest

ibis = pytest.importorskip("ibis")
from fugue_ibis import IbisEngine
from fugue_test.ibis_suite import IbisTests

from fugue_dask import DaskExecutionEngine, DaskIbisEngine


class DaskIbisTests(IbisTests.Tests):
    def make_engine(self):
        e = DaskExecutionEngine(dict(test=True))
        return e

    def make_ibis_engine(self) -> IbisEngine:
        return DaskIbisEngine(self._engine)
