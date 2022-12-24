import pytest

ibis = pytest.importorskip("ibis")
from fugue_dask import DaskExecutionEngine
from fugue_dask.ibis_engine import DaskIbisEngine
from fugue_ibis import IbisEngine
from fugue_test.ibis_suite import IbisTests


class DaskIbisTests(IbisTests.Tests):
    def make_engine(self):
        e = DaskExecutionEngine(conf=dict(test=True))
        return e

    def make_ibis_engine(self) -> IbisEngine:
        return DaskIbisEngine(self._engine)
