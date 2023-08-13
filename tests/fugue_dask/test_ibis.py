import pytest

ibis = pytest.importorskip("ibis")
from fugue_dask import DaskExecutionEngine
from fugue_dask.ibis_engine import DaskIbisEngine
from fugue_ibis import IbisEngine
from fugue_test.ibis_suite import IbisTests


class DaskIbisTests(IbisTests.Tests):
    @pytest.fixture(autouse=True)
    def init_client(self, fugue_dask_client):
        self.dask_client = fugue_dask_client

    def make_engine(self):
        e = DaskExecutionEngine(self.dask_client, conf=dict(test=True))
        return e

    def make_ibis_engine(self) -> IbisEngine:
        return DaskIbisEngine(self._engine)
