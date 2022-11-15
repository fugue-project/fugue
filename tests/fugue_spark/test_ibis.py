import pytest

ibis = pytest.importorskip("ibis")
from fugue_ibis import IbisEngine
from fugue_test.ibis_suite import IbisTests
from pyspark.sql import SparkSession

from fugue_spark import SparkExecutionEngine, SparkIbisEngine


class SparkIbisTests(IbisTests.Tests):
    @pytest.fixture(autouse=True)
    def init_session(self, spark_session):
        self.spark_session = spark_session

    def make_engine(self):
        session = SparkSession.builder.getOrCreate()
        e = SparkExecutionEngine(session, dict(test=True))
        return e

    def make_ibis_engine(self) -> IbisEngine:
        return SparkIbisEngine(self._engine)
