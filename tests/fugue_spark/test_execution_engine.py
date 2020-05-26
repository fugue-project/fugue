import pytest
from fugue.collections.partition import PartitionSpec
from fugue_spark.execution_engine import SparkExecutionEngine
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from pyspark.sql import SparkSession


class SparkExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        spark_session = SparkSession.builder.getOrCreate()
        e = SparkExecutionEngine(spark_session, dict(test=True))
        return e

    @pytest.fixture(autouse=True)
    def init_session(self, spark_session):
        self.spark_session = spark_session

    def test__join_outer_pandas_incompatible(self):
        return

    def test_repartition(self):
        return
        e = self.engine
        a = e.to_df([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]], "a:int,b:int")
        b = e.repartition(a, PartitionSpec())
        assert a is b
        b = e.repartition(a, PartitionSpec(num=3))
        assert 3 == b.num_partitions
        b = e.repartition(a, PartitionSpec(num="0"))
        assert a is b
        b = e.repartition(a, PartitionSpec(num="ROWCOUNT/2"))
        assert 2 == b.num_partitions
        b = e.repartition(a, PartitionSpec(num="ROWCOUNT-ROWCOUNT"))
        assert a is b
        b = e.repartition(a, PartitionSpec(by=["a"], num=3))
        assert a.num_partitions == b.num_partitions


class SparkExecutionEngineBuiltInTests(BuiltInTests.Tests):
    @pytest.fixture(autouse=True)
    def init_session(self, spark_session):
        self.spark_session = spark_session

    def make_engine(self):
        spark_session = SparkSession.builder.getOrCreate()
        e = SparkExecutionEngine(spark_session, dict(test=True))
        return e
