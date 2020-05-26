from typing import Any, Iterable, List

import pytest
from fugue.collections.partition import PartitionSpec
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.extensions.transformer import Transformer, transformer
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

    def _test_repartition(self):
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

    def test_repartition(self):
        with self.dag() as dag:
            a = dag.df([[0, 1], [0, 2], [0, 3], [0, 4], [
                       1, 1], [1, 2], [1, 3]], "a:int,b:int")
            c = a.partition(algo="even", num="ROWCOUNT").transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[1, 1, 1, 1, 1, 1, 1]))
            c = a.partition(algo="even", num="ROWCOUNT/2").transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[3, 2, 2]))
            c = a.partition(algo="even", by=["a"]).transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[3, 4]))
            c = a.partition(algo="even", by=["a"]).transform(AssertMaxNTransform)
            dag.output(c, using=assert_match, params=dict(values=[3, 4]))
            c = a.partition(num=1).transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[7]))
            c = a.partition(algo="rand", num=100).transform(count_partition).persist()

    def test_repartition_large(self):
        with self.dag() as dag:
            a = dag.df([[p, 0] for p in range(100)], "a:int,b:int")
            c = a.partition(algo="even", by=["a"]).transform(
                AssertMaxNTransform).persist()
            c = a.partition(algo="even", num="ROWCOUNT/2", by=["a"]).transform(
                AssertMaxNTransform, params=dict(n=2)).persist()
            c = a.partition(algo="even", num="ROWCOUNT").transform(
                AssertMaxNTransform).persist()
            dag.output(c, using=assert_all_n, params=dict(n=1, l=100))
            c = a.partition(
                algo="even", num="ROWCOUNT/2").transform(AssertMaxNTransform)
            dag.output(c, using=assert_all_n, params=dict(n=2, l=50))
            c = a.partition(num=1).transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[100]))


@transformer("ct:long")
def count_partition(df: List[List[Any]]) -> List[List[Any]]:
    return [[len(df)]]


class AssertMaxNTransform(Transformer):
    def get_output_schema(self, df):
        return "c:int"

    def transform(self, df):
        if not hasattr(self, "called"):
            self.called = 1
        else:
            self.called += 1
        n = self.params.get("n", 1)
        assert self.called <= n
        return ArrayDataFrame([[len(df.as_array())]], "c:int")


def assert_match(df: List[List[Any]], values: List[int]) -> None:
    assert set(values) == set(x[0] for x in df)


def assert_all_n(df: List[List[Any]], n, l) -> None:
    assert all(x[0] == n for x in df)
    assert l == len(df)
