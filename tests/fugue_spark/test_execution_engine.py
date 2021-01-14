from typing import Any, Iterable, List

import pytest
from fugue.collections.partition import PartitionSpec
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.extensions.transformer import Transformer, transformer
from fugue.workflow.workflow import FugueWorkflow
from fugue_spark.execution_engine import SparkExecutionEngine
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pytest import raises


class SparkExecutionEngineTests(ExecutionEngineTests.Tests):
    @pytest.fixture(autouse=True)
    def init_session(self, spark_session):
        self.spark_session = spark_session

    def make_engine(self):
        session = SparkSession.builder.getOrCreate()
        e = SparkExecutionEngine(session, dict(test=True))
        return e

    def test__join_outer_pandas_incompatible(self):
        return

    def test_to_df(self):
        e = self.engine
        o = ArrayDataFrame(
            [[1, 2]],
            "a:int,b:int",
            dict(a=1),
        )
        a = e.to_df(o)
        assert a is not o
        df_eq(a, o, throw=True)
        a = e.to_df([[1, None]], "a:int,b:int", dict(a=1))
        df_eq(a, [[1, None]], "a:int,b:int", dict(a=1), throw=True)

    def test_persist(self):
        e = self.engine
        o = ArrayDataFrame(
            [[1, 2]],
            "a:int,b:int",
            dict(a=1),
        )
        a = e.persist(o)
        df_eq(a, o, throw=True)
        a = e.persist(o, level=StorageLevel.MEMORY_ONLY)
        df_eq(a, o, throw=True)
        a = e.persist(o, level="MEMORY_ONLY")
        df_eq(a, o, throw=True)
        # this passed because persist is run once on ths same object
        e.persist(a, level="xyz")
        raises(ValueError, lambda: e.persist(o, level="xyz"))

    def test_sample_n(self):
        engine = self.engine
        a = engine.to_df([[x] for x in range(100)], "a:int")

        with raises(NotImplementedError):
            # replace is not allowed
            engine.sample(a, n=90, replace=True, metadata=(dict(a=1)))

        b = engine.sample(a, n=90, metadata=(dict(a=1)))
        assert abs(len(b.as_array()) - 90) < 2
        assert b.metadata == dict(a=1)


class SparkExecutionEnginePandasUDFTests(ExecutionEngineTests.Tests):
    @pytest.fixture(autouse=True)
    def init_session(self, spark_session):
        self.spark_session = spark_session

    def make_engine(self):
        session = SparkSession.builder.getOrCreate()
        e = SparkExecutionEngine(
            session, {"test": True, "fugue.spark.use_pandas_udf": True}
        )
        assert e.conf.get_or_throw("fugue.spark.use_pandas_udf", bool)
        return e

    def test__join_outer_pandas_incompatible(self):
        return

    def test_sample_n(self):
        engine = self.engine
        a = engine.to_df([[x] for x in range(100)], "a:int")

        b = engine.sample(a, n=90, metadata=(dict(a=1)))
        assert abs(len(b.as_array()) - 90) < 2
        assert b.metadata == dict(a=1)


class SparkExecutionEngineBuiltInTests(BuiltInTests.Tests):
    @pytest.fixture(autouse=True)
    def init_session(self, spark_session):
        self.spark_session = spark_session

    def make_engine(self):
        session = SparkSession.builder.getOrCreate()
        e = SparkExecutionEngine(
            session,
            {
                "test": True,
                "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
                "fugue.rpc.flask_server.host": "127.0.0.1",
                "fugue.rpc.flask_server.port": "1234",
                "fugue.rpc.flask_server.timeout": "2 sec",
                "spark.sql.shuffle.partitions": "10",
            },
        )
        return e

    def test_df_init(self):
        sdf = self.spark_session.createDataFrame([[1.1]], "a:double")
        a = FugueWorkflow().df(sdf)
        df_eq(a.compute(SparkExecutionEngine), [[1.1]], "a:double")

    def test_default_session(self):
        a = FugueWorkflow().df([[0]], "a:int")
        df_eq(a.compute(SparkExecutionEngine), [[0]], "a:int")

    def test_repartition(self):
        with self.dag() as dag:
            a = dag.df(
                [[0, 1], [0, 2], [0, 3], [0, 4], [1, 1], [1, 2], [1, 3]], "a:int,b:int"
            )
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
            c = (
                a.partition(algo="even", by=["a"])
                .transform(AssertMaxNTransform)
                .persist()
            )
            c = (
                a.partition(algo="even", num="ROWCOUNT/2", by=["a"])
                .transform(AssertMaxNTransform, params=dict(n=2))
                .persist()
            )
            c = (
                a.partition(algo="even", num="ROWCOUNT")
                .transform(AssertMaxNTransform)
                .persist()
            )
            dag.output(c, using=assert_all_n, params=dict(n=1, l=100))
            c = a.partition(algo="even", num="ROWCOUNT/2").transform(
                AssertMaxNTransform
            )
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
