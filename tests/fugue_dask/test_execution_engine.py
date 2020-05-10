from fugue_dask.execution_engine import DaskExecutionEngine
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from fugue.collections.partition import PartitionSpec


class DaskExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = DaskExecutionEngine()
        return e

    def test__join_outer_pandas_incompatible(self):
        return

    def test_repartition(self):
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


class DaskExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = DaskExecutionEngine()
        return e
