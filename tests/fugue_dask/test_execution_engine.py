from fugue.collections.partition import PartitionSpec
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_dask.execution_engine import DaskExecutionEngine
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.workflow.workflow import FugueWorkflow


class DaskExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = DaskExecutionEngine(dict(test=True))
        return e

    def test__join_outer_pandas_incompatible(self):
        return

    def test_map_with_dict_col(self):
        # TODO: add back
        return

    def test_to_df(self):
        e = self.engine
        a = e.to_df([[1, 2], [3, 4]], "a:int,b:int", dict(a=1))
        df_eq(a, [[1, 2], [3, 4]], "a:int,b:int", dict(a=1), throw=True)
        a = e.to_df(PandasDataFrame([[1, 2], [3, 4]], "a:int,b:int", dict(a=1)))
        df_eq(a, [[1, 2], [3, 4]], "a:int,b:int", dict(a=1), throw=True)
        assert a is e.to_df(a)

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

    def test_sample_n(self):
        # TODO: dask does not support sample by number of rows
        pass


class DaskExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = DaskExecutionEngine(dict(test=True))
        return e

    def test_default_init(self):
        a = FugueWorkflow().df([[0]], "a:int")
        df_eq(a.compute(DaskExecutionEngine), [[0]], "a:int")
