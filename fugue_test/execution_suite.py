from unittest import TestCase

from fugue.collections.partition import PartitionSpec
from fugue.dataframe import ArrayDataFrame
from fugue.execution.execution_engine import ExecutionEngine
from fugue.dataframe import df_eq


class ExecutionEngineTests(object):
    class Tests(TestCase):
        @classmethod
        def setUpClass(cls):
            cls._engine = cls.make_engine(cls)

        @property
        def engine(self) -> ExecutionEngine:
            return self._engine  # type: ignore

        @classmethod
        def tearDownClass(cls):
            cls._engine.stop()

        def make_engine(self) -> ExecutionEngine:  # pragma: no cover
            raise NotImplementedError

        def test_map_partitions(self):
            def select_top(no, data):
                for x in data:
                    yield x
                    break

            e = self.engine
            a = e.to_df(
                ArrayDataFrame([[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int")
            )
            c = e.map_partitions(a, select_top, a.schema, PartitionSpec())
            df_eq(c, [[1, 2]], "a:double,b:int", throw=True)
            c = e.map_partitions(
                a, select_top, a.schema, PartitionSpec(partition_by=["a"])
            )
            df_eq(c, [[None, 1], [1, 2], [3, 4]], "a:double,b:int", throw=True)
            c = e.map_partitions(
                a,
                select_top,
                a.schema,
                PartitionSpec(partition_by=["a"], presort="b DESC"),
            )
            df_eq(c, [[None, 4], [1, 2], [3, 4]], "a:double,b:int", throw=True)
            c = e.map_partitions(
                a,
                select_top,
                a.schema,
                PartitionSpec(partition_by=["a"], presort="b DESC", num_partitions=3),
            )
            df_eq(c, [[None, 4], [1, 2], [3, 4]], "a:double,b:int", throw=True)
