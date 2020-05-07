from unittest import TestCase

from fugue.collections.partition import PartitionSpec
from fugue.dataframe import ArrayDataFrame
from fugue.execution.execution_engine import ExecutionEngine
from fugue.dataframe.utils import _df_eq as df_eq


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

        def test__join_cross(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
            b = e.to_df([[6], [7]], "c:int")
            c = e.join(a, b, how="Cross")
            df_eq(c, [[1, 2, 6], [1, 2, 7], [3, 4, 6], [3, 4, 7]], "a:int,b:int,c:int")

            b = e.to_df([], "c:int")
            c = e.join(a, b, how="Cross")
            df_eq(c, [], "a:int,b:int,c:int", throw=True)

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:int")
            c = e.join(a, b, how="Cross")
            df_eq(c, [], "a:int,b:int,c:int", throw=True)

        def test__join_inner(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
            b = e.to_df([[6, 1], [2, 7]], "c:int,a:int")
            c = e.join(a, b, how="INNER", keys=["a"])
            df_eq(c, [[1, 2, 6]], "a:int,b:int,c:int", throw=True)
            c = e.join(b, a, how="INNER", keys=["a"])
            df_eq(c, [[6, 1, 2]], "c:int,a:int,b:int", throw=True)

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="INNER", keys=["a"])
            df_eq(c, [], "a:int,b:int,c:int", throw=True)

        def test__join_outer(self):
            e = self.engine

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:str,a:int")
            c = e.join(a, b, how="left_outer", keys=["a"])
            df_eq(c, [], "a:int,b:int,c:str", throw=True)

            a = e.to_df([], "a:int,b:str")
            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="right_outer", keys=["a"])
            df_eq(c, [], "a:int,b:str,c:int", throw=True)

            a = e.to_df([], "a:int,b:str")
            b = e.to_df([], "c:str,a:int")
            c = e.join(a, b, how="full_outer", keys=["a"])
            df_eq(c, [], "a:int,b:str,c:str", throw=True)

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([["6", 1], ["2", 7]], "c:str,a:int")
            c = e.join(a, b, how="left_OUTER", keys=["a"])
            df_eq(c, [[1, "2", "6"], [3, "4", None]], "a:int,b:str,c:str", throw=True)
            c = e.join(b, a, how="left_outer", keys=["a"])
            df_eq(c, [["6", 1, "2"], ["2", 7, None]], "c:str,a:int,b:str", throw=True)

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([["6", 1], ["2", 7]], "c:double,a:int")
            c = e.join(a, b, how="left_OUTER", keys=["a"])
            df_eq(
                c, [[1, "2", 6.0], [3, "4", None]], "a:int,b:str,c:double", throw=True
            )
            c = e.join(b, a, how="left_outer", keys=["a"])
            df_eq(
                c, [[6.0, 1, "2"], [2.0, 7, None]], "c:double,a:int,b:str", throw=True
            )

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([["6", 1], ["2", 7]], "c:int,a:int")
            c = e.join(a, b, how="left_OUTER", keys=["a"])
            df_eq(c, [[1, "2", 6], [3, "4", None]], "a:int,b:str,c:int", throw=True)
            c = e.join(b, a, how="left_outer", keys=["a"])
            df_eq(c, [[6, 1, "2"], [2, 7, None]], "c:int,a:int,b:str", throw=True)

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([[True, 1], [False, 7]], "c:bool,a:int")
            c = e.join(a, b, how="left_OUTER", keys=["a"])
            df_eq(c, [[1, "2", True], [3, "4", None]], "a:int,b:str,c:bool", throw=True)
            c = e.join(b, a, how="left_outer", keys=["a"])
            df_eq(
                c, [[True, 1, "2"], [False, 7, None]], "c:bool,a:int,b:str", throw=True
            )

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([["6", 1], ["2", 7]], "c:str,a:int")
            c = e.join(a, b, how="right_outer", keys=["a"])
            df_eq(c, [[1, "2", "6"], [7, None, "2"]], "a:int,b:str,c:str", throw=True)

            c = e.join(a, b, how="full_outer", keys=["a"])
            df_eq(
                c,
                [[1, "2", "6"], [3, "4", None], [7, None, "2"]],
                "a:int,b:str,c:str",
                throw=True,
            )

        def test__join_semi(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
            b = e.to_df([[6, 1], [2, 7]], "c:int,a:int")
            c = e.join(a, b, how="semi", keys=["a"])
            df_eq(c, [[1, 2]], "a:int,b:int", throw=True)
            c = e.join(b, a, how="semi", keys=["a"])
            df_eq(c, [[6, 1]], "c:int,a:int", throw=True)

            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="semi", keys=["a"])
            df_eq(c, [], "a:int,b:int", throw=True)

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="semi", keys=["a"])
            df_eq(c, [], "a:int,b:int", throw=True)

        def test__join_anti(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
            b = e.to_df([[6, 1], [2, 7]], "c:int,a:int")
            c = e.join(a, b, how="anti", keys=["a"])
            df_eq(c, [[3, 4]], "a:int,b:int", throw=True)
            c = e.join(b, a, how="anti", keys=["a"])
            df_eq(c, [[2, 7]], "c:int,a:int", throw=True)

            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="anti", keys=["a"])
            df_eq(c, [[1, 2], [3, 4]], "a:int,b:int", throw=True)

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="anti", keys=["a"])
            df_eq(c, [], "a:int,b:int", throw=True)
