import copy
from unittest import TestCase

from fugue.collections.partition import PartitionSpec
from fugue.dataframe import ArrayDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.execution.execution_engine import ExecutionEngine
from pytest import raises
from triad.exceptions import InvalidOperationError


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

        def test_init(self):
            print(self.engine)
            assert self.engine.log is not None
            assert self.engine.fs is not None
            assert copy.copy(self.engine) is self.engine
            assert copy.deepcopy(self.engine) is self.engine

        def test_map(self):
            def noop(cursor, data):
                return data

            def select_top(cursor, data):
                return ArrayDataFrame([cursor.row], cursor.row_schema)

            def on_init(partition_no, data):
                # TODO: this test is not sufficient
                assert partition_no >= 0
                data.peek_array()

            e = self.engine
            o = ArrayDataFrame(
                [[1, 2], [None, 2], [None, 1], [3, 4], [None, 4]],
                "a:double,b:int",
                dict(a=1),
            )
            a = e.to_df(o)
            # no partition
            c = e.map(a, noop, a.schema, PartitionSpec(), dict(a=1))
            df_eq(c, o, throw=True)
            # with key partition
            c = e.map(
                a, noop, a.schema, PartitionSpec(by=["a"], presort="b"), dict(a=1)
            )
            df_eq(c, o, throw=True)
            # select top
            c = e.map(a, select_top, a.schema, PartitionSpec(by=["a"], presort="b"))
            df_eq(c, [[None, 1], [1, 2], [3, 4]], "a:double,b:int", throw=True)
            # select top with another order
            c = e.map(
                a,
                select_top,
                a.schema,
                PartitionSpec(partition_by=["a"], presort="b DESC"),
                metadata=dict(a=1),
            )
            df_eq(
                c,
                [[None, 4], [1, 2], [3, 4]],
                "a:double,b:int",
                metadata=dict(a=1),
                throw=True,
            )
            # add num_partitions, on_init should not matter
            c = e.map(
                a,
                select_top,
                a.schema,
                PartitionSpec(partition_by=["a"], presort="b DESC", num_partitions=3),
                on_init=on_init,
            )
            df_eq(c, [[None, 4], [1, 2], [3, 4]], "a:double,b:int", throw=True)

        def test__join_cross(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
            b = e.to_df([[6], [7]], "c:int")
            c = e.join(a, b, how="Cross", metadata=dict(a=1))
            df_eq(
                c,
                [[1, 2, 6], [1, 2, 7], [3, 4, 6], [3, 4, 7]],
                "a:int,b:int,c:int",
                dict(a=1),
            )

            b = e.to_df([], "c:int")
            c = e.join(a, b, how="Cross", metadata=dict(a=1))
            df_eq(c, [], "a:int,b:int,c:int", metadata=dict(a=1), throw=True)

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:int")
            c = e.join(a, b, how="Cross")
            df_eq(c, [], "a:int,b:int,c:int", throw=True)

        def test__join_inner(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
            b = e.to_df([[6, 1], [2, 7]], "c:int,a:int")
            c = e.join(a, b, how="INNER", on=["a"], metadata=dict(a=1))
            df_eq(c, [[1, 2, 6]], "a:int,b:int,c:int", metadata=dict(a=1), throw=True)
            c = e.join(b, a, how="INNER", on=["a"])
            df_eq(c, [[6, 1, 2]], "c:int,a:int,b:int", throw=True)

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="INNER", on=["a"])
            df_eq(c, [], "a:int,b:int,c:int", throw=True)

        def test__join_outer(self):
            e = self.engine

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:str,a:int")
            c = e.join(a, b, how="left_outer", on=["a"], metadata=dict(a=1))
            df_eq(c, [], "a:int,b:int,c:str", metadata=dict(a=1), throw=True)

            a = e.to_df([], "a:int,b:str")
            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="right_outer", on=["a"])
            df_eq(c, [], "a:int,b:str,c:int", throw=True)

            a = e.to_df([], "a:int,b:str")
            b = e.to_df([], "c:str,a:int")
            c = e.join(a, b, how="full_outer", on=["a"])
            df_eq(c, [], "a:int,b:str,c:str", throw=True)

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([["6", 1], ["2", 7]], "c:str,a:int")
            c = e.join(a, b, how="left_OUTER", on=["a"])
            df_eq(c, [[1, "2", "6"], [3, "4", None]], "a:int,b:str,c:str", throw=True)
            c = e.join(b, a, how="left_outer", on=["a"])
            df_eq(c, [["6", 1, "2"], ["2", 7, None]], "c:str,a:int,b:str", throw=True)

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([["6", 1], ["2", 7]], "c:double,a:int")
            c = e.join(a, b, how="left_OUTER", on=["a"])
            df_eq(
                c, [[1, "2", 6.0], [3, "4", None]], "a:int,b:str,c:double", throw=True
            )
            c = e.join(b, a, how="left_outer", on=["a"])
            assert c.as_pandas().values.tolist()[1][2] is None
            df_eq(
                c, [[6.0, 1, "2"], [2.0, 7, None]], "c:double,a:int,b:str", throw=True
            )

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([["6", 1], ["2", 7]], "c:str,a:int")
            c = e.join(a, b, how="right_outer", on=["a"])
            assert c.as_pandas().values.tolist()[1][1] is None
            df_eq(c, [[1, "2", "6"], [7, None, "2"]], "a:int,b:str,c:str", throw=True)

            c = e.join(a, b, how="full_outer", on=["a"])
            df_eq(
                c,
                [[1, "2", "6"], [3, "4", None], [7, None, "2"]],
                "a:int,b:str,c:str",
                throw=True,
            )

        def test__join_outer_pandas_incompatible(self):
            e = self.engine

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([["6", 1], ["2", 7]], "c:int,a:int")
            c = e.join(a, b, how="left_OUTER", on=["a"], metadata=dict(a=1))
            df_eq(
                c,
                [[1, "2", 6], [3, "4", None]],
                "a:int,b:str,c:int",
                metadata=dict(a=1),
                throw=True,
            )
            c = e.join(b, a, how="left_outer", on=["a"])
            df_eq(c, [[6, 1, "2"], [2, 7, None]], "c:int,a:int,b:str", throw=True)

            a = e.to_df([[1, "2"], [3, "4"]], "a:int,b:str")
            b = e.to_df([[True, 1], [False, 7]], "c:bool,a:int")
            c = e.join(a, b, how="left_OUTER", on=["a"])
            df_eq(c, [[1, "2", True], [3, "4", None]], "a:int,b:str,c:bool", throw=True)
            c = e.join(b, a, how="left_outer", on=["a"])
            df_eq(
                c, [[True, 1, "2"], [False, 7, None]], "c:bool,a:int,b:str", throw=True
            )

        def test__join_semi(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
            b = e.to_df([[6, 1], [2, 7]], "c:int,a:int")
            c = e.join(a, b, how="semi", on=["a"], metadata=dict(a=1))
            df_eq(c, [[1, 2]], "a:int,b:int", metadata=dict(a=1), throw=True)
            c = e.join(b, a, how="semi", on=["a"])
            df_eq(c, [[6, 1]], "c:int,a:int", throw=True)

            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="semi", on=["a"])
            df_eq(c, [], "a:int,b:int", throw=True)

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="semi", on=["a"])
            df_eq(c, [], "a:int,b:int", throw=True)

        def test__join_anti(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
            b = e.to_df([[6, 1], [2, 7]], "c:int,a:int")
            c = e.join(a, b, how="anti", metadata=dict(a=1), on=["a"])
            df_eq(c, [[3, 4]], "a:int,b:int", metadata=dict(a=1), throw=True)
            c = e.join(b, a, how="anti", on=["a"])
            df_eq(c, [[2, 7]], "c:int,a:int", throw=True)

            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="anti", on=["a"])
            df_eq(c, [[1, 2], [3, 4]], "a:int,b:int", throw=True)

            a = e.to_df([], "a:int,b:int")
            b = e.to_df([], "c:int,a:int")
            c = e.join(a, b, how="anti", on=["a"])
            df_eq(c, [], "a:int,b:int", throw=True)

        def test_serialize_by_partition(self):
            e = self.engine
            a = e.to_df([[1, 2], [3, 4], [1, 5]], "a:int,b:int")
            s = e.serialize_by_partition(
                a, PartitionSpec(by=["a"], presort="b"), df_name="_0"
            )
            assert s.count() == 2
            s = e.persist(e.serialize_by_partition(a, PartitionSpec(), df_name="_0"))
            print(s.count())
            assert s.count() == 1
            s = e.persist(
                e.serialize_by_partition(a, PartitionSpec(by=["x"]), df_name="_0")
            )
            assert s.count() == 1

        def test_zip_dataframes(self):
            ps = PartitionSpec(by=["a"], presort="b DESC,c DESC")
            e = self.engine
            a = e.to_df([[1, 2], [3, 4], [1, 5]], "a:int,b:int")
            b = e.to_df([[6, 1], [2, 7]], "c:int,a:int")
            sa = e.serialize_by_partition(a, ps, df_name="_0")
            sb = e.serialize_by_partition(b, ps, df_name="_1")
            # test zip_dataframes with serialized dfs
            z1 = e.persist(e.zip_dataframes(sa, sb, how="inner", partition_spec=ps))
            assert 1 == z1.count()
            z2 = e.persist(
                e.zip_dataframes(sa, sb, how="left_outer", partition_spec=ps)
            )
            assert 2 == z2.count()

            # can't have duplicated keys
            raises(
                ValueError,
                lambda: e.zip_dataframes(sa, sa, how="inner", partition_spec=ps),
            )
            # not support semi or anti
            raises(
                InvalidOperationError,
                lambda: e.zip_dataframes(sa, sa, how="anti", partition_spec=ps),
            )
            raises(
                InvalidOperationError,
                lambda: e.zip_dataframes(sa, sa, how="leftsemi", partition_spec=ps),
            )
            raises(
                InvalidOperationError,
                lambda: e.zip_dataframes(sa, sa, how="LEFT SEMI", partition_spec=ps),
            )
            # can't specify keys for cross join
            raises(
                InvalidOperationError,
                lambda: e.zip_dataframes(sa, sa, how="cross", partition_spec=ps),
            )

            # test zip_dataframes with unserialized dfs
            z3 = e.persist(e.zip_dataframes(a, b, partition_spec=ps))
            df_eq(z1, z3, throw=True, check_metadata=False)
            z3 = e.persist(e.zip_dataframes(a, sb, partition_spec=ps))
            df_eq(z1, z3, throw=True, check_metadata=False)
            z3 = e.persist(e.zip_dataframes(sa, b, partition_spec=ps))
            df_eq(z1, z3, throw=True, check_metadata=False)

            z4 = e.persist(e.zip_dataframes(a, b, how="left_outer", partition_spec=ps))
            df_eq(z2, z4, throw=True, check_metadata=False)
            z4 = e.persist(e.zip_dataframes(a, sb, how="left_outer", partition_spec=ps))
            df_eq(z2, z4, throw=True, check_metadata=False)
            z4 = e.persist(e.zip_dataframes(sa, b, how="left_outer", partition_spec=ps))
            df_eq(z2, z4, throw=True, check_metadata=False)

            z5 = e.persist(e.zip_dataframes(a, b, how="cross"))
            assert z5.count() == 1
            assert len(z5.schema) == 2
            z6 = e.persist(e.zip_dataframes(sa, b, how="cross"))
            assert z6.count() == 2
            assert len(z6.schema) == 3

        def test_comap(self):
            ps = PartitionSpec(presort="b,c")
            e = self.engine
            a = e.to_df([[1, 2], [3, 4], [1, 5]], "a:int,b:int")
            b = e.to_df([[6, 1], [2, 7]], "c:int,a:int")
            z1 = e.persist(e.zip_dataframes(a, b))
            z2 = e.persist(e.zip_dataframes(a, b, partition_spec=ps, how="left_outer"))
            z3 = e.persist(e.serialize_by_partition(a, partition_spec=ps, df_name="_x"))
            z4 = e.persist(e.zip_dataframes(a, b, partition_spec=ps, how="cross"))

            def comap(cursor, dfs):
                assert not dfs.has_key
                v = ",".join([k + str(v.count()) for k, v in dfs.items()])
                keys = cursor.key_value_array
                if len(keys) == 0:
                    return ArrayDataFrame([[v]], "v:str")
                return ArrayDataFrame([keys + [v]], cursor.key_schema + "v:str")

            def on_init(partition_no, dfs):
                assert partition_no >= 0
                assert len(dfs) > 0

            res = e.comap(
                z1,
                comap,
                "a:int,v:str",
                PartitionSpec(),
                metadata=dict(a=1),
                on_init=on_init,
            )
            df_eq(res, [[1, "_02,_11"]], "a:int,v:str", metadata=dict(a=1), throw=True)

            # for outer joins, the NULL will be filled with empty dataframe
            res = e.comap(z2, comap, "a:int,v:str", PartitionSpec(), metadata=dict(a=1))
            df_eq(
                res,
                [[1, "_02,_11"], [3, "_01,_10"]],
                "a:int,v:str",
                metadata=dict(a=1),
                throw=True,
            )

            res = e.comap(z3, comap, "v:str", PartitionSpec(), metadata=dict(a=1))
            df_eq(res, [["_03"]], "v:str", metadata=dict(a=1), throw=True)

            res = e.comap(z4, comap, "v:str", PartitionSpec(), metadata=dict(a=1))
            df_eq(res, [["_03,_12"]], "v:str", metadata=dict(a=1), throw=True)
