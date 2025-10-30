# pylint: disable-all
try:
    import qpd_pandas  # noqa: F401

    HAS_QPD = True
except ImportError:  # pragma: no cover
    HAS_QPD = False

import copy
import os
import pickle
import sys
from datetime import datetime

import pandas as pd
import pytest
from pytest import raises
from triad.exceptions import InvalidOperationError
from triad.utils.io import isfile, makedirs, touch

import fugue.api as fa
import fugue.column.functions as ff
import fugue.test as ft
from fugue import (
    ArrayDataFrame,
    DataFrame,
    DataFrames,
    PandasDataFrame,
    PartitionSpec,
)
from fugue.column import all_cols, col, lit
from fugue.execution.native_execution_engine import NativeExecutionEngine


class ExecutionEngineTests(object):
    """ExecutionEngine level general test suite.
    Any new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    should pass this test suite.
    """

    class Tests(ft.FugueTestSuite):
        def test_init(self):
            print(self.engine)
            assert self.engine.log is not None
            assert copy.copy(self.engine) is self.engine
            assert copy.deepcopy(self.engine) is self.engine

        def test_get_parallelism(self):
            assert fa.get_current_parallelism() == 1

        def test_to_df_general(self):
            e = self.engine
            o = ArrayDataFrame(
                [[1.1, 2.2], [3.3, 4.4]],
                "a:double,b:double",
            )
            # all engines should accept these types of inputs
            # should take fugue.DataFrame
            self.df_eq(o, fa.as_fugue_engine_df(e, o), throw=True)
            # should take array, shema
            self.df_eq(
                o,
                fa.as_fugue_engine_df(e, [[1.1, 2.2], [3.3, 4.4]], "a:double,b:double"),
                throw=True,
            )
            # should take pandas dataframe
            pdf = pd.DataFrame([[1.1, 2.2], [3.3, 4.4]], columns=["a", "b"])
            self.df_eq(o, fa.as_fugue_engine_df(e, pdf), throw=True)

            # should convert string to datetime in to_df
            self.df_eq(
                fa.as_fugue_engine_df(e, [["2020-01-01"]], "a:datetime"),
                [[datetime(2020, 1, 1)]],
                "a:datetime",
                throw=True,
            )

            # should handle empty pandas dataframe
            o = ArrayDataFrame([], "a:double,b:str")
            pdf = pd.DataFrame([[0.1, "a"]], columns=["a", "b"])
            pdf = pdf[pdf.a < 0]
            self.df_eq(o, fa.as_fugue_engine_df(e, pdf), throw=True)

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_filter(self):
            a = ArrayDataFrame(
                [[1, 2], [None, 2], [None, 1], [3, 4], [None, 4]],
                "a:double,b:int",
            )
            b = fa.filter(a, col("a").not_null())
            self.df_eq(b, [[1, 2], [3, 4]], "a:double,b:int", throw=True)
            c = fa.filter(a, col("a").not_null() & (col("b") < 3))
            self.df_eq(c, [[1, 2]], "a:double,b:int", throw=True)
            c = fa.filter(a, col("a") + col("b") == 3)
            self.df_eq(c, [[1, 2]], "a:double,b:int", throw=True)

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_select(self):
            a = ArrayDataFrame(
                [[1, 2], [None, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int"
            )

            # simple
            b = fa.select(a, col("b"), (col("b") + 1).alias("c").cast(str))
            self.df_eq(
                b,
                [[2, "3"], [2, "3"], [1, "2"], [4, "5"], [4, "5"]],
                "b:int,c:str",
                throw=True,
            )

            # with distinct
            b = fa.select(
                a, col("b"), (col("b") + 1).alias("c").cast(str), distinct=True
            )
            self.df_eq(
                b,
                [[2, "3"], [1, "2"], [4, "5"]],
                "b:int,c:str",
                throw=True,
            )

            # wildcard
            b = fa.select(a, all_cols(), where=col("a") + col("b") == 3)
            self.df_eq(b, [[1, 2]], "a:double,b:int", throw=True)

            # aggregation
            b = fa.select(a, col("a"), ff.sum(col("b")).cast(float).alias("b"))
            self.df_eq(b, [[1, 2], [3, 4], [None, 7]], "a:double,b:double", throw=True)

            # having
            # https://github.com/fugue-project/fugue/issues/222
            col_b = ff.sum(col("b"))
            b = fa.select(
                a,
                col("a"),
                col_b.cast(float).alias("c"),
                having=(col_b >= 7) | (col("a") == 1),
            )
            self.df_eq(b, [[1, 2], [None, 7]], "a:double,c:double", throw=True)

            # literal + alias inference
            # https://github.com/fugue-project/fugue/issues/222
            col_b = ff.sum(col("b"))
            b = fa.select(
                a,
                col("a"),
                lit(1, "o").cast(str),
                col_b.cast(float).alias("c"),
                having=(col_b >= 7) | (col("a") == 1),
            )
            self.df_eq(
                b, [[1, "1", 2], [None, "1", 7]], "a:double,o:str,c:double", throw=True
            )

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_assign(self):
            a = ArrayDataFrame(
                [[1, 2], [None, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int"
            )

            b = fa.assign(a, x=1, b=col("b").cast(str), c=(col("b") + 1).cast(int))
            self.df_eq(
                b,
                [
                    [1, "2", 1, 3],
                    [None, "2", 1, 3],
                    [None, "1", 1, 2],
                    [3, "4", 1, 5],
                    [None, "4", 1, 5],
                ],
                "a:double,b:str,x:long,c:long",
                throw=True,
            )

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_aggregate(self):
            a = ArrayDataFrame(
                [[1, 2], [None, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int"
            )

            b = fa.aggregate(
                df=a,
                b=ff.max(col("b")),
                c=(ff.max(col("b")) * 2).cast("int32").alias("c"),
            )
            self.df_eq(b, [[4, 8]], "b:int,c:int", throw=True)

            b = fa.aggregate(
                a,
                "a",
                b=ff.max(col("b")),
                c=(ff.max(col("b")) * 2).cast("int32").alias("c"),
            )
            self.df_eq(
                b,
                [[None, 4, 8], [1, 2, 4], [3, 4, 8]],
                "a:double,b:int,c:int",
                throw=True,
            )

            with raises(ValueError):
                fa.aggregate(a, "a", b=ff.max(col("b")), x=1)

            with raises(ValueError):
                fa.aggregate(a, "a")

        def test_map(self):
            def noop(cursor, data):
                return data

            def on_init(partition_no, data):
                # TODO: this test is not sufficient
                assert partition_no >= 0
                data.peek_array()

            e = self.engine
            o = ArrayDataFrame(
                [[1, 2], [None, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int"
            )
            a = fa.as_fugue_engine_df(e, o)
            # no partition
            c = e.map_engine.map_dataframe(a, noop, a.schema, PartitionSpec())
            self.df_eq(c, o, throw=True)
            # with key partition
            c = e.map_engine.map_dataframe(
                a, noop, a.schema, PartitionSpec(by=["a"], presort="b")
            )
            self.df_eq(c, o, throw=True)
            # select top
            c = e.map_engine.map_dataframe(
                a, select_top, a.schema, PartitionSpec(by=["a"], presort="b")
            )
            self.df_eq(c, [[None, 1], [1, 2], [3, 4]], "a:double,b:int", throw=True)
            # select top with another order
            c = e.map_engine.map_dataframe(
                a,
                select_top,
                a.schema,
                PartitionSpec(partition_by=["a"], presort="b DESC"),
            )
            self.df_eq(
                c,
                [[None, 4], [1, 2], [3, 4]],
                "a:double,b:int",
                throw=True,
            )
            # add num_partitions, on_init should not matter
            c = e.map_engine.map_dataframe(
                a,
                select_top,
                a.schema,
                PartitionSpec(partition_by=["a"], presort="b DESC", num_partitions=3),
                on_init=on_init,
            )
            self.df_eq(c, [[None, 4], [1, 2], [3, 4]], "a:double,b:int", throw=True)

        def test_map_with_special_values(self):
            def with_nat(cursor, data):
                df = data.as_pandas()
                df["nat"] = pd.NaT
                schema = data.schema + "nat:datetime"
                return PandasDataFrame(df, schema)

            e = self.engine
            # test with multiple key with null values
            o = ArrayDataFrame(
                [[1, None, 1], [1, None, 0], [None, None, 2]], "a:double,b:double,c:int"
            )
            c = e.map_engine.map_dataframe(
                o, select_top, o.schema, PartitionSpec(by=["a", "b"], presort="c")
            )
            self.df_eq(
                c,
                [[1, None, 0], [None, None, 2]],
                "a:double,b:double,c:int",
                throw=True,
            )
            # test datetime with nat
            dt = datetime.now()
            o = ArrayDataFrame(
                [
                    [dt, 2, 1],
                    [None, 2, None],
                    [None, 1, None],
                    [dt, 5, 1],
                    [None, 4, None],
                ],
                "a:datetime,b:int,c:double",
            )
            c = e.map_engine.map_dataframe(
                o, select_top, o.schema, PartitionSpec(by=["a", "c"], presort="b DESC")
            )
            self.df_eq(
                c,
                [[None, 4, None], [dt, 5, 1]],
                "a:datetime,b:int,c:double",
                throw=True,
            )
            d = e.map_engine.map_dataframe(
                c, with_nat, "a:datetime,b:int,c:double,nat:datetime", PartitionSpec()
            )
            self.df_eq(
                d,
                [[None, 4, None, None], [dt, 5, 1, None]],
                "a:datetime,b:int,c:double,nat:datetime",
                throw=True,
            )
            # test list
            o = ArrayDataFrame([[dt, [1, 2]]], "a:datetime,b:[int]")
            c = e.map_engine.map_dataframe(
                o, select_top, o.schema, PartitionSpec(by=["a"])
            )
            self.df_eq(c, o, check_order=True, throw=True)

        def test_map_with_dict_col(self):
            e = self.engine
            dt = datetime.now()
            # test dict
            o = PandasDataFrame([[dt, dict(a=1)]], "a:datetime,b:{a:long}")
            c = e.map_engine.map_dataframe(
                o, select_top, o.schema, PartitionSpec(by=["a"])
            )
            self.df_eq(c, o, no_pandas=True, check_order=True, throw=True)

            # input has dict, output doesn't
            def mp2(cursor, data):
                return data[["a"]]

            c = e.map_engine.map_dataframe(
                o, mp2, "a:datetime", PartitionSpec(by=["a"])
            )
            self.df_eq(
                c,
                PandasDataFrame([[dt]], "a:datetime"),
                no_pandas=True,
                check_order=True,
                throw=True,
            )

            # input doesn't have dict, output has
            def mp3(cursor, data):
                return PandasDataFrame([[dt, dict(a=1)]], "a:datetime,b:{a:long}")

            c = e.map_engine.map_dataframe(
                c, mp3, "a:datetime,b:{a:long}", PartitionSpec(by=["a"])
            )
            self.df_eq(c, o, no_pandas=True, check_order=True, throw=True)

        def test_map_with_binary(self):
            e = self.engine
            o = ArrayDataFrame(
                [[pickle.dumps(BinaryObject("a"))], [pickle.dumps(BinaryObject("b"))]],
                "a:bytes",
            )
            c = e.map_engine.map_dataframe(o, binary_map, o.schema, PartitionSpec())
            expected = ArrayDataFrame(
                [
                    [pickle.dumps(BinaryObject("ax"))],
                    [pickle.dumps(BinaryObject("bx"))],
                ],
                "a:bytes",
            )
            self.df_eq(expected, c, no_pandas=True, check_order=False, throw=True)

        def test_join_multiple(self):
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[1, 2], [3, 4]], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [[1, 20], [3, 40]], "a:int,c:int")
            c = fa.as_fugue_engine_df(e, [[1, 200], [3, 400]], "a:int,d:int")
            d = fa.inner_join(a, b, c)
            self.df_eq(
                d,
                [[1, 2, 20, 200], [3, 4, 40, 400]],
                "a:int,b:int,c:int,d:int",
                throw=True,
            )

        def test__join_cross(self):
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[1, 2], [3, 4]], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [[6], [7]], "c:int")
            c = fa.join(a, b, how="Cross")
            self.df_eq(
                c,
                [[1, 2, 6], [1, 2, 7], [3, 4, 6], [3, 4, 7]],
                "a:int,b:int,c:int",
                throw=True,
            )

            b = fa.as_fugue_engine_df(e, [], "c:int")
            c = fa.cross_join(a, b)
            self.df_eq(c, [], "a:int,b:int,c:int", throw=True)

            a = fa.as_fugue_engine_df(e, [], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [], "c:int")
            c = fa.join(a, b, how="Cross")
            self.df_eq(c, [], "a:int,b:int,c:int", throw=True)

        def test__join_inner(self):
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[1, 2], [3, 4]], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [[6, 1], [2, 7]], "c:int,a:int")
            c = fa.join(a, b, how="INNER", on=["a"])
            self.df_eq(c, [[1, 2, 6]], "a:int,b:int,c:int", throw=True)
            c = fa.inner_join(b, a)
            self.df_eq(c, [[6, 1, 2]], "c:int,a:int,b:int", throw=True)

            a = fa.as_fugue_engine_df(e, [], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [], "c:int,a:int")
            c = fa.join(a, b, how="INNER", on=["a"])
            self.df_eq(c, [], "a:int,b:int,c:int", throw=True)

        def test__join_outer(self):
            e = self.engine

            a = fa.as_fugue_engine_df(e, [], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [], "c:str,a:int")
            c = fa.left_outer_join(a, b)
            self.df_eq(c, [], "a:int,b:int,c:str", throw=True)

            a = fa.as_fugue_engine_df(e, [], "a:int,b:str")
            b = fa.as_fugue_engine_df(e, [], "c:int,a:int")
            c = fa.right_outer_join(a, b)
            self.df_eq(c, [], "a:int,b:str,c:int", throw=True)

            a = fa.as_fugue_engine_df(e, [], "a:int,b:str")
            b = fa.as_fugue_engine_df(e, [], "c:str,a:int")
            c = fa.full_outer_join(a, b)
            self.df_eq(c, [], "a:int,b:str,c:str", throw=True)

            a = fa.as_fugue_engine_df(e, [[1, "2"], [3, "4"]], "a:int,b:str")
            b = fa.as_fugue_engine_df(e, [["6", 1], ["2", 7]], "c:str,a:int")
            c = fa.join(a, b, how="left_OUTER", on=["a"])
            self.df_eq(
                c, [[1, "2", "6"], [3, "4", None]], "a:int,b:str,c:str", throw=True
            )
            c = fa.join(b, a, how="left_outer", on=["a"])
            self.df_eq(
                c, [["6", 1, "2"], ["2", 7, None]], "c:str,a:int,b:str", throw=True
            )

            a = fa.as_fugue_engine_df(e, [[1, "2"], [3, "4"]], "a:int,b:str")
            b = fa.as_fugue_engine_df(e, [[6, 1], [2, 7]], "c:double,a:int")
            c = fa.join(a, b, how="left_OUTER", on=["a"])
            self.df_eq(
                c, [[1, "2", 6.0], [3, "4", None]], "a:int,b:str,c:double", throw=True
            )
            c = fa.join(b, a, how="left_outer", on=["a"])
            # assert c.as_pandas().values.tolist()[1][2] is None
            self.df_eq(
                c, [[6.0, 1, "2"], [2.0, 7, None]], "c:double,a:int,b:str", throw=True
            )

            a = fa.as_fugue_engine_df(e, [[1, "2"], [3, "4"]], "a:int,b:str")
            b = fa.as_fugue_engine_df(e, [["6", 1], ["2", 7]], "c:str,a:int")
            c = fa.join(a, b, how="right_outer", on=["a"])
            # assert c.as_pandas().values.tolist()[1][1] is None
            self.df_eq(
                c, [[1, "2", "6"], [7, None, "2"]], "a:int,b:str,c:str", throw=True
            )

            c = fa.join(a, b, how="full_outer", on=["a"])
            self.df_eq(
                c,
                [[1, "2", "6"], [3, "4", None], [7, None, "2"]],
                "a:int,b:str,c:str",
                throw=True,
            )

        def test__join_outer_pandas_incompatible(self):
            e = self.engine

            a = fa.as_fugue_engine_df(e, [[1, "2"], [3, "4"]], "a:int,b:str")
            b = fa.as_fugue_engine_df(e, [[6, 1], [2, 7]], "c:int,a:int")
            c = fa.join(a, b, how="left_OUTER", on=["a"])
            self.df_eq(
                c,
                [[1, "2", 6], [3, "4", None]],
                "a:int,b:str,c:int",
                throw=True,
            )
            c = fa.join(b, a, how="left_outer", on=["a"])
            self.df_eq(c, [[6, 1, "2"], [2, 7, None]], "c:int,a:int,b:str", throw=True)

            a = fa.as_fugue_engine_df(e, [[1, "2"], [3, "4"]], "a:int,b:str")
            b = fa.as_fugue_engine_df(e, [[True, 1], [False, 7]], "c:bool,a:int")
            c = fa.join(a, b, how="left_OUTER", on=["a"])
            self.df_eq(
                c, [[1, "2", True], [3, "4", None]], "a:int,b:str,c:bool", throw=True
            )
            c = fa.join(b, a, how="left_outer", on=["a"])
            self.df_eq(
                c, [[True, 1, "2"], [False, 7, None]], "c:bool,a:int,b:str", throw=True
            )

        def test__join_semi(self):
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[1, 2], [3, 4]], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [[6, 1], [2, 7]], "c:int,a:int")
            c = fa.join(a, b, how="semi", on=["a"])
            self.df_eq(c, [[1, 2]], "a:int,b:int", throw=True)
            c = fa.semi_join(b, a)
            self.df_eq(c, [[6, 1]], "c:int,a:int", throw=True)

            b = fa.as_fugue_engine_df(e, [], "c:int,a:int")
            c = fa.join(a, b, how="semi", on=["a"])
            self.df_eq(c, [], "a:int,b:int", throw=True)

            a = fa.as_fugue_engine_df(e, [], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [], "c:int,a:int")
            c = fa.join(a, b, how="semi", on=["a"])
            self.df_eq(c, [], "a:int,b:int", throw=True)

        def test__join_anti(self):
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[1, 2], [3, 4]], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [[6, 1], [2, 7]], "c:int,a:int")
            c = fa.join(a, b, how="anti", on=["a"])
            self.df_eq(c, [[3, 4]], "a:int,b:int", throw=True)
            c = fa.anti_join(b, a)
            self.df_eq(c, [[2, 7]], "c:int,a:int", throw=True)

            b = fa.as_fugue_engine_df(e, [], "c:int,a:int")
            c = fa.join(a, b, how="anti", on=["a"])
            self.df_eq(c, [[1, 2], [3, 4]], "a:int,b:int", throw=True)

            a = fa.as_fugue_engine_df(e, [], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [], "c:int,a:int")
            c = fa.join(a, b, how="anti", on=["a"])
            self.df_eq(c, [], "a:int,b:int", throw=True)

        def test__join_with_null_keys(self):
            # SQL will not match null values
            e = self.engine
            a = fa.as_fugue_engine_df(
                e, [[1, 2, 3], [4, None, 6]], "a:double,b:double,c:int"
            )
            b = fa.as_fugue_engine_df(
                e, [[1, 2, 33], [4, None, 63]], "a:double,b:double,d:int"
            )
            c = fa.join(a, b, how="INNER")
            self.df_eq(c, [[1, 2, 3, 33]], "a:double,b:double,c:int,d:int", throw=True)

        def test_union(self):
            e = self.engine
            a = fa.as_fugue_engine_df(
                e, [[1, 2, 3], [4, None, 6]], "a:double,b:double,c:int"
            )
            b = fa.as_fugue_engine_df(
                e, [[1, 2, 33], [4, None, 6]], "a:double,b:double,c:int"
            )
            c = fa.union(a, b)
            self.df_eq(
                c,
                [[1, 2, 3], [4, None, 6], [1, 2, 33]],
                "a:double,b:double,c:int",
                throw=True,
            )
            c = fa.union(a, b, distinct=False)
            self.df_eq(
                c,
                [[1, 2, 3], [4, None, 6], [1, 2, 33], [4, None, 6]],
                "a:double,b:double,c:int",
                throw=True,
            )
            d = fa.union(a, b, c, distinct=False)
            self.df_eq(
                d,
                [
                    [1, 2, 3],
                    [4, None, 6],
                    [1, 2, 33],
                    [4, None, 6],
                    [1, 2, 3],
                    [4, None, 6],
                    [1, 2, 33],
                    [4, None, 6],
                ],
                "a:double,b:double,c:int",
                throw=True,
            )

        def test_subtract(self):
            e = self.engine
            a = fa.as_fugue_engine_df(
                e, [[1, 2, 3], [1, 2, 3], [4, None, 6]], "a:double,b:double,c:int"
            )
            b = fa.as_fugue_engine_df(
                e, [[1, 2, 33], [4, None, 6]], "a:double,b:double,c:int"
            )
            c = fa.subtract(a, b)
            self.df_eq(
                c,
                [[1, 2, 3]],
                "a:double,b:double,c:int",
                throw=True,
            )
            x = fa.as_fugue_engine_df(e, [[1, 2, 33]], "a:double,b:double,c:int")
            y = fa.as_fugue_engine_df(e, [[4, None, 6]], "a:double,b:double,c:int")
            z = fa.subtract(a, x, y)
            self.df_eq(
                z,
                [[1, 2, 3]],
                "a:double,b:double,c:int",
                throw=True,
            )
            # TODO: EXCEPT ALL is not implemented (QPD issue)
            # c = fa.subtract(a, b, distinct=False)
            # self.df_eq(
            #     c,
            #     [[1, 2, 3], [1, 2, 3]],
            #     "a:double,b:double,c:int",
            #     throw=True,
            # )

        def test_intersect(self):
            e = self.engine
            a = fa.as_fugue_engine_df(
                e, [[1, 2, 3], [4, None, 6], [4, None, 6]], "a:double,b:double,c:int"
            )
            b = fa.as_fugue_engine_df(
                e,
                [[1, 2, 33], [4, None, 6], [4, None, 6], [4, None, 6]],
                "a:double,b:double,c:int",
            )
            c = fa.intersect(a, b)
            self.df_eq(
                c,
                [[4, None, 6]],
                "a:double,b:double,c:int",
                throw=True,
            )
            x = fa.as_fugue_engine_df(
                e,
                [[1, 2, 33]],
                "a:double,b:double,c:int",
            )
            y = fa.as_fugue_engine_df(
                e,
                [[4, None, 6], [4, None, 6], [4, None, 6]],
                "a:double,b:double,c:int",
            )
            z = fa.intersect(a, x, y)
            self.df_eq(
                z,
                [],
                "a:double,b:double,c:int",
                throw=True,
            )
            # TODO: INTERSECT ALL is not implemented (QPD issue)
            # c = fa.intersect(a, b, distinct=False)
            # self.df_eq(
            #     c,
            #     [[4, None, 6], [4, None, 6]],
            #     "a:double,b:double,c:int",
            #     throw=True,
            # )

        def test_distinct(self):
            e = self.engine
            a = fa.as_fugue_engine_df(
                e, [[4, None, 6], [1, 2, 3], [4, None, 6]], "a:double,b:double,c:int"
            )
            c = fa.distinct(a)
            self.df_eq(
                c,
                [[4, None, 6], [1, 2, 3]],
                "a:double,b:double,c:int",
                throw=True,
            )

        def test_dropna(self):
            e = self.engine
            a = fa.as_fugue_engine_df(
                e,
                [[4, None, 6], [1, 2, 3], [4, None, None]],
                "a:double,b:double,c:double",
            )
            c = fa.dropna(a)  # default
            d = fa.dropna(a, how="all")
            f = fa.dropna(a, how="any", thresh=2)
            g = fa.dropna(a, how="any", subset=["a", "c"])
            h = fa.dropna(a, how="any", thresh=1, subset=["a", "c"])
            self.df_eq(
                c,
                [[1, 2, 3]],
                "a:double,b:double,c:double",
                throw=True,
            )
            self.df_eq(
                d,
                [[4, None, 6], [1, 2, 3], [4, None, None]],
                "a:double,b:double,c:double",
                throw=True,
            )
            self.df_eq(
                f, [[4, None, 6], [1, 2, 3]], "a:double,b:double,c:double", throw=True
            )
            self.df_eq(
                g, [[4, None, 6], [1, 2, 3]], "a:double,b:double,c:double", throw=True
            )
            self.df_eq(
                h,
                [[4, None, 6], [1, 2, 3], [4, None, None]],
                "a:double,b:double,c:double",
                throw=True,
            )

        def test_fillna(self):
            e = self.engine
            a = fa.as_fugue_engine_df(
                e,
                [[4, None, 6], [1, 2, 3], [4, None, None]],
                "a:double,b:double,c:double",
            )
            c = fa.fillna(a, value=1)
            d = fa.fillna(a, {"b": 99, "c": -99})
            f = fa.fillna(a, value=-99, subset=["c"])
            g = fa.fillna(a, {"b": 99, "c": -99}, subset=["c"])  # subset ignored
            self.df_eq(
                c,
                [[4, 1, 6], [1, 2, 3], [4, 1, 1]],
                "a:double,b:double,c:double",
                throw=True,
            )
            self.df_eq(
                d,
                [[4, 99, 6], [1, 2, 3], [4, 99, -99]],
                "a:double,b:double,c:double",
                throw=True,
            )
            self.df_eq(
                f,
                [[4, None, 6], [1, 2, 3], [4, None, -99]],
                "a:double,b:double,c:double",
                throw=True,
            )
            self.df_eq(g, d, throw=True)
            raises(ValueError, lambda: fa.fillna(a, {"b": None, c: "99"}))
            raises(ValueError, lambda: fa.fillna(a, None))
            # raises(ValueError, lambda: fa.fillna(a, ["b"]))

        def test_sample(self):
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[x] for x in range(100)], "a:int")

            with raises(ValueError):
                fa.sample(a)  # must set one
            with raises(ValueError):
                fa.sample(a, n=90, frac=0.9)  # can't set both

            f = fa.sample(a, frac=0.8, replace=False)
            g = fa.sample(a, frac=0.8, replace=True)
            h = fa.sample(a, frac=0.8, seed=1)
            h2 = fa.sample(a, frac=0.8, seed=1)
            i = fa.sample(a, frac=0.8, seed=2)
            assert not self.df_eq(f, g, throw=False)
            self.df_eq(h, h2, throw=True)
            assert not self.df_eq(h, i, throw=False)
            assert abs(len(i.as_array()) - 80) < 10

        def test_take(self):
            e = self.engine
            ps = dict(by=["a"], presort="b DESC,c DESC")
            ps2 = dict(by=["c"], presort="b ASC")
            a = fa.as_fugue_engine_df(
                e,
                [
                    ["a", 2, 3],
                    ["a", 3, 4],
                    ["b", 1, 2],
                    ["b", 2, 2],
                    [None, 4, 2],
                    [None, 2, 1],
                ],
                "a:str,b:int,c:long",
            )
            b = fa.take(a, n=1, presort="b desc")
            c = fa.take(a, n=2, presort="a desc", na_position="first")
            d = fa.take(a, n=1, presort="a asc, b desc", partition=ps)
            f = fa.take(a, n=1, presort=None, partition=ps2)
            g = fa.take(a, n=2, presort="a desc", na_position="last")
            h = fa.take(a, n=2, presort="a", na_position="first")
            self.df_eq(
                b,
                [[None, 4, 2]],
                "a:str,b:int,c:long",
                throw=True,
            )
            self.df_eq(
                c,
                [[None, 4, 2], [None, 2, 1]],
                "a:str,b:int,c:long",
                throw=True,
            )
            self.df_eq(
                d,
                [["a", 3, 4], ["b", 2, 2], [None, 4, 2]],
                "a:str,b:int,c:long",
                throw=True,
            )
            self.df_eq(
                f,
                [["a", 2, 3], ["a", 3, 4], ["b", 1, 2], [None, 2, 1]],
                "a:str,b:int,c:long",
                throw=True,
            )
            self.df_eq(
                g,
                [["b", 1, 2], ["b", 2, 2]],
                "a:str,b:int,c:long",
                throw=True,
            )
            self.df_eq(
                h,
                [
                    [None, 4, 2],
                    [None, 2, 1],
                ],
                "a:str,b:int,c:long",
                throw=True,
            )
            a = fa.as_fugue_engine_df(
                e,
                [
                    ["a", 2, 3],
                    [None, 4, 2],
                    [None, 2, 1],
                ],
                "a:str,b:int,c:long",
            )
            i = fa.take(a, n=1, partition="a", presort=None)
            case1 = self.df_eq(
                i,
                [
                    ["a", 2, 3],
                    [None, 4, 2],
                ],
                "a:str,b:int,c:long",
                throw=False,
            )
            case2 = self.df_eq(
                i,
                [
                    ["a", 2, 3],
                    [None, 2, 1],
                ],
                "a:str,b:int,c:long",
                throw=False,
            )
            assert case1 or case2
            j = fa.take(a, n=2, partition="a", presort=None)
            self.df_eq(
                j,
                [
                    ["a", 2, 3],
                    [None, 4, 2],
                    [None, 2, 1],
                ],
                "a:str,b:int,c:long",
                throw=True,
            )
            raises(ValueError, lambda: fa.take(a, n=0.5, presort=None))

        def test_sample_n(self):
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[x] for x in range(100)], "a:int")

            b = fa.sample(a, n=90, replace=False)
            c = fa.sample(a, n=90, replace=True)
            d = fa.sample(a, n=90, seed=1)
            d2 = fa.sample(a, n=90, seed=1)
            e = fa.sample(a, n=90, seed=2)
            assert not self.df_eq(b, c, throw=False)
            self.df_eq(d, d2, throw=True)
            assert not self.df_eq(d, e, throw=False)
            assert abs(len(e.as_array()) - 90) < 2

        def test_comap(self):
            ps = PartitionSpec(presort="b,c")
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[1, 2], [3, 4], [1, 5]], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [[6, 1], [2, 7]], "c:int,a:int")
            with raises(InvalidOperationError):  # cross can't have partition by
                e.zip(
                    DataFrames([a, b]),
                    partition_spec=PartitionSpec(by=["a"]),
                    how="cross",
                )
            with raises(NotImplementedError):
                e.zip(
                    DataFrames([a, b]),
                    partition_spec=PartitionSpec(by=["a"]),
                    how="left_anti",
                )
            z1 = fa.persist(e.zip(DataFrames([a, b])))
            z2 = fa.persist(
                e.zip(DataFrames([a, b]), partition_spec=ps, how="left_outer")
            )
            z3 = fa.persist(
                e.zip(DataFrames([b, a]), partition_spec=ps, how="right_outer")
            )
            z4 = fa.persist(e.zip(DataFrames([a, b]), partition_spec=ps, how="cross"))
            z5 = fa.persist(
                e.zip(DataFrames([a, b]), partition_spec=ps, how="full_outer")
            )

            def comap(cursor, dfs):
                assert not dfs.has_key
                v = ",".join([k + str(v.count()) for k, v in dfs.items()])
                keys = (
                    cursor.key_value_array
                    if not dfs[0].empty
                    else dfs[1][["a"]].peek_array()
                )
                if len(keys) == 0:
                    return ArrayDataFrame([[v]], "v:str")
                return ArrayDataFrame([keys + [v]], cursor.key_schema + "v:str")

            def on_init(partition_no, dfs):
                assert not dfs.has_key
                assert partition_no >= 0
                assert len(dfs) > 0

            res = e.comap(
                z1,
                comap,
                "a:int,v:str",
                PartitionSpec(),
                on_init=on_init,
            )
            self.df_eq(res, [[1, "_02,_11"]], "a:int,v:str", throw=True)

            # for outer joins, the NULL will be filled with empty dataframe
            res = e.comap(z2, comap, "a:int,v:str", PartitionSpec())
            self.df_eq(
                res,
                [[1, "_02,_11"], [3, "_01,_10"]],
                "a:int,v:str",
                throw=True,
            )

            res = e.comap(z3, comap, "a:int,v:str", PartitionSpec())
            self.df_eq(
                res,
                [[1, "_01,_12"], [3, "_00,_11"]],
                "a:int,v:str",
                throw=True,
            )

            res = e.comap(z4, comap, "v:str", PartitionSpec())
            self.df_eq(res, [["_03,_12"]], "v:str", throw=True)

            res = e.comap(z5, comap, "a:int,v:str", PartitionSpec())
            self.df_eq(
                res,
                [[1, "_02,_11"], [3, "_01,_10"], [7, "_00,_11"]],
                "a:int,v:str",
                throw=True,
            )

        def test_comap_with_key(self):
            e = self.engine
            a = fa.as_fugue_engine_df(e, [[1, 2], [3, 4], [1, 5]], "a:int,b:int")
            b = fa.as_fugue_engine_df(e, [[6, 1], [2, 7]], "c:int,a:int")
            c = fa.as_fugue_engine_df(e, [[6, 1]], "c:int,a:int")
            z1 = fa.persist(e.zip(DataFrames(x=a, y=b)))
            z2 = fa.persist(e.zip(DataFrames(x=a, y=b, z=b)))
            z3 = fa.persist(
                e.zip(DataFrames(z=c), partition_spec=PartitionSpec(by=["a"]))
            )

            def comap(cursor, dfs):
                assert dfs.has_key
                v = ",".join([k + str(v.count()) for k, v in dfs.items()])
                keys = cursor.key_value_array
                # if len(keys) == 0:
                #    return ArrayDataFrame([[v]], "v:str")
                return ArrayDataFrame([keys + [v]], cursor.key_schema + "v:str")

            def on_init(partition_no, dfs):
                assert dfs.has_key
                assert partition_no >= 0
                assert len(dfs) > 0

            res = e.comap(
                z1,
                comap,
                "a:int,v:str",
                PartitionSpec(),
                on_init=on_init,
            )
            self.df_eq(res, [[1, "x2,y1"]], "a:int,v:str", throw=True)

            res = e.comap(
                z2,
                comap,
                "a:int,v:str",
                PartitionSpec(),
                on_init=on_init,
            )
            self.df_eq(res, [[1, "x2,y1,z1"]], "a:int,v:str", throw=True)

            res = e.comap(
                z3,
                comap,
                "a:int,v:str",
                PartitionSpec(),
                on_init=on_init,
            )
            self.df_eq(res, [[1, "z1"]], "a:int,v:str", throw=True)

        @pytest.fixture(autouse=True)
        def init_tmpdir(self, tmpdir):
            self.tmpdir = tmpdir

        def test_save_single_and_load_parquet(self):
            b = ArrayDataFrame([[6, 1], [2, 7]], "c:int,a:long")
            path = os.path.join(self.tmpdir, "a", "b")
            makedirs(path, exist_ok=True)
            # over write folder with single file
            fa.save(b, path, format_hint="parquet", force_single=True)
            assert isfile(path)
            c = fa.load(path, format_hint="parquet", columns=["a", "c"], as_fugue=True)
            self.df_eq(c, [[1, 6], [7, 2]], "a:long,c:int", throw=True)

            # overwirte single with folder (if applicable)
            b = ArrayDataFrame([[60, 1], [20, 7]], "c:int,a:long")
            fa.save(b, path, format_hint="parquet", mode="overwrite")
            c = fa.load(path, format_hint="parquet", columns=["a", "c"], as_fugue=True)
            self.df_eq(c, [[1, 60], [7, 20]], "a:long,c:int", throw=True)

        def test_save_and_load_parquet(self):
            b = ArrayDataFrame([[6, 1], [2, 7]], "c:int,a:long")
            path = os.path.join(self.tmpdir, "a", "b")
            fa.save(b, path, format_hint="parquet")
            c = fa.load(path, format_hint="parquet", columns=["a", "c"], as_fugue=True)
            self.df_eq(c, [[1, 6], [7, 2]], "a:long,c:int", throw=True)

        def test_load_parquet_folder(self):
            native = NativeExecutionEngine()
            a = ArrayDataFrame([[6, 1]], "c:int,a:long")
            b = ArrayDataFrame([[2, 7], [4, 8]], "c:int,a:long")
            path = os.path.join(self.tmpdir, "a", "b")
            fa.save(a, os.path.join(path, "a.parquet"), engine=native)
            fa.save(b, os.path.join(path, "b.parquet"), engine=native)
            touch(os.path.join(path, "_SUCCESS"))
            c = fa.load(path, format_hint="parquet", columns=["a", "c"], as_fugue=True)
            self.df_eq(c, [[1, 6], [7, 2], [8, 4]], "a:long,c:int", throw=True)

        def test_load_parquet_files(self):
            native = NativeExecutionEngine()
            a = ArrayDataFrame([[6, 1]], "c:int,a:long")
            b = ArrayDataFrame([[2, 7], [4, 8]], "c:int,a:long")
            path = os.path.join(self.tmpdir, "a", "b")
            f1 = os.path.join(path, "a.parquet")
            f2 = os.path.join(path, "b.parquet")
            fa.save(a, f1, engine=native)
            fa.save(b, f2, engine=native)
            c = fa.load(
                [f1, f2], format_hint="parquet", columns=["a", "c"], as_fugue=True
            )
            self.df_eq(c, [[1, 6], [7, 2], [8, 4]], "a:long,c:int", throw=True)

        def test_save_single_and_load_csv(self):
            b = ArrayDataFrame([[6.1, 1.1], [2.1, 7.1]], "c:double,a:double")
            path = os.path.join(self.tmpdir, "a", "b")
            makedirs(path, exist_ok=True)
            # over write folder with single file
            fa.save(b, path, format_hint="csv", header=True, force_single=True)
            assert isfile(path)
            c = fa.load(
                path, format_hint="csv", header=True, infer_schema=False, as_fugue=True
            )
            self.df_eq(c, [["6.1", "1.1"], ["2.1", "7.1"]], "c:str,a:str", throw=True)

            c = fa.load(
                path, format_hint="csv", header=True, infer_schema=True, as_fugue=True
            )
            self.df_eq(c, [[6.1, 1.1], [2.1, 7.1]], "c:double,a:double", throw=True)

            with raises(ValueError):
                c = fa.load(
                    path,
                    format_hint="csv",
                    header=True,
                    infer_schema=True,
                    columns="c:str,a:str",  # invalid to set schema when infer schema
                    as_fugue=True,
                )

            c = fa.load(
                path,
                format_hint="csv",
                header=True,
                infer_schema=False,
                columns=["a", "c"],
                as_fugue=True,
            )
            self.df_eq(c, [["1.1", "6.1"], ["7.1", "2.1"]], "a:str,c:str", throw=True)

            c = fa.load(
                path,
                format_hint="csv",
                header=True,
                infer_schema=False,
                columns="a:double,c:double",
                as_fugue=True,
            )
            self.df_eq(c, [[1.1, 6.1], [7.1, 2.1]], "a:double,c:double", throw=True)

            # overwirte single with folder (if applicable)
            b = ArrayDataFrame([[60.1, 1.1], [20.1, 7.1]], "c:double,a:double")
            fa.save(b, path, format_hint="csv", header=True, mode="overwrite")
            c = fa.load(
                path,
                format_hint="csv",
                header=True,
                infer_schema=False,
                columns=["a", "c"],
                as_fugue=True,
            )
            self.df_eq(c, [["1.1", "60.1"], ["7.1", "20.1"]], "a:str,c:str", throw=True)

        def test_save_single_and_load_csv_no_header(self):
            b = ArrayDataFrame([[6.1, 1.1], [2.1, 7.1]], "c:double,a:double")
            path = os.path.join(self.tmpdir, "a", "b")
            makedirs(path, exist_ok=True)
            # over write folder with single file
            fa.save(b, path, format_hint="csv", header=False, force_single=True)
            assert isfile(path)

            with raises(ValueError):
                c = fa.load(
                    path,
                    format_hint="csv",
                    header=False,
                    infer_schema=False,
                    as_fugue=True,
                    # when header is False, must set columns
                )

            c = fa.load(
                path,
                format_hint="csv",
                header=False,
                infer_schema=False,
                columns=["c", "a"],
                as_fugue=True,
            )
            self.df_eq(c, [["6.1", "1.1"], ["2.1", "7.1"]], "c:str,a:str", throw=True)

            c = fa.load(
                path,
                format_hint="csv",
                header=False,
                infer_schema=True,
                columns=["c", "a"],
                as_fugue=True,
            )
            self.df_eq(c, [[6.1, 1.1], [2.1, 7.1]], "c:double,a:double", throw=True)

            with raises(ValueError):
                c = fa.load(
                    path,
                    format_hint="csv",
                    header=False,
                    infer_schema=True,
                    columns="c:double,a:double",
                    as_fugue=True,
                )

            c = fa.load(
                path,
                format_hint="csv",
                header=False,
                infer_schema=False,
                columns="c:double,a:str",
                as_fugue=True,
            )
            self.df_eq(c, [[6.1, "1.1"], [2.1, "7.1"]], "c:double,a:str", throw=True)

        def test_save_and_load_csv(self):
            b = ArrayDataFrame([[6.1, 1.1], [2.1, 7.1]], "c:double,a:double")
            path = os.path.join(self.tmpdir, "a", "b")
            fa.save(b, path, format_hint="csv", header=True)
            c = fa.load(
                path,
                format_hint="csv",
                header=True,
                infer_schema=True,
                columns=["a", "c"],
                as_fugue=True,
            )
            self.df_eq(c, [[1.1, 6.1], [7.1, 2.1]], "a:double,c:double", throw=True)

        @pytest.mark.skipif(sys.platform == "win32", reason="skip on Windows")
        def test_load_csv_folder(self):
            native = NativeExecutionEngine()
            a = ArrayDataFrame([[6.1, 1.1]], "c:double,a:double")
            b = ArrayDataFrame([[2.1, 7.1], [4.1, 8.1]], "c:double,a:double")
            path = os.path.join(self.tmpdir, "a", "b")
            fa.save(
                a,
                os.path.join(path, "a.csv"),
                format_hint="csv",
                header=True,
                engine=native,
            )
            fa.save(
                b,
                os.path.join(path, "b.csv"),
                format_hint="csv",
                header=True,
                engine=native,
            )
            touch(os.path.join(path, "_SUCCESS"))
            c = fa.load(
                path,
                format_hint="csv",
                header=True,
                infer_schema=True,
                columns=["a", "c"],
                as_fugue=True,
            )
            self.df_eq(
                c, [[1.1, 6.1], [7.1, 2.1], [8.1, 4.1]], "a:double,c:double", throw=True
            )

        def test_save_single_and_load_json(self):
            b = ArrayDataFrame([[6, 1], [2, 7]], "c:int,a:long")
            path = os.path.join(self.tmpdir, "a", "b")
            makedirs(path, exist_ok=True)
            # over write folder with single file
            fa.save(b, path, format_hint="json", force_single=True)
            assert isfile(path)
            c = fa.load(path, format_hint="json", columns=["a", "c"], as_fugue=True)
            self.df_eq(c, [[1, 6], [7, 2]], "a:long,c:long", throw=True)

            # overwirte single with folder (if applicable)
            b = ArrayDataFrame([[60, 1], [20, 7]], "c:long,a:long")
            fa.save(b, path, format_hint="json", mode="overwrite")
            c = fa.load(path, format_hint="json", columns=["a", "c"], as_fugue=True)
            self.df_eq(c, [[1, 60], [7, 20]], "a:long,c:long", throw=True)

        def test_save_and_load_json(self):
            e = self.engine
            b = ArrayDataFrame([[6, 1], [3, 4], [2, 7], [4, 8], [6, 7]], "c:int,a:long")
            path = os.path.join(self.tmpdir, "a", "b")
            fa.save(
                e.repartition(fa.as_fugue_engine_df(e, b), PartitionSpec(num=2)),
                path,
                format_hint="json",
            )
            c = fa.load(path, format_hint="json", columns=["a", "c"], as_fugue=True)
            self.df_eq(
                c, [[1, 6], [7, 2], [4, 3], [8, 4], [7, 6]], "a:long,c:long", throw=True
            )

        def test_load_json_folder(self):
            native = NativeExecutionEngine()
            a = ArrayDataFrame([[6, 1], [3, 4]], "c:int,a:long")
            b = ArrayDataFrame([[2, 7], [4, 8]], "c:int,a:long")
            path = os.path.join(self.tmpdir, "a", "b")
            fa.save(a, os.path.join(path, "a.json"), format_hint="json", engine=native)
            fa.save(b, os.path.join(path, "b.json"), format_hint="json", engine=native)
            touch(os.path.join(path, "_SUCCESS"))
            c = fa.load(path, format_hint="json", columns=["a", "c"], as_fugue=True)
            self.df_eq(c, [[1, 6], [7, 2], [8, 4], [4, 3]], "a:long,c:long", throw=True)

        def test_engine_api(self):
            # complimentary tests not covered by the other tests
            with fa.engine_context(self.engine):
                df1 = fa.as_fugue_df([[0, 1], [2, 3]], schema="a:long,b:long")
                df1 = fa.repartition(df1, {"num": 2})
                df1 = fa.get_native_as_df(fa.broadcast(df1))
                df2 = pd.DataFrame([[0, 1], [2, 3]], columns=["a", "b"])
                df3 = fa.union(df1, df2, as_fugue=False)
                assert fa.is_df(df3) and not isinstance(df3, DataFrame)
                df4 = fa.union(df1, df2, as_fugue=True)
                assert isinstance(df4, DataFrame)
                self.df_eq(df4, fa.as_pandas(df3), throw=True)


def select_top(cursor, data):
    return ArrayDataFrame([cursor.row], cursor.row_schema)


class BinaryObject(object):
    def __init__(self, data=None):
        self.data = data


def binary_map(cursor, df):
    arr = df.as_array(type_safe=True)
    for i in range(len(arr)):
        obj = pickle.loads(arr[i][0])
        obj.data += "x"
        arr[i][0] = pickle.dumps(obj)
    return ArrayDataFrame(arr, df.schema)
