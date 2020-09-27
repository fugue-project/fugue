from datetime import datetime
from typing import Any
from unittest import TestCase

import numpy as np
import pandas as pd
from fugue.dataframe import ArrowDataFrame, DataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import (
    FugueDataFrameEmptyError,
    FugueDataFrameInitError,
    FugueDataFrameOperationError,
)
from pytest import raises
from triad.collections.schema import Schema


class DataFrameTests(object):
    """DataFrame level general test suite.
    All new DataFrame types should pass this test suite.
    """

    class Tests(TestCase):
        @classmethod
        def setUpClass(cls):
            pass

        @classmethod
        def tearDownClass(cls):
            pass

        def df(
            self, data: Any = None, schema: Any = None, metadata: Any = None
        ) -> DataFrame:  # pragma: no cover
            raise NotImplementedError

        def test_init_basic(self):
            raises(FugueDataFrameInitError, lambda: self.df())
            raises(FugueDataFrameInitError, lambda: self.df([]))
            raises(FugueDataFrameInitError, lambda: self.df([[]], Schema()))
            raises(FugueDataFrameInitError, lambda: self.df([[1]], Schema()))
            # raises(SchemaError, lambda: self.df([[1]]))  # schema can be inferred

            df = self.df([], "a:str,b:int")
            assert df.empty

        def test_datetime(self):
            df = self.df([["2020-01-01"], [None]], "a:datetime")
            assert [[datetime(2020, 1, 1)], [None]] == df.as_array(type_safe=True)

        def test_peek(self):
            df = self.df([], "x:str,y:double")
            raises(FugueDataFrameEmptyError, lambda: df.peek_array())
            raises(FugueDataFrameEmptyError, lambda: df.peek_dict())

            df = self.df([["a", 1.0], ["b", 2.0]], "x:str,y:double")
            assert not df.is_bounded or 2 == df.count()
            assert not df.empty
            assert ["a", 1.0] == df.peek_array()
            assert dict(x="a", y=1.0) == df.peek_dict()

        def test_as_pandas(self):
            df = self.df([["a", 1.0], ["b", 2.0]], "x:str,y:double")
            pdf = df.as_pandas()
            assert [["a", 1.0], ["b", 2.0]] == pdf.values.tolist()

            df = self.df([], "x:str,y:double")
            pdf = df.as_pandas()
            assert [] == pdf.values.tolist()

        def test_drop(self):
            df = self.df([], "a:str,b:int").drop(["a"])
            assert df.schema == "b:int"
            raises(
                FugueDataFrameOperationError, lambda: df.drop(["b"])
            )  # can't be empty
            raises(
                FugueDataFrameOperationError, lambda: df.drop(["x"])
            )  # cols must exist

            df = self.df([["a", 1]], "a:str,b:int").drop(["a"])
            assert df.schema == "b:int"
            raises(
                FugueDataFrameOperationError, lambda: df.drop(["b"])
            )  # can't be empty
            raises(
                FugueDataFrameOperationError, lambda: df.drop(["x"])
            )  # cols must exist
            assert [[1]] == df.as_array(type_safe=True)

        def test_select(self):
            df = self.df([], "a:str,b:int")[["b"]]
            assert df.schema == "b:int"
            raises(FugueDataFrameOperationError, lambda: df[["a"]])  # not existed
            raises(FugueDataFrameOperationError, lambda: df[[]])  # empty

            df = self.df([["a", 1]], "a:str,b:int")[["b"]]
            assert df.schema == "b:int"
            raises(FugueDataFrameOperationError, lambda: df[["a"]])  # not existed
            raises(FugueDataFrameOperationError, lambda: df[[]])  # empty
            assert [[1]] == df.as_array(type_safe=True)

            df = self.df([["a", 1, 2]], "a:str,b:int,c:int")
            df_eq(df[["c", "a"]], [[2, "a"]], "a:str,c:int")

        def test_rename(self):
            df = self.df([["a", 1]], "a:str,b:int")
            df2 = df.rename(columns=dict(a="aa"))
            assert df.schema == "a:str,b:int"
            df_eq(df2, [["a", 1]], "aa:str,b:int", throw=True)

        def test_rename_invalid(self):
            df = self.df([["a", 1]], "a:str,b:int")
            raises(
                FugueDataFrameOperationError, lambda: df.rename(columns=dict(aa="ab"))
            )

        def test_as_array(self):
            for func in [
                lambda df, *args, **kwargs: df.as_array(
                    *args, **kwargs, type_safe=True
                ),
                lambda df, *args, **kwargs: list(
                    df.as_array_iterable(*args, **kwargs, type_safe=True)
                ),
            ]:
                df = self.df([], "a:str,b:int")
                assert [] == func(df)

                df = self.df([["a", 1]], "a:str,b:int")
                assert [["a", 1]] == func(df)
                df = self.df([["a", 1]], "a:str,b:int")
                assert [["a", 1]] == func(df, ["a", "b"])
                df = self.df([["a", 1]], "a:str,b:int")
                assert [[1, "a"]] == func(df, ["b", "a"])

                for v in [1.0, np.float64(1.0)]:
                    df = self.df([[v, 1]], "a:double,b:int")
                    d = func(df)
                    assert [[1.0, 1]] == d
                    assert isinstance(d[0][0], float)
                    assert isinstance(d[0][1], int)

                # special values
                df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
                data = func(df)
                assert [[datetime(2020, 1, 1), 1]] == data
                assert isinstance(data[0][0], datetime)
                assert isinstance(data[0][1], int)

                df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
                assert [[None, 1]] == func(df)

                df = self.df([[float("nan"), 1]], "a:double,b:int")
                assert [[None, 1]] == func(df)

                df = self.df([[float("inf"), 1]], "a:double,b:int")
                assert [[float("inf"), 1]] == func(df)

        def test_as_dict_iterable(self):
            df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
            assert [dict(a=None, b=1)] == list(df.as_dict_iterable())
            df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
            assert [dict(a=datetime(2020, 1, 1), b=1)] == list(df.as_dict_iterable())

        def test_nested(self):
            data = [[[30, 40]]]
            df = self.df(data, "a:[int]")
            a = df.as_array(type_safe=True)
            assert data == a

            data = [[dict(a="1", b=[3, 4], d=1.0)], [dict(b=[30, 40])]]
            df = self.df(data, "a:{a:str,b:[int]}")
            a = df.as_array(type_safe=True)
            assert [[dict(a="1", b=[3, 4])], [dict(a=None, b=[30, 40])]] == a

            data = [[[dict(b=[30, 40])]]]
            df = self.df(data, "a:[{a:str,b:[int]}]")
            a = df.as_array(type_safe=True)
            assert [[[dict(a=None, b=[30, 40])]]] == a

        def test_binary(self):
            data = [[b"\x01\x05"]]
            df = self.df(data, "a:bytes")
            a = df.as_array(type_safe=True)
            assert data == a

        def test_as_arrow(self):
            # pd.Nat
            df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
            assert [dict(a=None, b=1)] == list(
                ArrowDataFrame(df.as_arrow()).as_dict_iterable()
            )
            # pandas timestamps
            df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
            assert [dict(a=datetime(2020, 1, 1), b=1)] == list(
                ArrowDataFrame(df.as_arrow()).as_dict_iterable()
            )
            # float nan, list
            data = [[[float("nan"), 2.0]]]
            df = self.df(data, "a:[float]")
            assert [[[None, 2.0]]] == ArrowDataFrame(df.as_arrow()).as_array()
            # dict
            data = [[dict(b="x")]]
            df = self.df(data, "a:{b:str}")
            assert data == ArrowDataFrame(df.as_arrow()).as_array()
            # list[dict]
            data = [[[dict(b=[30, 40])]]]
            df = self.df(data, "a:[{b:[int]}]")
            assert data == ArrowDataFrame(df.as_arrow()).as_array()

        def test_head(self):
            df = self.df([], "a:str,b:int")
            assert [] == df.head(1)
            df = self.df([["a", 1]], "a:str,b:int")
            if df.is_bounded:
                assert [["a", 1]] == df.head(1)
            assert [[1, "a"]] == df.head(1, ["b", "a"])

        def test_show(self):
            df = self.df([["a", 1]], "a:str,b:int")
            df.show()
