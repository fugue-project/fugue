# pylint: disable-all
# flake8: noqa

from datetime import date, datetime
from typing import Any
from unittest import TestCase

import numpy as np
import pandas as pd
from fugue.bag import Bag, LocalBag
from fugue.exceptions import FugueDataFrameOperationError, FugueDatasetEmptyError
from pytest import raises
from triad.collections.schema import Schema


class BagTests(object):
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

        def bg(self, data: Any = None) -> Bag:  # pragma: no cover
            raise NotImplementedError

        def test_init_basic(self):
            raises(Exception, lambda: self.bg())
            bg = self.bg([])
            assert df.empty

        def test_peek(self):
            bg = self.bg([])
            raises(FugueDatasetEmptyError, lambda: bg.peek())

            bg = self.bg(["x"])
            assert not bg.is_bounded or 1 == bg.count()
            assert not bg.empty
            assert "x" == bg.peek()

        def test_as_array(self):
            bg = self.bg([2, 1, "a"])
            assert [1, 2, "a"] == sorted(df.as_array())

        def test_as_array_special_values(self):
            bg = self.bg([2, None, "a"])
            assert [None, 2, "a"] == sorted(df.as_array())

            bg = self.bg([np.float16(0.1)])
            assert [np.float16(0.1)] == sorted(df.as_array())

        def test_show(self):
            df = self.df([["a", 1]], "a:str,b:int")
            df.show()

        def test_get_altered_schema(self):
            df = self.df([["a", 1]], "a:str,b:int")
            assert df._get_altered_schema("") == "a:str,b:int"
            assert df._get_altered_schema(None) == "a:str,b:int"
            assert df._get_altered_schema("b:str,a:str") == "a:str,b:str"
            with raises(FugueDataFrameOperationError):
                df._get_altered_schema("bb:str,a:str")
            with raises(NotImplementedError):
                df._get_altered_schema("b:binary")
            with raises(NotImplementedError):
                df._get_altered_schema("b:[str]")
            with raises(NotImplementedError):
                df._get_altered_schema("b:{x:str}")

        def test_alter_columns(self):
            # empty
            df = self.df([], "a:str,b:int")
            ndf = df.alter_columns("a:str,b:str")
            assert [] == ndf.as_array(type_safe=True)
            assert ndf.schema == "a:str,b:str"

            # no change
            df = self.df([["a", 1], ["c", None]], "a:str,b:int")
            ndf = df.alter_columns("b:int,a:str")
            assert [["a", 1], ["c", None]] == ndf.as_array(type_safe=True)
            assert ndf.schema == df.schema

            # bool -> str
            df = self.df([["a", True], ["b", False], ["c", None]], "a:str,b:bool")
            ndf = df.alter_columns("b:str")
            actual = ndf.as_array(type_safe=True)
            # Capitalization doesn't matter
            # and dataframes don't need to be consistent on capitalization
            expected1 = [["a", "True"], ["b", "False"], ["c", None]]
            expected2 = [["a", "true"], ["b", "false"], ["c", None]]
            assert expected1 == actual or expected2 == actual
            assert ndf.schema == "a:str,b:str"

            # int -> str
            df = self.df([["a", 1], ["c", None]], "a:str,b:int")
            ndf = df.alter_columns("b:str")
            assert [["a", "1"], ["c", None]] == ndf.as_array(type_safe=True)
            assert ndf.schema == "a:str,b:str"

            # int -> double
            df = self.df([["a", 1], ["c", None]], "a:str,b:int")
            ndf = df.alter_columns("b:double")
            assert [["a", 1], ["c", None]] == ndf.as_array(type_safe=True)
            assert ndf.schema == "a:str,b:double"

            # double -> str
            df = self.df([["a", 1.1], ["b", None]], "a:str,b:double")
            data = df.alter_columns("b:str").as_array(type_safe=True)
            assert [["a", "1.1"], ["b", None]] == data

            # double -> int
            df = self.df([["a", 1.0], ["b", None]], "a:str,b:double")
            data = df.alter_columns("b:int").as_array(type_safe=True)
            assert [["a", 1], ["b", None]] == data

            # date -> str
            df = self.df(
                [["a", date(2020, 1, 1)], ["b", date(2020, 1, 2)], ["c", None]],
                "a:str,b:date",
            )
            data = df.alter_columns("b:str").as_array(type_safe=True)
            assert [["a", "2020-01-01"], ["b", "2020-01-02"], ["c", None]] == data

            # datetime -> str
            df = self.df(
                [
                    ["a", datetime(2020, 1, 1, 3, 4, 5)],
                    ["b", datetime(2020, 1, 2, 16, 7, 8)],
                    ["c", None],
                ],
                "a:str,b:datetime",
            )
            data = df.alter_columns("b:str").as_array(type_safe=True)
            assert [
                ["a", "2020-01-01 03:04:05"],
                ["b", "2020-01-02 16:07:08"],
                ["c", None],
            ] == data

            # str -> bool
            df = self.df([["a", "trUe"], ["b", "False"], ["c", None]], "a:str,b:str")
            ndf = df.alter_columns("b:bool,a:str")
            assert [["a", True], ["b", False], ["c", None]] == ndf.as_array(
                type_safe=True
            )
            assert ndf.schema == "a:str,b:bool"

            # str -> int
            df = self.df([["a", "1"]], "a:str,b:str")
            ndf = df.alter_columns("b:int,a:str")
            assert [["a", 1]] == ndf.as_array(type_safe=True)
            assert ndf.schema == "a:str,b:int"

            # str -> double
            df = self.df([["a", "1.1"], ["b", "2"], ["c", None]], "a:str,b:str")
            ndf = df.alter_columns("b:double")
            assert [["a", 1.1], ["b", 2.0], ["c", None]] == ndf.as_array(type_safe=True)
            assert ndf.schema == "a:str,b:double"

            # str -> date
            df = self.df(
                [["1", "2020-01-01"], ["2", "2020-01-02 01:02:03"], ["3", None]],
                "a:str,b:str",
            )
            ndf = df.alter_columns("b:date,a:int")
            assert [
                [1, date(2020, 1, 1)],
                [2, date(2020, 1, 2)],
                [3, None],
            ] == ndf.as_array(type_safe=True)
            assert ndf.schema == "a:int,b:date"

            # str -> datetime
            df = self.df(
                [["1", "2020-01-01"], ["2", "2020-01-02 01:02:03"], ["3", None]],
                "a:str,b:str",
            )
            ndf = df.alter_columns("b:datetime,a:int")
            assert [
                [1, datetime(2020, 1, 1)],
                [2, datetime(2020, 1, 2, 1, 2, 3)],
                [3, None],
            ] == ndf.as_array(type_safe=True)
            assert ndf.schema == "a:int,b:datetime"

        def test_alter_columns_invalid(self):
            # invalid conversion
            with raises(Exception):
                df = self.df(
                    [["1", "x"], ["2", "y"], ["3", None]],
                    "a:str,b:str",
                )
                ndf = df.alter_columns("b:int")
                ndf.show()  # lazy dataframes will force to materialize
