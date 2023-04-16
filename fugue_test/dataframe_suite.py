# pylint: disable-all

from datetime import date, datetime
from typing import Any
from unittest import TestCase

import numpy as np
import pandas as pd
from pytest import raises

import fugue.api as fi
from fugue.dataframe import ArrowDataFrame, DataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueDataFrameOperationError, FugueDatasetEmptyError


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

        def df(self, data: Any = None, schema: Any = None) -> Any:  # pragma: no cover
            raise NotImplementedError

        def test_native(self):
            df = self.df([1], "a:int")
            assert fi.is_df(df)
            fdf = fi.as_fugue_df(df)
            assert isinstance(fdf, DataFrame)
            assert fi.is_df(fdf)
            ndf = fi.get_native_as_df(fdf)
            assert fi.is_df(ndf)
            assert not isinstance(ndf, DataFrame)
            ndf2 = fi.get_native_as_df(ndf)
            assert ndf2 is ndf

        def test_peek(self):
            df = self.df([], "x:str,y:double")
            raises(FugueDatasetEmptyError, lambda: fi.peek_array(df))
            raises(FugueDatasetEmptyError, lambda: fi.peek_dict(df))

            df = self.df([["a", 1.0], ["b", 2.0]], "x:str,y:double")
            assert not fi.is_bounded(df) or 2 == fi.count(df)
            assert not fi.is_empty(df)
            assert ["a", 1.0] == fi.peek_array(df)
            assert dict(x="a", y=1.0) == fi.peek_dict(df)

        def test_as_pandas(self):
            df = self.df([["a", 1.0], ["b", 2.0]], "x:str,y:double")
            pdf = fi.as_pandas(df)
            assert [["a", 1.0], ["b", 2.0]] == pdf.values.tolist()

            df = self.df([], "x:str,y:double")
            pdf = fi.as_pandas(df)
            assert [] == pdf.values.tolist()
            assert fi.is_local(pdf)

        def test_as_local(self):
            with raises(NotImplementedError):
                fi.as_local(10)
            with raises(NotImplementedError):
                fi.as_local_bounded(10)

            df = self.df([["a", 1.0], ["b", 2.0]], "x:str,y:double")
            ldf = fi.as_local(df)
            assert fi.is_local(ldf)
            lbdf = fi.as_local_bounded(df)
            assert fi.is_local(lbdf) and fi.is_bounded(lbdf)

            fdf = fi.as_fugue_df(df)
            fdf.reset_metadata({"a": 1})
            ldf = fi.as_local(fdf)
            assert ldf.metadata == {"a": 1}
            lbdf = fi.as_local_bounded(fdf)
            assert fi.is_local(lbdf) and fi.is_bounded(lbdf)
            assert ldf.metadata == {"a": 1}

        def test_drop_columns(self):
            df = fi.drop_columns(self.df([], "a:str,b:int"), ["a"])
            assert fi.get_schema(df) == "b:int"
            raises(
                FugueDataFrameOperationError, lambda: fi.drop_columns(df, ["b"])
            )  # can't be empty
            raises(
                FugueDataFrameOperationError, lambda: fi.drop_columns(df, ["x"])
            )  # cols must exist

            df = fi.drop_columns(self.df([["a", 1]], "a:str,b:int"), ["a"])
            assert fi.get_schema(df) == "b:int"
            raises(
                FugueDataFrameOperationError, lambda: fi.drop_columns(df, ["b"])
            )  # can't be empty
            raises(
                FugueDataFrameOperationError, lambda: fi.drop_columns(df, ["x"])
            )  # cols must exist
            assert [[1]] == fi.as_array(df, type_safe=True)

        def test_select(self):
            df = fi.select_columns(self.df([], "a:str,b:int"), ["b"])
            assert fi.get_schema(df) == "b:int"
            assert fi.get_column_names(df) == ["b"]
            raises(
                FugueDataFrameOperationError, lambda: fi.select_columns(df, [])
            )  # select empty
            raises(
                FugueDataFrameOperationError, lambda: fi.select_columns(df, ["a"])
            )  # not existed
            raises(
                FugueDataFrameOperationError, lambda: fi.select_columns(df, ["a"])
            )  # empty

            df = fi.select_columns(self.df([["a", 1]], "a:str,b:int"), ["b"])
            assert fi.get_schema(df) == "b:int"
            raises(
                FugueDataFrameOperationError, lambda: fi.select_columns(df, ["a"])
            )  # not existed
            raises(
                FugueDataFrameOperationError, lambda: fi.select_columns(df, ["a"])
            )  # empty
            assert [[1]] == fi.as_array(df, type_safe=True)

            df = self.df([["a", 1, 2]], "a:str,b:int,c:int")
            df_eq(
                fi.as_fugue_df(fi.select_columns(df, ["c", "a"])),
                [[2, "a"]],
                "a:str,c:int",
            )

        def test_rename(self):
            for data in [[["a", 1]], []]:
                df = self.df(data, "a:str,b:int")
                df2 = fi.rename(df, columns=dict(a="aa"))
                assert fi.get_schema(df) == "a:str,b:int"
                df_eq(fi.as_fugue_df(df2), data, "aa:str,b:int", throw=True)

            for data in [[["a", 1]], []]:
                df = self.df(data, "a:str,b:int")
                df3 = fi.rename(df, columns={})
                assert fi.get_schema(df3) == "a:str,b:int"
                df_eq(fi.as_fugue_df(df3), data, "a:str,b:int", throw=True)

        def test_rename_invalid(self):
            df = self.df([["a", 1]], "a:str,b:int")
            raises(
                FugueDataFrameOperationError,
                lambda: fi.rename(df, columns=dict(aa="ab")),
            )

        def test_as_array(self):
            for func in [
                lambda df, *args, **kwargs: fi.as_array(
                    df, *args, **kwargs, type_safe=True
                ),
                lambda df, *args, **kwargs: list(
                    fi.as_array_iterable(df, *args, **kwargs, type_safe=True)
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

        def test_as_array_special_values(self):
            for func in [
                lambda df, *args, **kwargs: fi.as_array(
                    df, *args, **kwargs, type_safe=True
                ),
                lambda df, *args, **kwargs: list(
                    fi.as_array_iterable(df, *args, **kwargs, type_safe=True)
                ),
            ]:
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
            assert [dict(a=None, b=1)] == list(fi.as_dict_iterable(df))
            df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
            assert [dict(a=datetime(2020, 1, 1), b=1)] == list(fi.as_dict_iterable(df))

        def test_list_type(self):
            data = [[[30, 40]]]
            df = self.df(data, "a:[int]")
            a = fi.as_array(df, type_safe=True)
            assert data == a

        def test_struct_type(self):
            data = [[{"a": 1}], [{"a": 2}]]
            df = self.df(data, "x:{a:int}")
            a = fi.as_array(df, type_safe=True)
            assert data == a

        def test_map_type(self):
            data = [[[("a", 1), ("b", 3)]], [[("b", 2)]]]
            df = self.df(data, "x:<str,int>")
            a = fi.as_array(df, type_safe=True)
            assert data == a

        def test_deep_nested_types(self):
            data = [[dict(a="1", b=[3, 4], d=1.0)], [dict(b=[30, 40])]]
            df = self.df(data, "a:{a:str,b:[int]}")
            a = fi.as_array(df, type_safe=True)
            assert [[dict(a="1", b=[3, 4])], [dict(a=None, b=[30, 40])]] == a

            data = [[[dict(b=[30, 40])]]]
            df = self.df(data, "a:[{a:str,b:[int]}]")
            a = fi.as_array(df, type_safe=True)
            assert [[[dict(a=None, b=[30, 40])]]] == a

        def test_binary_type(self):
            data = [[b"\x01\x05"]]
            df = self.df(data, "a:bytes")
            a = fi.as_array(df, type_safe=True)
            assert data == a

        def test_as_arrow(self):
            # empty
            df = self.df([], "a:int,b:int")
            assert [] == list(ArrowDataFrame(fi.as_arrow(df)).as_dict_iterable())
            assert fi.is_local(fi.as_arrow(df))
            # pd.Nat
            df = self.df([[pd.NaT, 1]], "a:datetime,b:int")
            assert [dict(a=None, b=1)] == list(
                ArrowDataFrame(fi.as_arrow(df)).as_dict_iterable()
            )
            # pandas timestamps
            df = self.df([[pd.Timestamp("2020-01-01"), 1]], "a:datetime,b:int")
            assert [dict(a=datetime(2020, 1, 1), b=1)] == list(
                ArrowDataFrame(fi.as_arrow(df)).as_dict_iterable()
            )
            # float nan, list
            data = [[[float("nan"), 2.0]]]
            df = self.df(data, "a:[float]")
            assert [[[None, 2.0]]] == ArrowDataFrame(fi.as_arrow(df)).as_array()
            # dict
            data = [[dict(b=True)]]
            df = self.df(data, "a:{b:bool}")
            assert data == ArrowDataFrame(fi.as_arrow(df)).as_array()
            # list[dict]
            data = [[[dict(b=[30, 40])]]]
            df = self.df(data, "a:[{b:[int]}]")
            assert data == ArrowDataFrame(fi.as_arrow(df)).as_array()

        def test_head(self):
            df = self.df([], "a:str,b:int")
            assert [] == fi.as_array(fi.head(df, 1))
            assert [] == fi.as_array(fi.head(df, 1, ["b"]))
            df = self.df([["a", 1]], "a:str,b:int")
            if fi.is_bounded(df):
                assert [["a", 1]] == fi.as_array(fi.head(df, 1))
            assert [[1, "a"]] == fi.as_array(fi.head(df, 1, ["b", "a"]))
            assert [] == fi.as_array(fi.head(df, 0))

            df = self.df([[0, 1], [0, 2], [1, 1], [1, 3]], "a:int,b:int")
            assert 2 == fi.count(fi.head(df, 2))
            df = self.df([[0, 1], [0, 2], [1, 1], [1, 3]], "a:int,b:int")
            assert 4 == fi.count(fi.head(df, 10))
            h = fi.head(df, 10)
            assert fi.is_local(h) and fi.is_bounded(h)

        def test_show(self):
            df = self.df([["a", 1]], "a:str,b:int")
            fi.show(df)

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
            ndf = fi.alter_columns(df, "a:str,b:str")
            assert [] == fi.as_array(ndf, type_safe=True)
            assert fi.get_schema(ndf) == "a:str,b:str"

            # no change
            df = self.df([["a", 1], ["c", None]], "a:str,b:int")
            ndf = fi.alter_columns(df, "b:int,a:str", as_fugue=True)
            assert [["a", 1], ["c", None]] == fi.as_array(ndf, type_safe=True)
            assert fi.get_schema(ndf) == "a:str,b:int"

            # bool -> str
            df = self.df([["a", True], ["b", False], ["c", None]], "a:str,b:bool")
            ndf = fi.alter_columns(df, "b:str", as_fugue=True)
            actual = fi.as_array(ndf, type_safe=True)
            # Capitalization doesn't matter
            # and dataframes don't need to be consistent on capitalization
            expected1 = [["a", "True"], ["b", "False"], ["c", None]]
            expected2 = [["a", "true"], ["b", "false"], ["c", None]]
            assert expected1 == actual or expected2 == actual
            assert fi.get_schema(ndf) == "a:str,b:str"

            # int -> str
            df = self.df([["a", 1], ["c", None]], "a:str,b:int")
            ndf = fi.alter_columns(df, "b:str", as_fugue=True)
            arr = fi.as_array(ndf, type_safe=True)
            assert [["a", "1"], ["c", None]] == arr or [
                ["a", "1.0"],
                ["c", None],
            ] == arr  # in pandas case, it can't treat [1, None] as an int col
            assert fi.get_schema(ndf) == "a:str,b:str"

            # int -> double
            df = self.df([["a", 1], ["c", None]], "a:str,b:int")
            ndf = fi.alter_columns(df, "b:double", as_fugue=True)
            assert [["a", 1], ["c", None]] == fi.as_array(ndf, type_safe=True)
            assert fi.get_schema(ndf) == "a:str,b:double"

            # double -> str
            df = self.df([["a", 1.1], ["b", None]], "a:str,b:double")
            data = fi.as_array(
                fi.alter_columns(df, "b:str", as_fugue=True), type_safe=True
            )
            assert [["a", "1.1"], ["b", None]] == data

            # double -> int
            df = self.df([["a", 1.0], ["b", None]], "a:str,b:double")
            data = fi.as_array(
                fi.alter_columns(df, "b:int", as_fugue=True), type_safe=True
            )
            assert [["a", 1], ["b", None]] == data

            # date -> str
            df = self.df(
                [["a", date(2020, 1, 1)], ["b", date(2020, 1, 2)], ["c", None]],
                "a:str,b:date",
            )
            data = fi.as_array(
                fi.alter_columns(df, "b:str", as_fugue=True), type_safe=True
            )
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
            data = fi.as_array(
                fi.alter_columns(df, "b:str", as_fugue=True), type_safe=True
            )
            assert [
                ["a", "2020-01-01 03:04:05"],
                ["b", "2020-01-02 16:07:08"],
                ["c", None],
            ] == data

            # str -> bool
            df = self.df([["a", "trUe"], ["b", "False"], ["c", None]], "a:str,b:str")
            ndf = fi.alter_columns(df, "b:bool,a:str", as_fugue=True)
            assert [["a", True], ["b", False], ["c", None]] == fi.as_array(
                ndf, type_safe=True
            )
            assert fi.get_schema(ndf) == "a:str,b:bool"

            # str -> int
            df = self.df([["a", "1"]], "a:str,b:str")
            ndf = fi.alter_columns(df, "b:int,a:str")
            assert [["a", 1]] == fi.as_array(ndf, type_safe=True)
            assert fi.get_schema(ndf) == "a:str,b:int"

            # str -> double
            df = self.df([["a", "1.1"], ["b", "2"], ["c", None]], "a:str,b:str")
            ndf = fi.alter_columns(df, "b:double", as_fugue=True)
            assert [["a", 1.1], ["b", 2.0], ["c", None]] == fi.as_array(
                ndf, type_safe=True
            )
            assert fi.get_schema(ndf) == "a:str,b:double"

            # str -> date
            df = self.df(
                [["1", "2020-01-01"], ["2", "2020-01-02"], ["3", None]],
                "a:str,b:str",
            )
            ndf = fi.alter_columns(df, "b:date,a:int", as_fugue=True)
            assert [
                [1, date(2020, 1, 1)],
                [2, date(2020, 1, 2)],
                [3, None],
            ] == fi.as_array(ndf, type_safe=True)
            assert fi.get_schema(ndf) == "a:int,b:date"

            # str -> datetime
            df = self.df(
                [
                    ["1", "2020-01-01 01:02:03"],
                    ["2", "2020-01-02 01:02:03"],
                    ["3", None],
                ],
                "a:str,b:str",
            )
            ndf = fi.alter_columns(df, "b:datetime,a:int", as_fugue=True)
            assert [
                [1, datetime(2020, 1, 1, 1, 2, 3)],
                [2, datetime(2020, 1, 2, 1, 2, 3)],
                [3, None],
            ] == fi.as_array(ndf, type_safe=True)
            assert fi.get_schema(ndf) == "a:int,b:datetime"

        def test_alter_columns_invalid(self):
            # invalid conversion
            with raises(Exception):
                df = self.df(
                    [["1", "x"], ["2", "y"], ["3", None]],
                    "a:str,b:str",
                )
                ndf = fi.alter_columns(df, "b:int")
                fi.show(ndf)  # lazy dataframes will force to materialize

    class NativeTests(Tests):
        def to_native_df(self, pdf: pd.DataFrame) -> Any:  # pragma: no cover
            raise NotImplementedError

        def test_get_altered_schema(self):
            pass

        def test_get_column_names(self):
            df = self.to_native_df(pd.DataFrame([[0, 1, 2]], columns=["0", "1", "2"]))
            assert fi.get_column_names(df) == ["0", "1", "2"]

        def test_rename_any_names(self):
            pdf = self.to_native_df(pd.DataFrame([[0, 1, 2]], columns=["a", "b", "c"]))
            df = fi.rename(pdf, {})
            assert fi.get_column_names(df) == ["a", "b", "c"]

            pdf = self.to_native_df(pd.DataFrame([[0, 1, 2]], columns=["0", "1", "2"]))
            df = fi.rename(pdf, {"0": "_0", "1": "_1", "2": "_2"})
            assert fi.get_column_names(df) == ["_0", "_1", "_2"]
