from typing import Any

import pandas as pd
import pyarrow as pa
from pytest import raises

import fugue.api as fa
import fugue.test as ft
from fugue.dataframe import ArrowDataFrame
from fugue_test.dataframe_suite import DataFrameTests


@ft.fugue_test_suite("native", mark_test=True)
class ArrowDataFrameTests(DataFrameTests.Tests):
    def df(self, data: Any = None, schema: Any = None) -> ArrowDataFrame:
        return ArrowDataFrame(data, schema)


@ft.fugue_test_suite("native", mark_test=True)
class NativeArrowDataFrameTests(DataFrameTests.NativeTests):
    def df(self, data: Any = None, schema: Any = None) -> pd.DataFrame:
        return ArrowDataFrame(data, schema).as_arrow()

    def to_native_df(self, pdf: pd.DataFrame) -> Any:  # pragma: no cover
        return pa.Table.from_pandas(pdf)

    def test_num_partitions(self):
        assert fa.get_num_partitions(self.df([[0, 1]], "a:int,b:int")) == 1


def test_init():
    df = ArrowDataFrame(schema="a:str,b:int")
    assert df.empty
    assert df.schema == "a:str,b:int"
    assert df.is_bounded

    df = ArrowDataFrame(pd.DataFrame([], columns=["a", "b"]), schema="a:str,b:int")
    assert df.empty
    assert df.schema == "a:str,b:int"
    assert df.is_bounded

    data = [["a", "1"], ["b", "2"]]
    df = ArrowDataFrame(data, "a:str,b:str")
    assert [["a", "1"], ["b", "2"]] == df.as_array(type_safe=True)
    data = [["a", 1], ["b", 2]]
    df = ArrowDataFrame(data, "a:str,b:int")
    assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)
    df = ArrowDataFrame(data, "a:str,b:double")
    assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)

    ddf = ArrowDataFrame(df.native)
    assert [["a", 1.0], ["b", 2.0]] == ddf.as_array(type_safe=True)

    df = ArrowDataFrame(df.as_pandas(), "a:str,b:double")
    assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)
    df = ArrowDataFrame(df.as_pandas()["b"])
    assert [[1.0], [2.0]] == df.as_array(type_safe=True)

    df = ArrowDataFrame([], "x:str,y:double")
    assert df.empty
    assert df.is_local
    assert df.is_bounded

    raises(Exception, lambda: ArrowDataFrame(123))


def test_large_types_are_accepted():
    t = pa.table(
        {
            "a": pa.array(["x", None], pa.large_string()),
            "b": pa.array([["y"], None], pa.large_list(pa.large_string())),
            "c": pa.array([b"z", None], pa.large_binary()),
        }
    )
    df = ArrowDataFrame(t)
    assert df.schema == "a:str,b:[str],c:bytes"
    assert df.as_array() == [["x", ["y"], b"z"], [None, None, None]]


def test_no_copy_without_large_types():
    t = pa.table({"a": pa.array(["x"], pa.string())})
    assert ArrowDataFrame(t).native is t


def _large_table():
    return pa.table(
        {
            "a": pa.array(["x", None], pa.large_string()),
            "b": pa.array([["y"], None], pa.large_list(pa.large_string())),
        }
    )


def test_get_schema_large_types():
    assert fa.get_schema(_large_table()) == "a:str,b:[str]"


def test_alter_columns_large_types():
    res = fa.alter_columns(_large_table(), "a:str")
    assert fa.get_schema(res) == "a:str,b:[str]"
