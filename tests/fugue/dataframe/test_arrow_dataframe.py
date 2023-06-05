from typing import Any

import pandas as pd
import pyarrow as pa
from fugue.dataframe import ArrowDataFrame
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
import fugue.api as fa


class ArrowDataFrameTests(DataFrameTests.Tests):
    def df(self, data: Any = None, schema: Any = None) -> ArrowDataFrame:
        return ArrowDataFrame(data, schema)


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
