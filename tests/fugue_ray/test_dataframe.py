from typing import Any

import pandas as pd
import pyarrow as pa
import ray
import ray.data as rd
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.arrow_dataframe import _build_empty_arrow
from fugue.dataframe.utils import (
    get_column_names,
    rename,
)
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
from triad import Schema

from fugue_ray import RayDataFrame


class RayDataFrameTests(DataFrameTests.Tests):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def df(self, data: Any = None, schema: Any = None) -> RayDataFrame:
        return RayDataFrame(data, schema)

    def test_ray_init(self):
        df = RayDataFrame(schema="a:str,b:int")
        assert df.empty
        assert df.schema == "a:str,b:int"
        assert df.is_bounded

        df = RayDataFrame(pd.DataFrame([], columns=["a", "b"]), schema="a:str,b:int")
        assert df.empty
        assert df.schema == "a:str,b:int"
        assert df.is_bounded

        pdf = pd.DataFrame(dict(a=[1.1, 2.2], b=["a", "b"]))
        pdf = pdf[pdf.a < 0]
        df = RayDataFrame(pdf)
        assert df.empty
        assert df.schema == "a:double,b:str"
        assert df.is_bounded

        df = RayDataFrame([], "x:str,y:double")
        assert df.empty
        assert not df.is_local
        assert df.is_bounded

        df = RayDataFrame(ArrayDataFrame([], "x:str,y:double"))
        assert df.empty
        assert not df.is_local
        assert df.is_bounded

        df = RayDataFrame(_build_empty_arrow(Schema("x:str,y:double")))
        assert df.empty
        assert df.schema == "x:str,y:double"
        assert df.is_bounded

        ddf = RayDataFrame(df)
        assert ddf.empty
        assert ddf.schema == "x:str,y:double"

        ddf = RayDataFrame(df.native, "x:str,y:double")
        assert ddf.empty
        assert ddf.schema == "x:str,y:double"

        ddf = RayDataFrame(df[["x"]].native, "x:str")
        assert ddf.empty
        assert ddf.schema == "x:str"

        raises(Exception, lambda: RayDataFrame(123))

        raises(NotImplementedError, lambda: RayDataFrame(rd.from_items([1, 2])))

    def test_ray_cast(self):
        data = [["a", "1"], ["b", "2"]]
        df = RayDataFrame(data, "a:str,b:str")
        assert [["a", "1"], ["b", "2"]] == df.as_array(type_safe=True)
        data = [["a", 1], ["b", 2]]
        df = RayDataFrame(data, "a:str,b:int")
        assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)
        df = RayDataFrame(data, "a:str,b:double")
        assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)

        ddf = RayDataFrame(df.native)
        assert [["a", 1.0], ["b", 2.0]] == ddf.as_array(type_safe=True)

        df = RayDataFrame(df.as_pandas(), "a:str,b:double")
        assert [["a", 1.0], ["b", 2.0]] == df.as_array(type_safe=True)
        df = RayDataFrame(df.as_pandas()["b"])
        assert [[1.0], [2.0]] == df.as_array(type_safe=True)

    def test_ray_num_partitions(self):
        rdf = rd.from_pandas(pd.DataFrame(dict(a=range(10))))
        df = RayDataFrame(rdf.repartition(5))
        assert 5 == df.num_partitions

    def _test_get_column_names(self):
        df = rd.from_pandas(pd.DataFrame([[0, 10, 20]], columns=["0", "1", "2"]))
        assert get_column_names(df) == ["0", "1", "2"]

        df = rd.from_arrow(
            pa.Table.from_pandas(pd.DataFrame([[0, 10, 20]], columns=["0", "1", "2"]))
        )
        assert get_column_names(df) == ["0", "1", "2"]

    def _test_rename(self):
        rdf = rd.from_pandas(pd.DataFrame([[0, 10, 20]], columns=["a", "b", "c"]))
        df = rename(rdf, {})
        assert isinstance(df, rd.Dataset)
        assert get_column_names(df) == ["a", "b", "c"]

        pdf = rd.from_pandas(pd.DataFrame([[0, 10, 20]], columns=["0", "1", "2"]))
        df = rename(pdf, {"0": "_0", "1": "_1", "2": "_2"})
        assert isinstance(df, rd.Dataset)
        assert get_column_names(df) == ["_0", "_1", "_2"]


class NativeRayDataFrameTests(DataFrameTests.NativeTests):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def df(self, data: Any = None, schema: Any = None):
        res = RayDataFrame(data, schema)
        # native ray dataset can't handle the schema when empty
        return res if res.empty else res.native

    def to_native_df(self, pdf: pd.DataFrame) -> Any:
        return rd.from_pandas(pdf)
