from typing import Any

import dask.dataframe as pd
import pandas as pd
import pyarrow as pa
import ray
import ray.data as rd
from fugue.dataframe.array_dataframe import ArrayDataFrame
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

    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> RayDataFrame:
        return RayDataFrame(data, schema, metadata)

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

        df = RayDataFrame(pa.Table.from_pylist([], Schema("x:str,y:double").pa_schema))
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

    def test_ray_metadata(self):
        data = ArrayDataFrame(
            [["a", "1"], ["b", "2"]], "a:str,b:str", metadata=dict(a=1)
        )
        df = RayDataFrame(data)
        assert df.metadata == dict(a=1)
        df = RayDataFrame(df)
        assert df.metadata == dict(a=1)

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
