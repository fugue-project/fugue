import json
import math
from datetime import datetime
from typing import Any

import dask.dataframe as pd
import numpy as np
import pandas as pd
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_dask import DaskDataFrame
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
from triad.collections.schema import Schema

try:
    import ray

    from fugue_ray import RayDataFrame
except Exception:
    pass


class RayDataFrameTests(DataFrameTests.Tests):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2)

    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> RayDataFrame:
        return RayDataFrame(data, schema, metadata)


def test_init():
    df = RayDataFrame(schema="a:str,b:int")
    assert df.empty
    assert df.schema == "a:str,b:int"
    assert df.is_bounded

    df = RayDataFrame(pd.DataFrame([], columns=["a", "b"]), schema="a:str,b:int")
    assert df.empty
    assert df.schema == "a:str,b:int"
    assert df.is_bounded

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

    df = RayDataFrame([], "x:str,y:double")
    assert df.empty
    assert not df.is_local
    assert df.is_bounded

    raises(Exception, lambda: RayDataFrame(123))
