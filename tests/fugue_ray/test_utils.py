import ray
from triad import Schema

from fugue_ray import RayDataFrame
from fugue_ray._utils.dataframe import add_partition_key


def test_add_partition_key():
    with ray.init():
        df = RayDataFrame([[0, "a"], [1, "b"]], "a:int,b:str")
        res, s = add_partition_key(df.native, df.schema, ["b", "a"], output_key="x")
        assert s == Schema("a:int,b:str,x:binary")

        res, s = add_partition_key(df.native, df.schema, ["b"], output_key="x")
        assert s == "a:int,b:str,x:str"
        assert RayDataFrame(res, s).as_array() == [[0, "a", "a"], [1, "b", "b"]]
