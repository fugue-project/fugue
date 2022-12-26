import copy

import pandas as pd
from pytest import raises
from triad.collections.schema import Schema

from fugue.dataframe import ArrayDataFrame, DataFrame
from fugue.api import as_fugue_df, get_native_as_df
from fugue.bag.array_bag import ArrayBag


def test_as_fugue_df():
    with raises(NotImplementedError):
        as_fugue_df(10)
    with raises(TypeError):
        as_fugue_df(ArrayBag([1, 2]))
    df = pd.DataFrame([[0]], columns=["a"])
    assert isinstance(as_fugue_df(df), DataFrame)


def test_get_native_as_df():
    with raises(NotImplementedError):
        get_native_as_df(10)
    # other tests are in the suites


def test_show():
    df = ArrayDataFrame(schema="a:str,b:str")
    df.show()

    assert repr(df) == df._repr_html_()

    s = " ".join(["x"] * 2)
    df = ArrayDataFrame([[s, 1], ["b", 2]], "a:str,b:str")
    df.show()

    s = " ".join(["x"] * 200)
    df = ArrayDataFrame([[s, 1], ["b", 2]], "a:str,b:str")
    df.show()

    s = " ".join(["x"] * 200)
    df = ArrayDataFrame([[s, 1], ["b", s]], "a:str,b:str")
    df.show()

    s = "".join(["x"] * 2000)
    df = ArrayDataFrame([[s, 1], ["b", None]], "a:str,b:str")
    df.show()

    s = " ".join(["x"] * 20)
    schema = [f"a{x}:str" for x in range(20)]
    data = [[f"aasdfjasdfka;sdf{x}:str" for x in range(20)]]
    df = ArrayDataFrame(data, schema)
    df.show()

    s = " ".join(["x"] * 200)
    df = ArrayDataFrame([[s, 1], ["b", "s"]], "a:str,b:str")
    df.show(n=1, with_count=True, title="abc")


def test_lazy_schema():
    df = MockDF([["a", 1], ["b", 2]], "a:str,b:str")
    assert callable(df._schema)
    assert df.schema == "a:str,b:str"


def test_get_info_str():
    df = ArrayDataFrame([["a", 1], ["b", 2]], "a:str,b:str")
    assert '{"schema": "a:str,b:str", "type": '
    '"tests.collections.dataframe.test_dataframe.MockDF", "metadata": {}}' == df.get_info_str()


def test_copy():
    df = ArrayDataFrame([["a", 1], ["b", 2]], "a:str,b:str")
    assert copy.copy(df) is df
    assert copy.deepcopy(df) is df


class MockDF(ArrayDataFrame):
    def __init__(self, df=None, schema=None):
        super().__init__(df=df, schema=schema)
        DataFrame.__init__(self, lambda: Schema(schema))
