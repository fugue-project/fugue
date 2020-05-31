from fugue.dataframe import ArrayDataFrame, DataFrame
from triad.collections.schema import Schema
import copy


def test_show():
    df = ArrayDataFrame(schema="a:str,b:str")
    df.show()

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
    df.show(best_width=2)

    s = " ".join(["x"] * 200)
    df = ArrayDataFrame([[s, 1], ["b", "s"]], "a:str,b:str", metadata=dict(a=1, b=2))
    df.show(rows=1, show_count=True, title="abc")


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
    def __init__(self, df=None, schema=None, metadata=None):
        super(). __init__(df=df, schema=schema, metadata=metadata)
        DataFrame.__init__(self, lambda: Schema(schema))
