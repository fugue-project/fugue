from typing import Any, Dict, Iterable, List

import pandas as pd
from fugue.dataframe import ArrayDataFrame, DataFrames
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions.transformer import (CoTransformer, cotransformer,
                                          to_transformer)
from pytest import raises
from triad.collections.schema import Schema
from triad.utils.hash import to_uuid


def test_transformer():
    assert isinstance(t1, CoTransformer)
    df1 = ArrayDataFrame([[0, 2]], "a:int,b:int")
    df2 = ArrayDataFrame([[0, 2]], "a:int,c:int")
    dfs = DataFrames(df1, df2)
    t1._output_schema = t1.get_output_schema(dfs)
    assert t1.output_schema == "a:int,b:int"
    t2._output_schema = t2.get_output_schema(dfs)
    assert t2.output_schema == "b:int,a:int"
    assert [[0, 2, 1]] == list(t3(df1.as_array(), df2.as_pandas()))


def test_to_transformer():
    a = to_transformer(MockTransformer)
    assert isinstance(a, MockTransformer)
    b = to_transformer("MockTransformer")
    assert isinstance(b, MockTransformer)

    a = to_transformer(t1, None)
    assert isinstance(a, CoTransformer)
    a._x = 1
    # every parse should produce a different transformer even the input is
    # a transformer instance
    b = to_transformer(t1, None)
    assert isinstance(b, CoTransformer)
    assert "_x" not in b.__dict__
    c = to_transformer("t1", None)
    assert isinstance(c, CoTransformer)
    assert "_x" not in c.__dict__
    c._x = 1
    d = to_transformer("t1", None)
    assert isinstance(d, CoTransformer)
    assert "_x" not in d.__dict__
    raises(FugueInterfacelessError, lambda: to_transformer(t4, None))
    raises(FugueInterfacelessError, lambda: to_transformer("t4", None))
    e = to_transformer("t4", "a:int,b:int")
    assert isinstance(e, CoTransformer)


def test_to_transformer_determinism():
    a = to_transformer(t1, None)
    b = to_transformer(t1, None)
    c = to_transformer("t1", None)
    assert a is not b
    assert to_uuid(a) == to_uuid(b)
    assert a is not c
    assert to_uuid(a) == to_uuid(c)

    a = to_transformer(t4, "*,b:int")
    b = to_transformer("t4", "*,b:int")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)

    a = to_transformer(t4, "a:int,b:int")
    b = to_transformer("t4", Schema("a:int,b:int"))
    assert a is not b
    assert to_uuid(a) == to_uuid(b)

    a = to_transformer(MockTransformer)
    b = to_transformer("MockTransformer")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)


@cotransformer(["a:int", None, "b:int"])
def t1(df1: Iterable[Dict[str, Any]], df2: pd.DataFrame) -> Iterable[Dict[str, Any]]:
    for r in df1:
        r["b"] = 1
        yield r


@cotransformer([Schema("b:int"), "a:int"])
def t2(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = 1
        yield r


@cotransformer("a:int,b:int,c:int")
def t3(df1: Iterable[List[Any]], df2: pd.DataFrame) -> Iterable[List[Any]]:
    for r in df1:
        r += [1]
        yield r


def t4(df1: Iterable[List[Any]], df2: pd.DataFrame) -> Iterable[List[Any]]:
    for r in df1:
        r += [1]
        yield r


class MockTransformer(CoTransformer):
    def get_output_schema(self, dfs):
        pass

    def transform(self, dfs):
        pass
