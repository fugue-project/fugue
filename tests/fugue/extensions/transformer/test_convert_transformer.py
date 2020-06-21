from typing import Any, Dict, Iterable, List

from fugue.dataframe import ArrayDataFrame
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions.transformer import Transformer, _to_transformer, transformer
from pytest import raises
from triad.collections.schema import Schema
from triad.utils.hash import to_uuid


def test_transformer():
    assert isinstance(t1, Transformer)
    df = ArrayDataFrame([[0]], "a:int")
    t1._output_schema = t1.get_output_schema(df)
    assert t1.output_schema == "a:int,b:int"
    t2._output_schema = t2.get_output_schema(df)
    assert t2.output_schema == "b:int,a:int"
    t3._output_schema = t3.get_output_schema(df)
    assert t3.output_schema == "a:int,b:int"
    assert [[0, 1]] == list(t3(df.as_array_iterable()))


def test__to_transformer():
    a = _to_transformer(MockTransformer)
    assert isinstance(a, MockTransformer)
    b = _to_transformer("MockTransformer")
    assert isinstance(b, MockTransformer)

    a = _to_transformer(t1, None)
    assert isinstance(a, Transformer)
    a._x = 1
    # every parse should produce a different transformer even the input is
    # a transformer instance
    b = _to_transformer(t1, None)
    assert isinstance(b, Transformer)
    assert "_x" not in b.__dict__
    c = _to_transformer("t1", None)
    assert isinstance(c, Transformer)
    assert "_x" not in c.__dict__
    c._x = 1
    d = _to_transformer("t1", None)
    assert isinstance(d, Transformer)
    assert "_x" not in d.__dict__
    raises(FugueInterfacelessError, lambda: _to_transformer(t4, None))
    raises(FugueInterfacelessError, lambda: _to_transformer("t4", None))
    e = _to_transformer("t4", "*,b:int")
    assert isinstance(e, Transformer)
    f = _to_transformer("t5")
    assert isinstance(f, Transformer)


def test__to_transformer_determinism():
    a = _to_transformer(t1, None)
    b = _to_transformer(t1, None)
    c = _to_transformer("t1", None)
    assert a is not b
    assert to_uuid(a) == to_uuid(b)
    assert a is not c
    assert to_uuid(a) == to_uuid(c)

    a = _to_transformer(t4, "*,b:int")
    b = _to_transformer("t4", "*,b:int")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)

    a = _to_transformer(t4, "a:int,b:int")
    b = _to_transformer("t4", Schema("a:int,b:int"))
    assert a is not b
    assert to_uuid(a) == to_uuid(b)

    a = _to_transformer(MockTransformer)
    b = _to_transformer("MockTransformer")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)


@transformer(["*", None, "b:int"])
def t1(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = 1
        yield r


@transformer([Schema("b:int"), "*"])
def t2(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = 1
        yield r


@transformer(Schema("a:int, b:int"))
def t3(df: Iterable[List[Any]]) -> Iterable[List[Any]]:
    for r in df:
        r += [1]
        yield r


def t4(df: Iterable[List[Any]]) -> Iterable[List[Any]]:
    for r in df:
        r += [1]
        yield r


# schema: *,b:int
def t5(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = 1
        yield r


class MockTransformer(Transformer):
    def get_output_schema(self, df):
        pass

    def transform(self, df):
        pass
