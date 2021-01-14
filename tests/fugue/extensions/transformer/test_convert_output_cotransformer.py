from typing import Any, Dict, Iterable, List, Callable

import pandas as pd
from fugue.dataframe import ArrayDataFrame, DataFrames
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions.transformer import (
    CoTransformer,
    _to_output_transformer,
    output_cotransformer,
)
from fugue.extensions.transformer.constants import OUTPUT_TRANSFORMER_DUMMY_SCHEMA
from pytest import raises
from triad.collections.schema import Schema
from triad.utils.hash import to_uuid


def test_transformer():
    assert isinstance(t1, CoTransformer)
    df1 = ArrayDataFrame([[0, 2]], "a:int,b:int")
    df2 = ArrayDataFrame([[0, 2]], "a:int,c:int")
    dfs = DataFrames(df1, df2)
    assert t1.get_output_schema(dfs) == OUTPUT_TRANSFORMER_DUMMY_SCHEMA


def test__to_output_transformer():
    a = _to_output_transformer(MockTransformer)
    assert isinstance(a, MockTransformer)
    b = _to_output_transformer("MockTransformer")
    assert isinstance(b, MockTransformer)

    a = _to_output_transformer(t1)
    assert isinstance(a, CoTransformer)
    a._x = 1
    # every parse should produce a different transformer even the input is
    # a transformer instance
    b = _to_output_transformer(t1)
    assert isinstance(b, CoTransformer)
    assert "_x" not in b.__dict__
    c = _to_output_transformer("t1")
    assert isinstance(c, CoTransformer)
    assert "_x" not in c.__dict__
    c._x = 1
    d = _to_output_transformer("t1")
    assert isinstance(d, CoTransformer)
    assert "_x" not in d.__dict__
    e = _to_output_transformer("t4")
    assert isinstance(e, CoTransformer)
    f = _to_output_transformer("t5")
    assert isinstance(f, CoTransformer)
    g = _to_output_transformer("t6")
    assert isinstance(g, CoTransformer)
    i = _to_output_transformer("t7")
    assert isinstance(i, CoTransformer)


def test__to_output_transformer_determinism():
    a = _to_output_transformer(t1)
    b = _to_output_transformer(t1)
    c = _to_output_transformer("t1")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)
    assert a is not c
    assert to_uuid(a) == to_uuid(c)

    a = _to_output_transformer(t4)
    b = _to_output_transformer("t4")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)

    a = _to_output_transformer(MockTransformer)
    b = _to_output_transformer("MockTransformer")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)

    a = _to_output_transformer(t7)
    b = _to_output_transformer("t7")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)


@output_cotransformer()
def t1(df1: Iterable[Dict[str, Any]], df2: pd.DataFrame) -> None:
    pass


def t4(df1: Iterable[List[Any]], df2: pd.DataFrame) -> None:
    pass


def t5(df1: Iterable[List[Any]], df2: pd.DataFrame, **kwargs) -> None:
    pass


def t6(df1: Iterable[List[Any]], df2: pd.DataFrame) -> Iterable[pd.DataFrame]:
    pass

def t7(df1: Iterable[List[Any]], df2: pd.DataFrame, c:Callable) -> Iterable[pd.DataFrame]:
    pass


class MockTransformer(CoTransformer):
    def get_output_schema(self, dfs):
        pass

    def transform(self, dfs):
        pass
