from typing import Any, Dict, Iterable

from fugue.dataframe import ArrayDataFrame, DataFrame, DataFrames
from fugue.exceptions import FugueInterfacelessError
from fugue.execution import ExecutionEngine, NativeExecutionEngine
from fugue.extensions.outputter import (
    Outputter,
    _to_outputter,
    outputter,
    register_outputter,
)
from pytest import raises
from triad.collections.dict import ParamDict
from triad.utils.hash import to_uuid


def test_outputter():
    assert isinstance(t1, Outputter)
    assert isinstance(t2, Outputter)


def test_register():
    register_outputter("x", MockOutputter)
    b = _to_outputter("x")
    assert isinstance(b, MockOutputter)

    raises(
        KeyError,
        lambda: register_outputter("x", MockOutputter, on_dup="raise"),
    )


def test__to_outputter():
    a = _to_outputter(MockOutputter)
    assert isinstance(a, MockOutputter)
    b = _to_outputter("MockOutputter")
    assert isinstance(b, MockOutputter)

    a = _to_outputter(T0)
    assert isinstance(a, Outputter)
    a = _to_outputter(T0())

    assert isinstance(a, Outputter)
    a = _to_outputter(t1)
    assert isinstance(a, Outputter)
    a._x = 1
    b = _to_outputter(t1)
    assert isinstance(b, Outputter)
    assert "_x" not in b.__dict__
    c = _to_outputter(t1)
    assert isinstance(c, Outputter)
    assert "_x" not in c.__dict__
    c._x = 1
    d = _to_outputter("t1")
    assert isinstance(d, Outputter)
    assert "_x" not in d.__dict__
    raises(FugueInterfacelessError, lambda: _to_outputter("abc"))

    assert isinstance(_to_outputter(t3), Outputter)
    assert isinstance(_to_outputter(t4), Outputter)
    assert isinstance(_to_outputter(t5), Outputter)
    assert isinstance(_to_outputter(t6), Outputter)
    assert isinstance(_to_outputter(t7), Outputter)


def test_run_outputter():
    df = ArrayDataFrame([[0]], "a:int")
    dfs = DataFrames(df1=df, df2=df)
    dfs2 = DataFrames(df, df)
    assert not dfs2.has_key

    class Ct(object):
        pass

    c = Ct()
    o1 = _to_outputter(t3)
    o1(df, df, 2, c)
    assert 4 == c.value
    c.value = 0
    o1._params = ParamDict([("a", 2), ("b", c)], deep=False)
    o1._execution_engine = None
    o1.process(dfs)
    assert 4 == c.value
    c.value = 0
    o1._params = ParamDict([("a", 2), ("b", c)], deep=False)
    o1._execution_engine = None
    o1.process(dfs2)
    assert 4 == c.value

    c = Ct()
    o1 = _to_outputter(t5)
    o1("dummy", dfs, 2, c)
    assert 4 == c.value
    c.value = 0
    o1("dummy", dfs2, 2, c)
    assert 4 == c.value
    c.value = 0
    o1._params = ParamDict([("a", 2), ("b", c)], deep=False)
    o1._execution_engine = NativeExecutionEngine()
    o1.process(dfs)
    assert 4 == c.value
    c.value = 0
    o1._params = ParamDict([("a", 2), ("b", c)], deep=False)
    o1._execution_engine = NativeExecutionEngine()
    o1.process(dfs2)
    assert 4 == c.value


def test__to_outputter_determinism():
    a = _to_outputter(t1)
    b = _to_outputter(t1)
    c = _to_outputter("t1")
    d = _to_outputter("t2")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)
    assert a is not c
    assert to_uuid(a) == to_uuid(c)
    assert to_uuid(a) != to_uuid(d)

    a = _to_outputter(MockOutputter)
    b = _to_outputter("MockOutputter")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)


def test_to_outputter_validation():
    @outputter(input_has=" a , b ")
    def ov1(df: Iterable[Dict[str, Any]]) -> None:
        pass

    # input_has: a , b
    def ov2(df: Iterable[Dict[str, Any]]) -> None:
        pass

    class MockOutputterV(Outputter):
        @property
        def validation_rules(self):
            return {"input_is": "a:int,b:int"}

        def process(self, dfs):
            pass

    a = _to_outputter(ov1, None)
    assert {"input_has": ["a", "b"]} == a.validation_rules
    b = _to_outputter(ov2, None)
    assert {"input_has": ["a", "b"]} == b.validation_rules
    c = _to_outputter(MockOutputterV)
    assert {"input_is": "a:int,b:int"} == c.validation_rules


class T0(Outputter):
    def process(self, dfs):
        pass


@outputter()
def t1(df: Iterable[Dict[str, Any]]) -> None:
    pass


@outputter()
def t2(e: ExecutionEngine, df1: DataFrame, df2: DataFrame) -> None:
    pass


def t3(df1: DataFrame, df2: DataFrame, a, b) -> None:
    b.value = df1.count() + df2.count() + a


def t4(e: ExecutionEngine, df1: DataFrame, df2: DataFrame, a: int, b: str) -> None:
    pass


def t5(e: ExecutionEngine, dfs: DataFrames, a, b) -> None:
    assert e is not None
    b.value = sum(x.count() for x in dfs.values()) + a


def t6(dfs: DataFrames) -> None:
    pass


def t7(df7: DataFrames, **kwargs) -> None:
    pass


class MockOutputter(Outputter):
    def process(self, dfs):
        pass
