from typing import Any, Dict, Iterable, List

from fugue.dataframe import ArrayDataFrame, DataFrame, DataFrames
from fugue.exceptions import FugueInterfacelessError
from fugue.execution import ExecutionEngine
from fugue.outputter import Outputter, outputter, to_outputter
from pytest import raises
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema


def test_outputter():
    assert isinstance(t1, Outputter)
    assert isinstance(t2, Outputter)


def test_to_outputter():
    a = to_outputter(T0)
    assert isinstance(a, Outputter)
    a = to_outputter(T0())

    assert isinstance(a, Outputter)
    a = to_outputter(t1)
    assert isinstance(a, Outputter)
    a._x = 1
    b = to_outputter(t1)
    assert isinstance(b, Outputter)
    assert "_x" not in b.__dict__
    c = to_outputter(t1)
    assert isinstance(c, Outputter)
    assert "_x" not in c.__dict__
    c._x = 1
    d = to_outputter("t1")
    assert isinstance(d, Outputter)
    assert "_x" not in d.__dict__
    raises(FugueInterfacelessError, lambda: to_outputter("abc"))

    assert isinstance(to_outputter(t3), Outputter)
    assert isinstance(to_outputter(t4), Outputter)
    assert isinstance(to_outputter(t5), Outputter)
    assert isinstance(to_outputter(t6), Outputter)


def test_run_outputter():
    df = ArrayDataFrame([[0]], "a:int")
    dfs = DataFrames(df1=df, df2=df)
    dfs2 = DataFrames(df, df)
    assert not dfs2.has_key

    class Ct(object):
        pass

    c = Ct()
    o1 = to_outputter(t3)
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
    o1 = to_outputter(t5)
    o1("dummy", dfs, 2, c)
    assert 4 == c.value
    c.value = 0
    o1("dummy", dfs2, 2, c)
    assert 4 == c.value
    c.value = 0
    o1._params = ParamDict([("a", 2), ("b", c)], deep=False)
    o1._execution_engine = "dummy"
    o1.process(dfs)
    assert 4 == c.value
    c.value = 0
    o1._params = ParamDict([("a", 2), ("b", c)], deep=False)
    o1._execution_engine = "dummy"
    o1.process(dfs2)
    assert 4 == c.value


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
