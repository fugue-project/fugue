from typing import Any, Dict, Iterable, List

from fugue.dataframe import ArrayDataFrame, DataFrame, DataFrames
from fugue.dataframe.dataframe import LocalDataFrame
from fugue.exceptions import FugueInterfacelessError
from fugue.execution import ExecutionEngine
from fugue.extensions.processor import Processor, processor, to_processor
from pytest import raises
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema


def test_processor():
    assert isinstance(t1, Processor)
    assert isinstance(t2, Processor)


def test_to_processor():
    a = to_processor(T0)
    assert isinstance(a, Processor)
    a = to_processor(T0())

    assert isinstance(a, Processor)
    a = to_processor(t1)
    assert isinstance(a, Processor)
    a._x = 1
    b = to_processor(t1)
    assert isinstance(b, Processor)
    assert "_x" not in b.__dict__
    c = to_processor(t1)
    assert isinstance(c, Processor)
    assert "_x" not in c.__dict__
    c._x = 1
    d = to_processor("t1")
    assert isinstance(d, Processor)
    assert "_x" not in d.__dict__
    raises(FugueInterfacelessError, lambda: to_processor("abc"))

    assert isinstance(to_processor(t3), Processor)
    assert isinstance(to_processor(t4), Processor)
    assert isinstance(to_processor(t5), Processor)
    assert isinstance(to_processor(t6), Processor)
    raises(FugueInterfacelessError, lambda: to_processor(t6, "a:int"))
    assert isinstance(to_processor(t7, "a:int"), Processor)
    raises(FugueInterfacelessError, lambda: to_processor(t7))
    assert isinstance(to_processor(t8), Processor)


def test_run_processor():
    df = ArrayDataFrame([[0]], "a:int")
    dfs = DataFrames(df1=df, df2=df)
    dfs2 = DataFrames(df, df)
    assert not dfs2.has_key

    o1 = to_processor(t3)
    assert 4 == o1(df, df, 2).as_array()[0][0]

    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = None
    assert 4 == o1.process(dfs).as_array()[0][0]
    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = None
    assert 4 == o1.process(dfs2).as_array()[0][0]

    o1 = to_processor(t5)
    assert 4 == o1("dummy", dfs, 2)[0][0]
    assert 4 == o1("dummy", dfs2, 2)[0][0]
    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = "dummy"
    assert 4 == o1.process(dfs).as_array()[0][0]
    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = "dummy"
    assert 4 == o1.process(dfs2).as_array()[0][0]


class T0(Processor):
    def process(self, dfs) -> DataFrame:
        return dfs[0]


@processor("")
def t1(df: Iterable[Dict[str, Any]]) -> DataFrame:
    pass


@processor("a:int")
def t2(e: ExecutionEngine, df1: DataFrame, df2: DataFrame) -> List[List[Any]]:
    pass


def t3(df1: DataFrame, df2: DataFrame, a) -> DataFrame:
    value = df1.count() + df2.count() + a
    return ArrayDataFrame([[value]], "a:int")


def t4(e: ExecutionEngine, df1: DataFrame, df2: DataFrame, a: int, b: str) -> DataFrame:
    pass


@processor("a:int")
def t5(e: ExecutionEngine, dfs: DataFrames, a) -> List[List[Any]]:
    assert e is not None
    value = sum(x.count() for x in dfs.values()) + a
    return ArrayDataFrame([[value]], "a:int").as_array()


def t6(dfs: DataFrames) -> DataFrame:
    pass


def t7(dfs: DataFrames) -> List[List[Any]]:
    pass


# schema: a:int
def t8(e: ExecutionEngine, df1: DataFrame, df2: DataFrame) -> List[List[Any]]:
    pass
