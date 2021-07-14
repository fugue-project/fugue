from typing import Any, Dict, Iterable, List

import pandas as pd
from fugue.dataframe import ArrayDataFrame, DataFrame, DataFrames
from fugue.exceptions import FugueInterfacelessError
from fugue.execution import ExecutionEngine, NativeExecutionEngine
from fugue.extensions.processor import (
    Processor,
    _to_processor,
    processor,
    register_processor,
)
from pytest import raises
from triad.collections.dict import ParamDict
from triad.utils.hash import to_uuid


def test_processor():
    assert isinstance(t1, Processor)
    assert isinstance(t2, Processor)


def test_register():
    register_processor("x", MockProcessor)
    b = _to_processor("x")
    assert isinstance(b, MockProcessor)

    raises(
        KeyError,
        lambda: register_processor("x", MockProcessor, on_dup="throw"),
    )


def test__to_processor():
    a = _to_processor(MockProcessor)
    assert isinstance(a, MockProcessor)
    b = _to_processor("MockProcessor")
    assert isinstance(b, MockProcessor)

    a = _to_processor(T0)
    assert isinstance(a, Processor)
    a = _to_processor(T0())

    assert isinstance(a, Processor)
    a = _to_processor(t1)
    assert isinstance(a, Processor)
    a._x = 1
    b = _to_processor(t1)
    assert isinstance(b, Processor)
    assert "_x" not in b.__dict__
    c = _to_processor(t1)
    assert isinstance(c, Processor)
    assert "_x" not in c.__dict__
    c._x = 1
    d = _to_processor("t1")
    assert isinstance(d, Processor)
    assert "_x" not in d.__dict__
    raises(FugueInterfacelessError, lambda: _to_processor("abc"))

    assert isinstance(_to_processor(t3), Processor)
    assert isinstance(_to_processor(t4), Processor)
    assert isinstance(_to_processor(t5), Processor)
    assert isinstance(_to_processor(t6), Processor)
    raises(FugueInterfacelessError, lambda: _to_processor(t6, "a:int"))
    assert isinstance(_to_processor(t7, "a:int"), Processor)
    raises(FugueInterfacelessError, lambda: _to_processor(t7))
    assert isinstance(_to_processor(t8), Processor)
    assert isinstance(_to_processor(t9), Processor)
    assert isinstance(_to_processor(t10), Processor)
    assert isinstance(_to_processor(t11), Processor)


def test_run_processor():
    df = ArrayDataFrame([[0]], "a:int")
    dfs = DataFrames(df1=df, df2=df)
    dfs2 = DataFrames(df, df)
    assert not dfs2.has_key

    o1 = _to_processor(t3)
    assert 4 == o1(df, df, 2).as_array()[0][0]

    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = None
    assert 4 == o1.process(dfs).as_array()[0][0]
    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = None
    assert 4 == o1.process(dfs2).as_array()[0][0]

    o1 = _to_processor(t5)
    assert 4 == o1("dummy", dfs, 2)[0][0]
    assert 4 == o1("dummy", dfs2, 2)[0][0]
    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = NativeExecutionEngine()
    assert 4 == o1.process(dfs).as_array()[0][0]
    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = NativeExecutionEngine()
    assert 4 == o1.process(dfs2).as_array()[0][0]


def test__to_processor_determinism():
    a = _to_processor(t1, None)
    b = _to_processor(t1, None)
    c = _to_processor("t1", None)
    d = _to_processor("t2", None)
    assert a is not b
    assert to_uuid(a) == to_uuid(b)
    assert a is not c
    assert to_uuid(a) == to_uuid(c)
    assert to_uuid(a) != to_uuid(d)

    a = _to_processor(MockProcessor)
    b = _to_processor("MockProcessor")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)


def test_to_processor_validation():
    @processor("b:int", input_has=" a , b ")
    def pv1(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        for r in df:
            r["b"] = 1
            yield r

    # input_has: a , b
    # schema: b:int
    def pv2(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        for r in df:
            r["b"] = 1
            yield r

    class MockProcessorV(Processor):
        @property
        def validation_rules(self):
            return {"input_is": "a:int,b:int"}

        def process(self, dfs):
            return dfs[0]

    a = _to_processor(pv1, None)
    assert {"input_has": ["a", "b"]} == a.validation_rules
    b = _to_processor(pv2, None)
    assert {"input_has": ["a", "b"]} == b.validation_rules
    c = _to_processor(MockProcessorV)
    assert {"input_is": "a:int,b:int"} == c.validation_rules


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


def t9(e: ExecutionEngine, df1: DataFrame, df2: DataFrame) -> pd.DataFrame:
    pass


def t10(e: ExecutionEngine, df1: DataFrame, df2: DataFrame) -> Iterable[pd.DataFrame]:
    pass


def t11(
    e: ExecutionEngine, df1: DataFrame, df2: DataFrame, **kwargs
) -> Iterable[pd.DataFrame]:
    pass


class MockProcessor(Processor):
    def process(self, dfs):
        pass
