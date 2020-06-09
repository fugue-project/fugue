from typing import Any, Dict, Iterable, List

from fugue.dataframe import ArrayDataFrame, DataFrame, DataFrames
from fugue.dataframe.dataframe import LocalDataFrame
from fugue.exceptions import FugueInterfacelessError
from fugue.execution import ExecutionEngine
from fugue.extensions.creator import Creator, creator, to_creator
from pytest import raises
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema
from triad.utils.hash import to_uuid


def test_creator():
    assert isinstance(t1, Creator)
    assert isinstance(t2, Creator)


def test_to_creator():
    a = to_creator(T0)
    assert isinstance(a, Creator)
    a = to_creator(T0())

    assert isinstance(a, Creator)
    a = to_creator(t1)
    assert isinstance(a, Creator)
    a._x = 1
    b = to_creator(t1)
    assert isinstance(b, Creator)
    assert "_x" not in b.__dict__
    c = to_creator(t1)
    assert isinstance(c, Creator)
    assert "_x" not in c.__dict__
    c._x = 1
    d = to_creator("t1")
    assert isinstance(d, Creator)
    assert "_x" not in d.__dict__
    raises(FugueInterfacelessError, lambda: to_creator("abc"))

    assert isinstance(to_creator(t3), Creator)
    assert isinstance(to_creator(t4), Creator)
    assert isinstance(to_creator(t5), Creator)
    assert isinstance(to_creator(t6), Creator)
    raises(FugueInterfacelessError, lambda: to_creator(t6, "a:int"))
    assert isinstance(to_creator(t7, "a:int"), Creator)
    raises(FugueInterfacelessError, lambda: to_creator(t7))
    assert isinstance(to_creator(t8), Creator)

def test_run_creator():
    o1 = to_creator(t3)
    assert 4 == o1(4).as_array()[0][0]

    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = None
    assert 2 == o1.create().as_array()[0][0]

    o1 = to_creator(t5)
    assert 4 == o1("dummy", 4)[0][0]
    o1._params = ParamDict([("a", 2)], deep=False)
    o1._execution_engine = "dummy"
    assert 2 == o1.create().as_array()[0][0]


def test_to_creator_determinism():
    a = to_creator(t1, None)
    b = to_creator(t1, None)
    c = to_creator("t1", None)
    d = to_creator("t2", None)
    assert a is not b
    assert to_uuid(a) == to_uuid(b)
    assert a is not c
    assert to_uuid(a) == to_uuid(c)
    assert to_uuid(a) != to_uuid(d)

    a = to_creator(T0)
    b = to_creator("T0")
    assert a is not b
    assert to_uuid(a) == to_uuid(b)


class T0(Creator):
    def create(self) -> DataFrame:
        return None


@creator("")
def t1() -> DataFrame:
    pass


@creator("a:int")
def t2(e: ExecutionEngine, a, b) -> List[List[Any]]:
    pass


def t3(a) -> DataFrame:
    return ArrayDataFrame([[a]], "a:int")


def t4(e: ExecutionEngine, a: int, b: str) -> DataFrame:
    pass


@creator("a:int")
def t5(e: ExecutionEngine, a) -> List[List[Any]]:
    assert e is not None
    return ArrayDataFrame([[a]], "a:int").as_array()


def t6() -> DataFrame:
    pass


def t7() -> List[List[Any]]:
    pass


# schema: a:int
def t8(e: ExecutionEngine, a, b) -> List[List[Any]]:
    pass
