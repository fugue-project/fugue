from typing import Any, Dict, Iterable, List

import pandas as pd
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.execution import ExecutionEngine
from fugue._utils.interfaceless import FunctionWrapper, _parse_function, parse_output_schema_from_comment
from pytest import raises
from triad.utils.iter import EmptyAwareIterable
from triad.utils.hash import to_uuid


def test_parse_output_schema_from_comment():
    def a():
        pass

    # asdfasdf
    def b():
        pass

    # asdfasdf
    # schema : s:int
    # # # schema : a :  int,b:str
    # asdfasdf
    def c():
        pass

    # schema:
    def d():
        pass

    assert parse_output_schema_from_comment(a) is None
    assert parse_output_schema_from_comment(b) is None
    assert "a:int,b:str" == parse_output_schema_from_comment(c)
    assert parse_output_schema_from_comment(d) is None


def test_parse_function():
    _parse_function(f1, "^edlp$", "n")
    _parse_function(f2, "^xxxx$", "n")
    _parse_function(f3, "^ss$", "l")
    _parse_function(f4, "^ss$", "d")
    _parse_function(f5, "^ss$", "n")
    _parse_function(f6, "^x$", "n")
    raises(TypeError, lambda: _parse_function(f6, "^xx$", "n"))
    raises(TypeError, lambda: _parse_function(f6, "^x$", "x"))
    raises(TypeError, lambda: _parse_function(f7, "^s$", "n"))
    _parse_function(f8, "^syz$", "n")
    _parse_function(f9, "^syz$", "n")
    _parse_function(f10, "^ss$", "n")
    _parse_function(f11, "^$", "s")
    _parse_function(f12, "^$", "s")
    _parse_function(f13, "^e?(c|[dl]+)x*$", "n")
    _parse_function(f14, "^e?(c|[dl]+)x*$", "n")
    raises(TypeError, lambda: _parse_function(f15, "^e?(c|[dl]+)x*$", "n"))


def test_function_wrapper():
    for f in [f20, f21, f22, f23, f24, f25, f26, f30, f31, f32]:
        df = ArrayDataFrame([[0]], "a:int")
        w = FunctionWrapper(f, "^[ldsp][ldsp]$", "[ldsp]")
        res = w.run([df], dict(a=df), ignore_unknown=False, output_schema="a:int")
        df_eq(res, [[0], [0]], "a:int", throw=True)

    # test other data types, simple operations
    w = FunctionWrapper(f27)
    assert 3 == w(1, 2)
    assert 3 == w.run([1, 2], dict(), ignore_unknown=False)
    assert 3 == w.run([5], dict(a=1, b=2), ignore_unknown=True)  # dict will overwrite
    assert 3 == w.run([], dict(a=1, b=2, c=4), ignore_unknown=True)
    raises(ValueError, lambda: w.run([], dict(a=1, b=2, c=4), ignore_unknown=False))

    # test default and required
    w = FunctionWrapper(f28)
    assert 3 == w.run([], dict(a=1, b=2), ignore_unknown=False)
    assert 2 == w.run([], dict(a=1), ignore_unknown=False)
    assert 3 == w.run([], dict(a=1, b=2), ignore_unknown=True)
    assert 3 == w.run([], dict(a=1, b=2, c=4), ignore_unknown=True)
    raises(ValueError, lambda: w.run([], dict(a=1, b=2, c=4), ignore_unknown=False))
    raises(ValueError, lambda: w.run([], dict(b=2), ignore_unknown=True))

    # test kwargs
    w = FunctionWrapper(f29)
    assert 3 == w.run([], dict(a=1, b=2), ignore_unknown=False)
    assert 1 == w.run([], dict(a=1), ignore_unknown=False)
    assert 3 == w.run([], dict(a=1, b=2), ignore_unknown=True)
    assert 7 == w.run([], dict(a=1, b=2, c=4), ignore_unknown=True)
    assert 7 == w.run([], dict(a=1, b=2, c=4), ignore_unknown=False)


def test_function_wrapper_determinisn():
    w1 = FunctionWrapper(f20, "^[ldsp][ldsp]$", "[ldsp]")
    w2 = FunctionWrapper(f20, "^[ldsp][ldsp]$", "[ldsp]")
    assert w1 is not w2
    assert to_uuid(w1) == to_uuid(w2)


def f1(e: ExecutionEngine, a: DataFrame, b: LocalDataFrame, c: pd.DataFrame) -> None:
    pass


def f2(e: int, a, b: int, c):
    pass


def f3(e: List[List[Any]], a: Iterable[List[Any]]) -> LocalDataFrame:
    pass


def f4(e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]) -> DataFrame:
    pass


def f5(e: "List[Dict[str, Any]]", a: "Iterable[Dict[str, Any]]") -> "None":
    pass


def f6(e: List[Dict[str, str]]):
    pass


def f7(e: List[Dict[str, Any]] = []):
    pass


def f8(e: List[Dict[str, Any]], *k, **a):
    pass


def f9(e: List[Dict[str, Any]], *k: List[Any], **a: Dict[str, Any]):
    pass


def f10(e: EmptyAwareIterable[List[Any]], a: EmptyAwareIterable[Dict[str, Any]]):
    pass


def f11() -> EmptyAwareIterable[List[Any]]:
    pass


def f12() -> EmptyAwareIterable[Dict[str, Any]]:
    pass


def f13(e: ExecutionEngine, dfs: DataFrames, a, b) -> None:
    pass


def f14(e: ExecutionEngine, df1: DataFrame, df2: LocalDataFrame, a, b) -> None:
    pass


def f15(e: ExecutionEngine, dfs1: DataFrames, dfs2: DataFrames, a, b) -> None:
    pass


def f20(e: List[List[Any]], a: Iterable[List[Any]]) -> LocalDataFrame:
    e += list(a)
    return ArrayDataFrame(e, "a:int")


def f21(e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]) -> DataFrame:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return ArrayDataFrame(arr, "a:int")


def f22(e: List[List[Any]], a: Iterable[List[Any]]) -> List[List[Any]]:
    e += list(a)
    return ArrayDataFrame(e, "a:int").as_array()


def f23(e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]) -> Iterable[List[Any]]:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return ArrayDataFrame(arr, "a:int").as_array_iterable()


def f24(e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return ArrayDataFrame(arr, "a:int").as_pandas()


def f25(e: DataFrame, a: LocalDataFrame) -> List[Dict[str, Any]]:
    e = e.as_array()
    e += list(a.as_array())
    return list(ArrayDataFrame(e, "a:int").as_dict_iterable())


def f26(e: pd.DataFrame, a: LocalDataFrame) -> Iterable[Dict[str, Any]]:
    e = list(PandasDataFrame(e).as_array())
    e += list(a.as_array())
    return ArrayDataFrame(e, "a:int").as_dict_iterable()


def f27(a, b):
    return a + b


def f28(a, b=1):
    return a + b


def f29(a, **args):
    for v in args.values():
        a += v
    return a


def f30(e: EmptyAwareIterable[List[Any]], a: EmptyAwareIterable[Dict[str, Any]]) -> LocalDataFrame:
    e.peek()
    a.peek()
    e = list(e)
    e += [[x["a"]] for x in a]
    return ArrayDataFrame(e, "a:int")


def f31(e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]) -> EmptyAwareIterable[List[Any]]:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return ArrayDataFrame(arr, "a:int").as_array_iterable()


def f32(e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]) -> EmptyAwareIterable[Dict[str, Any]]:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return ArrayDataFrame(arr, "a:int").as_dict_iterable()
