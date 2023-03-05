from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from pytest import raises
from triad.utils.iter import EmptyAwareIterable

from fugue import (
    ArrayDataFrame,
    DataFrame,
    DataFrames,
    ExecutionEngine,
    LocalDataFrame,
    NativeExecutionEngine,
    PandasDataFrame,
)
from fugue.collections.function_wrapper import (
    FunctionWrapper,
    _CallableParam,
    _NoneParam,
    _OptionalCallableParam,
    _OtherParam,
    annotated_param,
    parse_annotation,
)
from fugue.exceptions import FuguePluginsRegistrationError


class _Dummy:
    pass


def test_registration():
    with raises(FuguePluginsRegistrationError):

        @annotated_param(_Dummy, ".")
        class _D1:  # not a subclass of AnnotatedParam
            pass

    with raises(FuguePluginsRegistrationError):

        @annotated_param(_Dummy)  # _NoneParam doesn't allow child to reuse the code
        class _D2(_NoneParam):
            pass


def test_parse_annotation():
    p = parse_annotation(None)
    assert p.code == "x"
    assert isinstance(p, _OtherParam)
    p = parse_annotation(type(None))
    assert p.code == "x"
    assert isinstance(p, _OtherParam)
    p = parse_annotation(type(None), none_as_other=False)
    assert p.code == "n"
    assert isinstance(p, _NoneParam)

    p = parse_annotation(callable, none_as_other=False)
    assert isinstance(p, _CallableParam)
    p = parse_annotation(Callable, none_as_other=False)
    assert isinstance(p, _CallableParam)

    p = parse_annotation(Optional[callable], none_as_other=False)
    assert isinstance(p, _OptionalCallableParam)
    p = parse_annotation(Optional[Callable], none_as_other=False)
    assert isinstance(p, _OptionalCallableParam)

    p = parse_annotation(DataFrame)
    assert p.code == "d"

    p = parse_annotation(ExecutionEngine)
    assert p.code == "e"

    p = parse_annotation(NativeExecutionEngine)
    assert p.code == "e"


def test_parse_function():
    def _parse_function(f, params_re, return_re):
        FunctionWrapper(f, params_re, return_re)

    _parse_function(f1, "^edlpl$", "n")
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
    _parse_function(f14, "^0?e?(c|[dl]+)x*$", "n")
    _parse_function(f16, "^0e?(c|[dl]+)x*$", "n")
    _parse_function(f33, "^$", "q")
    raises(TypeError, lambda: _parse_function(f34, "^[sq]$", "q"))
    _parse_function(f36, "^FFfff+$", "F")


def f1(
    e: ExecutionEngine, a: DataFrame, b: LocalDataFrame, c: pd.DataFrame, d: pa.Table
) -> None:
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


def f16(self, e: ExecutionEngine, df1: DataFrame, df2: LocalDataFrame, a, b) -> None:
    pass


def f33() -> Iterable[pd.DataFrame]:
    pass


def f34(e: Iterable[pd.DataFrame]):
    pass


def f35(e: pd.DataFrame, a: LocalDataFrame) -> Iterable[pd.DataFrame]:
    e = PandasDataFrame(e, "a:int").as_pandas()
    a = ArrayDataFrame(a, "a:int").as_pandas()
    return iter([e, a])


def f36(
    a1: Callable,
    b1: Callable[[str], str],
    c1: Optional[Callable],
    d1: Optional[Callable[[str], str]],
    e1: Optional[callable],
) -> callable:
    pass
