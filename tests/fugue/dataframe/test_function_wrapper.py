import copy
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

import pandas as pd
import pyarrow as pa
from pytest import raises
from triad import to_uuid
from triad.utils.iter import EmptyAwareIterable

from fugue import (
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    IterableDataFrame,
    LocalDataFrame,
    IterablePandasDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.function_wrapper import _IterablePandasParam, _IterableArrowParam
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.dev import DataFrameFunctionWrapper


def test_function_wrapper():
    for f in [f20, f21, f212, f22, f23, f24, f25, f26, f30, f31, f32, f35, f36]:
        df = ArrayDataFrame([[0]], "a:int")
        w = DataFrameFunctionWrapper(f, "^[ldsp][ldsp]$", "[ldspq]")
        res = w.run([df], dict(a=df), ignore_unknown=False, output_schema="a:int")
        df_eq(res, [[0], [0]], "a:int", throw=True)
        w.run([df], dict(a=df), ignore_unknown=False, output=False)

    # test other data types, simple operations
    w = DataFrameFunctionWrapper(f27)
    assert w.get_format_hint() is None
    assert 3 == w(1, 2)
    assert 3 == w.run([1, 2], dict(), ignore_unknown=False)
    assert 3 == w.run([5], dict(a=1, b=2), ignore_unknown=True)  # dict will overwrite
    assert 3 == w.run([], dict(a=1, b=2, c=4), ignore_unknown=True)
    raises(ValueError, lambda: w.run([], dict(a=1, b=2, c=4), ignore_unknown=False))

    # test default and required
    w = DataFrameFunctionWrapper(f28)
    assert 3 == w.run([], dict(a=1, b=2), ignore_unknown=False)
    assert 2 == w.run([], dict(a=1), ignore_unknown=False)
    assert 3 == w.run([], dict(a=1, b=2), ignore_unknown=True)
    assert 3 == w.run([], dict(a=1, b=2, c=4), ignore_unknown=True)
    raises(ValueError, lambda: w.run([], dict(a=1, b=2, c=4), ignore_unknown=False))
    raises(ValueError, lambda: w.run([], dict(b=2), ignore_unknown=True))

    # test kwargs
    w = DataFrameFunctionWrapper(f29)
    assert 3 == w.run([], dict(a=1, b=2), ignore_unknown=False)
    assert 1 == w.run([], dict(a=1), ignore_unknown=False)
    assert 3 == w.run([], dict(a=1, b=2), ignore_unknown=True)
    assert 7 == w.run([], dict(a=1, b=2, c=4), ignore_unknown=True)
    assert 7 == w.run([], dict(a=1, b=2, c=4), ignore_unknown=False)

    # test method inside class
    class Test(object):
        def t(self, a=1, b=2) -> int:
            return a + b

    test = Test()
    # instance method test
    w = DataFrameFunctionWrapper(test.t, "^0?.*", ".*")
    assert 4 == w.run([], kwargs={"b": 3}, ignore_unknown=True)
    assert 5 == w.run([2], kwargs={"b": 3}, ignore_unknown=True)

    # format hint
    w = DataFrameFunctionWrapper(f10)
    assert w.get_format_hint() == "pyarrow"

    w = DataFrameFunctionWrapper(f11)
    assert w.get_format_hint() == "pandas"

    w = DataFrameFunctionWrapper(f12)
    assert w.get_format_hint() == "pyarrow"

    w = DataFrameFunctionWrapper(f13)
    assert w.get_format_hint() == "pandas"

    w = DataFrameFunctionWrapper(f14)
    assert w.get_format_hint() == "pandas"


def test_function_wrapper_determinism():
    w1 = DataFrameFunctionWrapper(f20, "^[ldsp][ldsp]$", "[ldsp]")
    w2 = DataFrameFunctionWrapper(f20, "^[ldsp][ldsp]$", "[ldsp]")
    assert w1 is not w2
    assert to_uuid(w1) == to_uuid(w2)


def test_function_wrapper_copy():
    class Test(object):
        def __init__(self):
            self.n = 0

        def t(self) -> None:
            self.n += 1

    test = Test()
    w1 = DataFrameFunctionWrapper(test.t, "", "n")
    w2 = copy.copy(w1)
    w3 = copy.deepcopy(w1)
    w1.run([], {}, output=False)
    w2.run([], {}, output=False)
    w3.run([], {}, output=False)
    assert 3 == test.n


def test_iterable_pandas_dataframes():
    p = _IterablePandasParam(None)
    pdf = pd.DataFrame([[0]], columns=["a"])
    df = PandasDataFrame(pdf)
    data = list(p.to_input_data(df, ctx=None))
    assert 1 == len(data)
    assert data[0] is pdf  # this is to guarantee no copy in any wrapping logic
    assert data[0].values.tolist() == [[0]]

    dfs = IterablePandasDataFrame([df, df])
    data = list(p.to_input_data(dfs, ctx=None))
    assert 2 == len(data)
    assert data[0] is pdf
    assert data[1] is pdf

    def get_pdfs():
        yield pdf
        yield pdf

    # without schema change, there is no copy
    odf = p.to_output_df(get_pdfs(), df.schema, ctx=None)
    data = list(odf.native)
    assert 2 == len(data)
    assert data[0].native is pdf
    assert data[1].native is pdf

    # with schema change, there is copy
    odf = p.to_output_df(get_pdfs(), "a:double", ctx=None)
    data = list(odf.native)
    assert 2 == len(data)
    assert data[0].native is not pdf
    assert data[1].native is not pdf


def test_iterable_arrow_dataframes():
    p = _IterableArrowParam(None)
    pdf = pa.Table.from_pandas(pd.DataFrame([[0]], columns=["a"]))
    df = ArrowDataFrame(pdf)
    data = list(p.to_input_data(df, ctx=None))
    assert 1 == len(data)
    assert data[0] is pdf  # this is to guarantee no copy in any wrapping logic

    dfs = IterablePandasDataFrame([df, df])
    data = list(p.to_input_data(dfs, ctx=None))
    assert 2 == len(data)
    assert data[0] is pdf
    assert data[1] is pdf

    def get_pdfs():
        yield pdf
        yield pdf

    # without schema change, there is no copy
    odf = p.to_output_df(get_pdfs(), df.schema, ctx=None)
    data = list(odf.native)
    assert 2 == len(data)
    assert data[0].native is pdf
    assert data[1].native is pdf

    # with schema change, there is copy
    odf = p.to_output_df(get_pdfs(), "a:double", ctx=None)
    data = list(odf.native)
    assert 2 == len(data)
    assert data[0].native is not pdf
    assert data[1].native is not pdf


def f10(x: Any, y: pa.Table) -> None:
    pass


def f11(x: Any, y: pd.DataFrame, z: pa.Table) -> pa.Table:
    pass


def f12(x: Any, y) -> pa.Table:
    pass


def f13(x: Any, y: Iterable[pd.DataFrame]) -> pa.Table:
    pass


def f14() -> Iterator[pd.DataFrame]:
    pass


def f20(e: List[List[Any]], a: Iterable[List[Any]]) -> LocalDataFrame:
    e += list(a)
    return IterableDataFrame(e, "a:int")


def f21(e: List[Dict[str, Any]], a: Iterator[Dict[str, Any]]) -> DataFrame:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return IterableDataFrame(arr, "a:int")


def f212(e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]) -> DataFrame:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return ArrayDataFrame(arr, "a:int")


def f22(e: List[List[Any]], a: Iterator[List[Any]]) -> List[List[Any]]:
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


def f30(
    e: EmptyAwareIterable[List[Any]], a: EmptyAwareIterable[Dict[str, Any]]
) -> LocalDataFrame:
    e.peek()
    a.peek()
    e = list(e)
    e += [[x["a"]] for x in a]
    return ArrayDataFrame(e, "a:int")


def f31(
    e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]
) -> EmptyAwareIterable[List[Any]]:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return ArrayDataFrame(arr, "a:int").as_array_iterable()


def f32(
    e: List[Dict[str, Any]], a: Iterable[Dict[str, Any]]
) -> EmptyAwareIterable[Dict[str, Any]]:
    e += list(a)
    arr = [[x["a"]] for x in e]
    return ArrayDataFrame(arr, "a:int").as_dict_iterable()


def f35(e: pd.DataFrame, a: LocalDataFrame) -> Iterable[pd.DataFrame]:
    e = PandasDataFrame(e, "a:int").as_pandas()
    a = ArrayDataFrame(a, "a:int").as_pandas()
    return iter([e, a])


def f36(e: pd.DataFrame, a: LocalDataFrame) -> Iterable[pa.Table]:
    e = PandasDataFrame(e, "a:int").as_arrow()
    a = ArrayDataFrame(a, "a:int").as_arrow()
    return iter([e, a])
