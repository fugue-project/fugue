import pytest

ibis = pytest.importorskip("ibis")
import pandas as pd
from fugue import PandasDataFrame
from fugue.dataframe.utils import _df_eq
from triad import Schema

from fugue_ibis._utils import LazyIbisObject, materialize, to_ibis_schema, to_schema


def test_schema():
    a = Schema(
        "a:bool,b:int8,c:uint8,d:int16,e:uint16,f:int32,g:uint32,h:int64,i:uint64"
    )
    b = ibis.schema(
        [
            ("a", "boolean"),
            ("b", "int8"),
            ("c", "uint8"),
            ("d", "int16"),
            ("e", "uint16"),
            ("f", "int32"),
            ("g", "uint32"),
            ("h", "int64"),
            ("i", "uint64"),
        ]
    )
    assert to_ibis_schema(a) == b
    assert a == to_schema(b)

    a = Schema("a:float32,b:float64,c:datetime,d:date,e:binary,f:string")
    b = ibis.schema(
        [
            ("a", "float32"),
            ("b", "float64"),
            ("c", "timestamp"),
            ("d", "date"),
            ("e", "binary"),
            ("f", "string"),
        ]
    )
    assert to_ibis_schema(a) == b
    assert a == to_schema(b)

    a = Schema("a:[int],b:[{a:str}],c:{a:str},d:{a:[int]}")
    assert to_schema(to_ibis_schema(a)) == a


def test_materialize():
    tdf1 = pd.DataFrame([[0, 1], [3, 4]], columns=["a", "b"])

    _test_expr(lambda a: a, a=tdf1)
    _test_expr(lambda a: a[a.b], a=tdf1)
    _test_expr(lambda a: a[a.a >= 1], a=tdf1)
    _test_expr(lambda a: a[a.a.isin([0, 2])], a=tdf1)
    _test_expr(lambda a: a[a.b, a.a], a=tdf1)
    _test_expr(lambda a: a.b + ibis.literal(1, "int8"), a=tdf1)
    _test_expr(lambda a: 1 + a.b, a=tdf1)
    _test_expr(lambda a: a[(1 * a.a).name("x"), (1 / a.b).name("y")], a=tdf1)
    _test_expr(lambda a: a.group_by("a").aggregate(a.b.sum()), a=tdf1)

    tdf2 = pd.DataFrame([[0, "x"], [4, "y"]], columns=["a", "c"])
    _test_expr(lambda t, t2: t.join(t2, t.a == t2.a)[t, t2.c], t=tdf1, t2=tdf2)

    tdf3 = pd.DataFrame([[0, 1, 2], [3, 4, 5]], columns=["a", "b", "c"])
    _test_expr(
        lambda t: (
            t.group_by(["a", "b"])
            .aggregate(the_sum=t.c.sum())
            .group_by("a")
            .aggregate(mad=lambda x: x.the_sum.abs().mean())
        ),
        t=tdf3,
    )


def _test_expr(func, **dfs):
    con = ibis.pandas.connect(dfs)
    kwargs = {k: con.table(k) for k in dfs.keys()}
    expected = func(**kwargs).execute()

    lkwargs = {k: LazyIbisObject(k) for k in dfs.keys()}
    expr = func(**lkwargs)
    actual = materialize(expr, lambda k: kwargs[k]).execute()

    _df_eq(PandasDataFrame(actual), PandasDataFrame(expected), throw=True)
