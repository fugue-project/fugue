import ibis
import pandas as pd
from fugue_ibis._lazy import LazyIbisObject, materialize
from fugue.dataframe.utils import _df_eq
from fugue import PandasDataFrame


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

    lkwargs = {k: LazyIbisObject() for k in dfs.keys()}
    context = {id(lkwargs[k]): kwargs[k] for k in dfs.keys()}
    expr = func(**lkwargs)
    actual = materialize(expr, context).execute()

    _df_eq(PandasDataFrame(actual), PandasDataFrame(expected), throw=True)