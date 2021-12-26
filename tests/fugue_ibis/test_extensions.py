import pytest

ibis = pytest.importorskip("ibis")
from fugue import FugueWorkflow, NativeExecutionEngine

from fugue_ibis import as_fugue, as_ibis, run_ibis
from fugue_ibis.execution.ibis_engine import to_ibis_engine
from fugue_ibis.execution.pandas_backend import PandasIbisEngine
from pytest import raises


def test_to_ibis_engine():
    e = NativeExecutionEngine()
    ie = PandasIbisEngine(e)
    assert isinstance(to_ibis_engine(e, None), PandasIbisEngine)
    assert isinstance(to_ibis_engine(e, ie), PandasIbisEngine)
    with raises(NotImplementedError):
        to_ibis_engine(e, "dummy")


def test_run_ibis():
    def _test1(con: ibis.BaseBackend) -> ibis.Expr:
        tb = con.table("a")
        return tb

    def _test2(con: ibis.BaseBackend) -> ibis.Expr:
        tb = con.table("a")
        return tb.mutate(c=tb.a + tb.b)

    with FugueWorkflow() as dag:
        df = dag.df([[0, 1], [2, 3]], "a:long,b:long")
        res = run_ibis(_test1, a=df)
        res.assert_eq(df)
        df = dag.df([[0, 1], [2, 3]], "a:long,b:long")
        res = run_ibis(_test2, a=df)
        df2 = dag.df([[0, 1, 1], [2, 3, 5]], "a:long,b:long,c:long")
        res.assert_eq(df2)


def test_run_as_ibis():
    with FugueWorkflow() as dag:
        df = dag.df([[0, 1], [2, 3]], "a:long,b:long")
        idf = as_ibis(df)
        res = as_fugue(idf)
        res.assert_eq(df)

    with FugueWorkflow() as dag:
        df1 = dag.df([[0, 1], [2, 3]], "a:long,b:long")
        df2 = dag.df([[0, ["x"]], [3, ["y"]]], "a:long,c:[str]")
        idf1 = as_ibis(df1)
        idf2 = as_ibis(df2)
        idf = idf1.inner_join(idf2, idf1.a == idf2.a)[idf1, idf2.c]
        res = as_fugue(idf)
        expected = dag.df([[0, 1, ["x"]]], "a:long,b:long,c:[str]")
        res.assert_eq(expected, check_order=True, check_schema=True)

    with FugueWorkflow() as dag:
        idf1 = dag.df([[0, 1], [2, 3]], "a:long,b:long").as_ibis()
        idf2 = dag.df([[0, ["x"]], [3, ["y"]]], "a:long,c:[str]").as_ibis()
        res = idf1.inner_join(idf2, idf1.a == idf2.a)[idf1, idf2.c].as_fugue()
        expected = dag.df([[0, 1, ["x"]]], "a:long,b:long,c:[str]")
        res.assert_eq(expected, check_order=True, check_schema=True)
