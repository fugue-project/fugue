import pytest

ibis = pytest.importorskip("ibis")
import duckdb
from fugue import FugueWorkflow, NativeExecutionEngine
from fugue_ibis import IbisEngine, run_ibis
from fugue_test.ibis_suite import IbisTests

from fugue_duckdb import DuckDBIbisEngine, DuckExecutionEngine


class DuckDBIbisTests(IbisTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)
        cls._ibis_engine = cls.make_ibis_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()

    def make_engine(self):
        e = DuckExecutionEngine(dict(test=True), self._con)
        return e

    def make_ibis_engine(self) -> IbisEngine:
        return DuckDBIbisEngine(self._engine)

    def test_run_ibis_duck(self):
        def _test1(con: ibis.BaseBackend) -> ibis.Expr:
            tb = con.table("a")
            return tb

        def _test2(con: ibis.BaseBackend) -> ibis.Expr:
            tb = con.table("a")
            return tb.mutate(c=tb.a + tb.b)

        dag = FugueWorkflow()
        df = dag.df([[0, 1], [2, 3]], "a:long,b:long")
        res = run_ibis(_test1, ibis_engine="duck", a=df)
        res.assert_eq(df)
        df = dag.df([[0, 1], [2, 3]], "a:long,b:long")
        res = run_ibis(_test2, ibis_engine="duckdb", a=df)
        df2 = dag.df([[0, 1, 1], [2, 3, 5]], "a:long,b:long,c:long")
        res.assert_eq(df2)
        dag.run(self.engine)
        dag.run(NativeExecutionEngine())
