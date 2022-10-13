# pylint: disable-all
from unittest import TestCase

import ibis
from fugue import ExecutionEngine, FugueWorkflow, register_default_sql_engine
from fugue_ibis import IbisEngine, as_fugue, as_ibis, run_ibis


class IbisTests(object):
    """Ibis test suite.
    Any new engine from :class:`~fugue_ibis.execution.ibis_engine.IbisEngine`
    should also pass this test suite.
    """

    class Tests(TestCase):
        @classmethod
        def setUpClass(cls):
            register_default_sql_engine(lambda engine: engine.sql_engine)
            cls._engine = cls.make_engine(cls)
            cls._ibis_engine = cls.make_ibis_engine(cls)

        @property
        def engine(self) -> ExecutionEngine:
            return self._engine  # type: ignore

        @property
        def ibis_engine(self) -> ExecutionEngine:
            return self._ibis_engine  # type: ignore

        @classmethod
        def tearDownClass(cls):
            cls._engine.stop()

        def make_engine(self) -> ExecutionEngine:  # pragma: no cover
            raise NotImplementedError

        def make_ibis_engine(self) -> IbisEngine:  # pragma: no cover
            raise NotImplementedError

        def test_run_ibis(self):
            def _test1(con: ibis.BaseBackend) -> ibis.Expr:
                tb = con.table("a")
                return tb

            def _test2(con: ibis.BaseBackend) -> ibis.Expr:
                tb = con.table("a")
                return tb.mutate(c=tb.a + tb.b)

            dag = FugueWorkflow()
            df = dag.df([[0, 1], [2, 3]], "a:long,b:long")
            res = run_ibis(_test1, ibis_engine=self.ibis_engine, a=df)
            res.assert_eq(df)
            df = dag.df([[0, 1], [2, 3]], "a:long,b:long")
            res = run_ibis(_test2, ibis_engine=self.ibis_engine, a=df)
            df2 = dag.df([[0, 1, 1], [2, 3, 5]], "a:long,b:long,c:long")
            res.assert_eq(df2)
            dag.run(self.engine)

        def test_run_as_ibis(self):
            dag = FugueWorkflow()
            df = dag.df([[0, 1], [2, 3]], "a:long,b:long")
            idf = as_ibis(df)
            res = as_fugue(idf)
            res.assert_eq(df)
            dag.run(self.engine)

            dag = FugueWorkflow()
            df1 = dag.df([[0, 1], [2, 3]], "a:long,b:long")
            df2 = dag.df([[0, ["x"]], [3, ["y"]]], "a:long,c:[str]")
            idf1 = as_ibis(df1)
            idf2 = as_ibis(df2)
            idf = idf1.inner_join(idf2, idf1.a == idf2.a)[idf1, idf2.c]
            res = as_fugue(idf)
            expected = dag.df([[0, 1, ["x"]]], "a:long,b:long,c:[str]")
            res.assert_eq(expected, check_order=True, check_schema=True)
            dag.run(self.engine)

            dag = FugueWorkflow()
            idf1 = dag.df([[0, 1], [2, 3]], "a:long,b:long").as_ibis()
            idf2 = dag.df([[0, ["x"]], [3, ["y"]]], "a:long,c:[str]").as_ibis()
            res = idf1.inner_join(idf2, idf1.a == idf2.a)[idf1, idf2.c].as_fugue()
            expected = dag.df([[0, 1, ["x"]]], "a:long,b:long,c:[str]")
            res.assert_eq(expected, check_order=True, check_schema=True)
            dag.run(self.engine)

        def test_literal(self):
            dag = FugueWorkflow()
            idf1 = dag.df([[0, 1], [2, 3]], "a:long,b:long").as_ibis()
            res = idf1.mutate(c=idf1.b + 10).as_fugue()
            expected = dag.df([[0, 1, 11], [2, 3, 13]], "a:long,b:long,c:long")
            res.assert_eq(expected, check_order=True, check_schema=True)
            dag.run(self.engine)
