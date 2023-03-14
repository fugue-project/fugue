import pickle

import duckdb
import pandas as pd
import pyarrow as pa
from pytest import raises

import fugue.api as fa
from fugue import ArrowDataFrame, DataFrame, FugueWorkflow, fsql
from fugue.api import engine_context
from fugue.plugins import infer_execution_engine
from fugue_duckdb import DuckExecutionEngine
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests


class DuckExecutionEngineTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)
        fa.set_global_engine(cls._engine)

    @classmethod
    def tearDownClass(cls):
        fa.clear_global_engine()
        cls._con.close()

    def make_engine(self):
        e = DuckExecutionEngine(
            {
                "test": True,
                "fugue.duckdb.pragma.threads": 2,
                "fugue.duckdb.extensions": ", json , , httpfs",
            },
            self._con,
        )
        return e

    def test_properties(self):
        assert not self.engine.is_distributed
        assert not self.engine.map_engine.is_distributed
        assert not self.engine.sql_engine.is_distributed

    def test_duckdb_extensions(self):
        df = fa.fugue_sql(
            """
        SELECT COUNT(*) AS ct FROM duckdb_extensions()
        WHERE loaded AND extension_name IN ('httpfs', 'json')
        """,
            as_fugue=True,
        )
        assert 2 == df.as_array()[0][0]

    def test_duck_to_df(self):
        e = self.engine
        a = e.to_df([[1, 2, 3]], "a:double,b:double,c:int")
        assert isinstance(a, DuckDataFrame)
        b = e.to_df(a.native_as_df())
        assert isinstance(b, DuckDataFrame)

    def test_table_operations(self):
        se = self.engine.sql_engine
        df = fa.as_fugue_df([[0, 1]], schema="a:int,b:long")
        assert se._get_table("_t_x") is None
        se.save_table(df, "_t_x")
        assert se._get_table("_t_x") is not None
        res = se.load_table("_t_x").as_array()
        assert [[0, 1]] == res
        df = fa.as_fugue_df([[1, 2]], schema="a:int,b:long")
        se.save_table(df, "_t_x")
        res = se.load_table("_t_x").as_array()
        assert [[1, 2]] == res
        with raises(Exception):
            se.save_table(df, "_t_x", mode="error")

    def test_intersect_all(self):
        e = self.engine
        a = e.to_df([[1, 2, 3], [4, None, 6], [4, None, 6]], "a:double,b:double,c:int")
        b = e.to_df(
            [[1, 2, 33], [4, None, 6], [4, None, 6], [4, None, 6]],
            "a:double,b:double,c:int",
        )
        raises(NotImplementedError, lambda: e.intersect(a, b, distinct=False))
        # DuckDB 0.5.0 stopped support INTERSECT ALL
        # c = e.intersect(a, b, distinct=False)
        # df_eq(
        #     c,
        #     [[4, None, 6], [4, None, 6]],
        #     "a:double,b:double,c:int",
        #     throw=True,
        # )


class DuckBuiltInTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()

    def make_engine(self):
        e = DuckExecutionEngine(
            {"test": True, "fugue.duckdb.pragma.threads": 2}, self._con
        )
        return e

    def test_special_types(self):
        def assert_data(df: DataFrame) -> None:
            assert df.schema == "a:datetime,b:bytes,c:[long]"

        df = pd.DataFrame(
            [["2020-01-01", pickle.dumps([1, 2]), [1, 2]]], columns=list("abc")
        )
        df["a"] = pd.to_datetime(df.a)

        with FugueWorkflow() as dag:
            x = dag.df(df)
            result = dag.select("SELECT * FROM ", x)
            result.output(assert_data)
        dag.run(self.engine)


def test_builtin_connection():
    dag = FugueWorkflow()
    df = dag.df([[0], [1]], "a:long")
    df = dag.select("SELECT * FROM", df, "WHERE a<1")
    df.assert_eq(dag.df([[0]], "a:long"))

    dag.run("duckdb")
    dag.run(("native", "duck"))


def test_configs():
    dag = FugueWorkflow()
    df = dag.df([[None], [1]], "a:double")
    df = dag.select("SELECT * FROM ", df, "ORDER BY a LIMIT 1")
    df.assert_eq(dag.df([[None]], "a:double"))

    dag.run(
        "duckdb",
        {
            "fugue.duckdb.pragma.threads": 2,
            "fugue.duckdb.pragma.default_null_order": "NULLS FIRST",
        },
    )

    dag = FugueWorkflow()
    df = dag.df([[None], [1]], "a:double")
    df = dag.select("SELECT * FROM ", df, "ORDER BY a LIMIT 1")
    df.assert_eq(dag.df([[1]], "a:double"))

    dag.run(
        "duckdb",
        {
            "fugue.duckdb.pragma.threads": 2,
            "fugue.duckdb.pragma.default_null_order": "NULLS LAST",
        },
    )

    with raises(ValueError):
        # invalid config format
        dag.run("duckdb", {"fugue.duckdb.pragma.threads;xx": 2})

    with raises(Exception):
        # non-existent config
        dag.run("duckdb", {"fugue.duckdb.pragma.threads_xx": 2})


def test_annotations():
    con = duckdb.connect()

    def cr(c: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        return c.query("SELECT 1 AS a")

    def pr(df: duckdb.DuckDBPyRelation) -> pa.Table:
        return df.arrow()

    def ot(df: duckdb.DuckDBPyRelation) -> None:
        assert 1 == df.df().shape[0]

    dag = FugueWorkflow()
    dag.create(cr).process(pr).output(ot)

    dag.run(con)


def test_output_types():
    pdf = pd.DataFrame([[0]], columns=["a"])
    con = duckdb.connect()

    res = fsql(
        """
    CREATE [[0]] SCHEMA a:int
    YIELD DATAFRAME AS a
    CREATE [[0]] SCHEMA b:int
    YIELD LOCAL DATAFRAME AS b
    """
    ).run(con)

    assert isinstance(res["a"], DuckDataFrame)
    assert isinstance(res["b"], ArrowDataFrame)

    x = fa.union(pdf, pdf, engine=con)
    assert isinstance(x, duckdb.DuckDBPyRelation)

    con.close()

    # ephemeral
    res = fsql(
        """
    CREATE [[0]] SCHEMA a:int
    YIELD DATAFRAME AS a
    CREATE [[0]] SCHEMA b:int
    YIELD LOCAL DATAFRAME AS b
    """
    ).run("duck")

    assert isinstance(res["a"], ArrowDataFrame)
    assert isinstance(res["b"], ArrowDataFrame)

    x = fa.union(pdf, pdf, engine="duckdb")
    assert isinstance(x, pa.Table)

    # in context
    with engine_context("duck"):
        res = fsql(
            """
        CREATE [[0]] SCHEMA a:int
        YIELD DATAFRAME AS a
        CREATE [[0]] SCHEMA b:int
        YIELD LOCAL DATAFRAME AS b
        """
        ).run()

        assert isinstance(res["a"], DuckDataFrame)
        assert isinstance(res["b"], ArrowDataFrame)

        x = fa.union(pdf, pdf)
        assert isinstance(x, duckdb.DuckDBPyRelation)


def test_infer_engine():
    con = duckdb.connect()
    df = con.from_df(pd.DataFrame([[0]], columns=["a"]))
    assert infer_execution_engine([df]) == "duckdb"

    fdf = DuckDataFrame(df)
    assert infer_execution_engine([fdf]) == "duckdb"

    con.close()
