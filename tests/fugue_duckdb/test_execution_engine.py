import pickle

import duckdb
import pandas as pd
import pyarrow as pa
from fugue import DataFrame, FugueWorkflow, ArrowDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_duckdb import DuckExecutionEngine
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_sql import fsql
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from pytest import raises


class DuckExecutionEngineTests(ExecutionEngineTests.Tests):
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

    def test_intersect_all(self):
        e = self.engine
        a = e.to_df([[1, 2, 3], [4, None, 6], [4, None, 6]], "a:double,b:double,c:int")
        b = e.to_df(
            [[1, 2, 33], [4, None, 6], [4, None, 6], [4, None, 6]],
            "a:double,b:double,c:int",
        )
        c = e.intersect(a, b, distinct=False)
        df_eq(
            c,
            [[4, None, 6], [4, None, 6]],
            "a:double,b:double,c:int",
            throw=True,
        )


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

        with self.dag() as dag:
            x = dag.df(df)
            result = dag.select("SELECT * FROM ", x)
            result.output(assert_data)


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


def test_sql_yield():
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
