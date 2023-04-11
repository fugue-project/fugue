import pickle
from typing import Any, List

import duckdb
import pandas as pd
import pyarrow as pa
from pytest import raises

import fugue.api as fa
import fugue.plugins as fp
from fugue import (
    ArrayDataFrame,
    ArrowDataFrame,
    DataFrame,
    DuckDataFrame,
    FugueWorkflow,
    NativeExecutionEngine,
    PandasDataFrame,
    fsql,
)
from fugue.execution.execution_engine import _get_file_threshold
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests


class MockExecutionEngine(NativeExecutionEngine):
    def to_df(self, df, schema: Any = None) -> DataFrame:
        # force PandasDataFrame for testing certain branches
        fdf = fa.as_fugue_df(df, schema=schema)
        return PandasDataFrame(fdf.as_pandas(), schema=fdf.schema)


class NativeExecutionEnginePandasTests(ExecutionEngineTests.Tests):
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
        e = MockExecutionEngine(
            {
                "test": True,
                "fugue.duckdb.pragma.threads": 2,
                "fugue.duckdb.extensions": ", json , , httpfs",
            },
            self._con,
        )
        return e


class NativeExecutionEngineDuckDBTests(ExecutionEngineTests.Tests):
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
        e = NativeExecutionEngine(
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
        e = self.engine.sql_engine
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


class NativeExecutionEngineBuiltInDuckDBTests(BuiltInTests.Tests):
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
        e = NativeExecutionEngine(
            {
                "test": True,
                "fugue.duckdb.pragma.threads": 2,
                "fugue.duckdb.extensions": ", json , , httpfs",
            },
            self._con,
        )
        return e

    def test_coarse_partition(self):
        def verify_coarse_partition(df: pd.DataFrame) -> List[List[Any]]:
            ct = df.a.nunique()
            s = df.a * 1000 + df.b
            ordered = ((s - s.shift(1)).dropna() >= 0).all(axis=None)
            return [[ct, ordered]]

        def assert_(df: pd.DataFrame, rc: int, n: int, check_ordered: bool) -> None:
            if rc > 0:
                assert len(df) == rc
            assert df.ct.sum() == n
            if check_ordered:
                assert (df.ordered == True).all()

        gps = 100
        partition_num = 6
        df = pd.DataFrame(dict(a=list(range(gps)) * 10, b=range(gps * 10))).sample(
            frac=1.0
        )
        with FugueWorkflow() as dag:
            a = dag.df(df)
            c = a.partition(
                algo="coarse", by="a", presort="b", num=partition_num
            ).transform(verify_coarse_partition, schema="ct:int,ordered:bool")
            dag.output(
                c,
                using=assert_,
                params=dict(rc=0, n=gps, check_ordered=True),
            )
        dag.run(self.engine)

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


def test_duck_annotations():
    with duckdb.connect() as con:

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
    with duckdb.connect() as con:

        res = fsql(
            """
        CREATE [[0]] SCHEMA a:int
        SELECT *
        YIELD DATAFRAME AS a
        CREATE [[0]] SCHEMA b:int
        YIELD LOCAL DATAFRAME AS b
        """
        ).run(con)

        assert isinstance(res["a"], DuckDataFrame)
        assert isinstance(res["b"], ArrayDataFrame)

        x = fa.union(pdf, pdf, engine=con)
        assert isinstance(x, duckdb.DuckDBPyRelation)

    # ephemeral
    res = fsql(
        """
    CREATE [[0]] SCHEMA a:int
    YIELD DATAFRAME AS a
    CREATE [[0]] SCHEMA b:int
    SELECT *
    YIELD LOCAL DATAFRAME AS b
    """
    ).run("duck")

    assert isinstance(res["a"], ArrayDataFrame)
    # yield local in ephemeral will convert duck df to arrow
    assert isinstance(res["b"], ArrowDataFrame)

    x = fa.union(pdf, pdf, engine="duckdb")
    assert isinstance(x, pa.Table)

    # in context
    with fa.engine_context("duck"):
        res = fsql(
            """
        CREATE [[0]] SCHEMA a:int
        SELECT *
        YIELD DATAFRAME AS a
        CREATE [[0]] SCHEMA b:int
        YIELD LOCAL DATAFRAME AS b
        """
        ).run()

        assert isinstance(res["a"], DuckDataFrame)
        assert isinstance(res["b"], ArrayDataFrame)

        x = fa.union(pdf, pdf)
        assert isinstance(x, duckdb.DuckDBPyRelation)


def test_infer_engine():
    con = duckdb.connect()
    df = con.from_df(pd.DataFrame([[0]], columns=["a"]))
    assert fp.infer_execution_engine([df]) == "duckdb"

    fdf = DuckDataFrame(df)
    assert fp.infer_execution_engine([fdf]) == "duckdb"

    con.close()


def test_get_file_threshold():
    assert -1 == _get_file_threshold(None)
    assert -2 == _get_file_threshold(-2)
    assert 1024 == _get_file_threshold("1k")


def test_pa_annotations():
    def cr() -> pa.Table:
        return pa.Table.from_pandas(pd.DataFrame([[0]], columns=["a"]))

    def pr(df: pa.Table) -> pd.DataFrame:
        return df.to_pandas()

    def ot(df: pa.Table) -> None:
        assert 1 == df.to_pandas().shape[0]

    dag = FugueWorkflow()
    dag.create(cr).process(pr).output(ot)

    dag.run()
