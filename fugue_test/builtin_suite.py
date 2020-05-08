from typing import Any, Dict, Iterable
from unittest import TestCase

import pandas as pd
from adagio.instances import WorkflowContext
from fugue.dag.workflow import FugueWorkflow
from fugue.dataframe import DataFrame, LocalDataFrame, PandasDataFrame
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.execution import ExecutionEngine
from fugue.execution.naive_execution_engine import SqliteEngine
from fugue.transformer import Transformer, transformer


class BuiltInTests(object):
    class Tests(TestCase):
        @classmethod
        def setUpClass(cls):
            cls._engine = cls.make_engine(cls)

        @property
        def engine(self) -> ExecutionEngine:
            return self._engine  # type: ignore

        @classmethod
        def tearDownClass(cls):
            cls._engine.stop()

        def make_engine(self) -> ExecutionEngine:  # pragma: no cover
            raise NotImplementedError

        def dag(self) -> "DagTester":
            return DagTester(self.engine)

        def test_create_show(self):
            with self.dag() as dag:
                dag.df([[0]], "a:int").partition(num=2).show()
                dag.df(ArrayDataFrame([[0]], "a:int")).show()

        def test_transform(self):
            with self.dag() as dag:
                a = dag.df(
                    [[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int", dict(x=1)
                )
                c = a.transform(MockTransform1, params=dict(p="10"))
                dag.df(
                    [[1, 2, 4, 10], [None, 1, 4, 10], [3, 4, 4, 10], [None, 4, 4, 10]],
                    "a:double,b:int,ct:int,p:int",
                ).assert_eq(c)

                c = a.transform(MockTransform1, partition={"by": ["a"]})
                dag.df(
                    [[None, 1, 2, 1], [None, 4, 2, 1], [1, 2, 1, 1], [3, 4, 1, 1]],
                    "a:double,b:int,ct:int,p:int",
                ).assert_eq(c)

                c = a.transform(mock_tf1, partition={"by": ["a"], "presort": "b DESC"})
                dag.df(
                    [[None, 4, 2, 1], [None, 1, 2, 1], [1, 2, 1, 1], [3, 4, 1, 1]],
                    "a:double,b:int,ct:int,p:int",
                ).assert_eq(c)

                c = a.transform(
                    mock_tf2_except,
                    schema="*",
                    partition={"by": ["a"], "presort": "b DESC"},
                    ignore_errors=[NotImplementedError],
                )
                dag.df([[1, 2], [3, 4]], "a:double,b:int").assert_eq(c)

                c = a.partition(by="a", presort="b DESC").transform(
                    mock_tf2_except, schema="*", ignore_errors=[NotImplementedError]
                )
                dag.df([[1, 2], [3, 4]], "a:double,b:int").assert_eq(c)

        def test_join(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, 20], [3, 30]], "a:int,b:int")
                dag.join(a, how="inner").assert_eq(a)

                b = ArrayDataFrame([[2, 200], [3, 300]], "a:int,c:int")
                c = ArrayDataFrame([[2, 2000]], "a:int,d:int")
                d = a.join(b, c, how="inner", keys=["a"])
                dag.df([[2, 20, 200, 2000]], "a:int,b:int,c:int,d:int").assert_eq(d)

        def test_select(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, 20], [3, 30]], "x:long,y:long")
                b = dag.df([[2, 20, 40], [3, 30, 90]], "x:long,y:long,z:long")
                dag.select("SELECT *,x*y AS z FROM", a, "WHERE x>=2").assert_eq(b)

                c = ArrayDataFrame([[2, 20, 40], [3, 30, 90]], "x:long,y:long,zb:long")
                dag.select(
                    "  SELECT t1.*,z AS zb FROM ",
                    a,
                    "AS t1 INNER JOIN",
                    b,
                    "AS t2 ON t1.x=t2.x  ",
                ).assert_eq(c)

                # no select
                dag.select(
                    "t1.*,z AS zb FROM ", a, "AS t1 INNER JOIN", b, "AS t2 ON t1.x=t2.x"
                ).assert_eq(c)

                # specify sql engine
                dag.select(
                    "SELECT t1.*,z AS zb FROM ",
                    a,
                    "AS t1 INNER JOIN",
                    b,
                    "AS t2 ON t1.x=t2.x",
                    sql_engine=SqliteEngine,
                ).assert_eq(c)

                # specify sql engine
                dag.select(
                    "SELECT t1.*,z AS zb FROM ",
                    a,
                    "AS t1 INNER JOIN",
                    b,
                    "AS t2 ON t1.x=t2.x",
                    sql_engine="SqliteEngine",
                ).assert_eq(c)

                # no input
                dag.select("1 AS a").assert_eq(ArrayDataFrame([[1]], "a:long"))


class DagTester(FugueWorkflow):
    def __init__(self, engine: ExecutionEngine):
        super().__init__(engine)
        self.engine = engine
        self.ctx = WorkflowContext()

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.ctx.run(self._spec, {})


class MockTransform1(Transformer):
    def get_output_schema(self, df: DataFrame) -> Any:
        assert "x" in df.metadata
        return [df.schema, "ct:int,p:int"]

    def init_physical_partition(self, df: LocalDataFrame) -> None:
        assert "x" in df.metadata
        self.pn = self.cursor.physical_partition_no
        self.ks = self.key_schema

    def init_logical_partition(self, df: LocalDataFrame) -> None:
        assert "x" in df.metadata
        self.ln = self.cursor.partition_no

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        assert "x" in df.metadata
        pdf = df.as_pandas()
        pdf["p"] = self.params.get("p", 1)
        pdf["ct"] = pdf.shape[0]
        return PandasDataFrame(pdf, self.output_schema)


@transformer("*,ct:int,p:int")
def mock_tf1(df: pd.DataFrame, p=1) -> pd.DataFrame:
    df["ct"] = df.shape[0]
    df["p"] = p
    return df


def mock_tf2_except(df: Iterable[Dict[str, Any]], p=1) -> Iterable[Dict[str, Any]]:
    n = 0
    for row in df:
        yield row
        n += 1
        if n > 1:
            raise NotImplementedError
