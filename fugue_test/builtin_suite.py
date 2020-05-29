import os
from typing import Any, Dict, Iterable, List
from unittest import TestCase

import pandas as pd
import pytest
from fugue.dag.workflow import FugueWorkflow
from fugue.dag.workflow_context import FugueWorkflowContext
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame, PandasDataFrame
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.execution import ExecutionEngine
from fugue.execution.native_execution_engine import SqliteEngine
from fugue.extensions.outputter import Outputter
from fugue.extensions.processor import Processor
from fugue.extensions.transformer import (
    CoTransformer,
    Transformer,
    cotransformer,
    transformer,
)
from triad.collections.fs import FileSystem


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
                dag.df([[0]], "a:int").persist().partition(num=2).show()
                dag.df(dag.df([[0]], "a:int")).persist().broadcast().show()

        def test_create_process_output(self):
            with self.dag() as dag:
                a = dag.create(mock_creator, params=dict(p=2))
                a.assert_eq(ArrayDataFrame([[2]], "a:int"))
                b = dag.process(a, a, using=mock_processor)
                b.assert_eq(ArrayDataFrame([[2]], "a:int"))
                dag.output(a, b, using=mock_outputter)
                b2 = dag.process(a, a, a, using=mock_processor2)
                b2.assert_eq(ArrayDataFrame([[3]], "a:int"))
                b2 = dag.process(a, a, a, using=MockProcessor3)
                b2.assert_eq(ArrayDataFrame([[3]], "a:int"))
                a.process(mock_processor2).assert_eq(ArrayDataFrame([[1]], "a:int"))
                a.output(mock_outputter2)
                a.partition(num=3).output(MockOutputter3)
                dag.output(dict(aa=a, bb=b), using=MockOutputter4)

        def test_zip(self):
            with self.dag() as dag:
                a = dag.df([[1, 2], [2, 3], [2, 5]], "a:int,b:int")
                b = dag.df([[1, 3]], "a:int,c:int")
                c1 = a.zip(b)
                c2 = dag.zip(a, b)
                c1.assert_eq(c2, check_metadata=False)

                a = dag.df([[1, 2], [2, 3], [2, 5]], "a:int,b:int")
                b = dag.df([[1, 3]], "a:int,c:int")
                c1 = a.zip(b, how="left_outer", partition=dict(presort="b DESC, c ASC"))
                c2 = dag.zip(
                    a, b, how="left_outer", partition=dict(presort="b DESC, c ASC")
                )
                c1.assert_eq(c2, check_metadata=False)

                a = dag.df([[1, 2, 0], [1, 3, 1]], "a:int,b:int,c:int")
                b = dag.df([[1, 2, 1], [1, 3, 2]], "a:int,b:int,d:int")
                c = dag.df([[1, 4]], "a:int,e:int")
                e = dag.df([[1, 2], [1, 3]], "a:int,b:int")
                dag.zip(a, b, c)[["a", "b"]].assert_eq(e, check_metadata=False)

        def test_transform(self):
            with self.dag() as dag:
                a = dag.df([[1, 2], [3, 4]], "a:double,b:int", dict(x=1))
                c = a.transform(mock_tf0)
                dag.df([[1, 2, 1], [3, 4, 1]], "a:double,b:int,p:int").assert_eq(c)

                a = dag.df(
                    [[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int", dict(x=1)
                )
                c = a.transform(mock_tf0, params=dict(p="10"))
                dag.df(
                    [[1, 2, 10], [None, 1, 10], [3, 4, 10], [None, 4, 10]],
                    "a:double,b:int,p:int",
                ).assert_eq(c)

        def test_transform_by(self):
            with self.dag() as dag:
                a = dag.df(
                    [[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int", dict(x=1)
                )
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

                c = a.partition(by=["a"], presort="b DESC").transform(
                    mock_tf2_except, schema="*", ignore_errors=[NotImplementedError]
                )
                dag.df([[1, 2], [3, 4]], "a:double,b:int").assert_eq(c)

        def test_cotransform(self):
            with self.dag() as dag:
                a = dag.df([[1, 2], [1, 3], [2, 1]], "a:int,b:int", dict(x=1))
                b = dag.df([[1, 2], [3, 4]], "a:int,c:int", dict(x=1))
                c = dag.transform(a.zip(b), using=MockCoTransform1)
                e = dag.df([[1, 2, 1, 1]], "a:int,ct1:int,ct2:int,p:int")
                e.assert_eq(c)
                c = dag.transform(a.zip(b), using=MockCoTransform1, params=dict(p=10))
                e = dag.df([[1, 2, 1, 10]], "a:int,ct1:int,ct2:int,p:int")
                e.assert_eq(c)
                # with deco
                c = dag.transform(a.zip(b), using=mock_co_tf1, params=dict(p=10))
                e = dag.df([[1, 2, 1, 10]], "a:int,ct1:int,ct2:int,p:int")
                e.assert_eq(c)
                a.zip(b).transform(mock_co_tf1, params=dict(p=10)).assert_eq(e)
                # interfaceless
                c = dag.transform(
                    a.zip(b),
                    using=mock_co_tf2,
                    schema="a:int,ct1:int,ct2:int,p:int",
                    params=dict(p=10),
                )
                e = dag.df([[1, 2, 1, 10]], "a:int,ct1:int,ct2:int,p:int")
                e.assert_eq(c)
                # single df
                c = dag.transform(a.zip(), using=mock_co_tf3)
                e = dag.df([[1, 3, 1]], "a:int,ct1:int,p:int")
                e.assert_eq(c)
                c = dag.transform(a.zip(partition=dict(by=["a"])), using=mock_co_tf3)
                e = dag.df([[1, 2, 1], [2, 1, 1]], "a:int,ct1:int,p:int")
                e.assert_eq(c)
                c = a.partition(by=["a"]).zip().transform(mock_co_tf3)
                e = dag.df([[1, 2, 1], [2, 1, 1]], "a:int,ct1:int,p:int")
                e.assert_eq(c)
                # ignore errors
                c = (
                    a.partition(by=["a"])
                    .zip()
                    .transform(mock_co_tf4_ex, ignore_errors=[NotImplementedError])
                )
                e = dag.df([[1, 2, 1]], "a:int,ct1:int,p:int")
                e.assert_eq(c)

        def test_join(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, 20], [3, 30]], "a:int,b:int")
                dag.join(a, how="inner").assert_eq(a)

                b = dag.df([[2, 200], [3, 300]], "a:int,c:int")
                c = dag.df([[2, 2000]], "a:int,d:int")
                d = a.join(b, c, how="inner", on=["a"])
                dag.df([[2, 20, 200, 2000]], "a:int,b:int,c:int,d:int").assert_eq(d)

        def test_select(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, 20], [3, 30]], "x:long,y:long")
                b = dag.df([[2, 20, 40], [3, 30, 90]], "x:long,y:long,z:long")
                dag.select("* FROM", a).assert_eq(a)
                dag.select("SELECT *,x*y AS z FROM", a, "WHERE x>=2").assert_eq(b)

                c = dag.df([[2, 20, 40], [3, 30, 90]], "x:long,y:long,zb:long")
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
                dag.select("9223372036854775807 AS a").assert_eq(
                    dag.df([[9223372036854775807]], "a:long")
                )

                # make sure transform -> select works
                b = a.transform(mock_tf1)
                a = a.transform(mock_tf1)
                aa = dag.select("* FROM", a)
                dag.select("* FROM", b).assert_eq(aa)

        def test_col_ops(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, 20]], "x:long,y:long")
                aa = dag.df([[1, 10], [2, 20]], "xx:long,y:long")
                a.rename({"x": "xx"}).assert_eq(aa)
                a[["x"]].assert_eq(ArrayDataFrame([[1], [2]], "x:long"))

                a.drop(["y", "yy"], if_exists=True).assert_eq(
                    ArrayDataFrame([[1], [2]], "x:long")
                )

                a[["x"]].rename(x="xx").assert_eq(ArrayDataFrame([[1], [2]], "xx:long"))

        @pytest.fixture(autouse=True)
        def init_tmpdir(self, tmpdir):
            self.tmpdir = tmpdir

        def test_io(self):
            path = os.path.join(self.tmpdir, "a")
            path2 = os.path.join(self.tmpdir, "b.csv")
            with self.dag() as dag:
                b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
                b.partition(num=3).save(path, fmt="parquet", single=True)
                b.save(path2, header=True)
            assert FileSystem().isfile(path)
            with self.dag() as dag:
                a = dag.load(path, fmt="parquet", columns=["a", "c"])
                a.assert_eq(dag.df([[1, 6], [7, 2]], "a:long,c:int"))
                a = dag.load(path2, header=True, columns="c:int,a:long")
                a.assert_eq(dag.df([[6, 1], [2, 7]], "c:int,a:long"))


class DagTester(FugueWorkflow):
    def __init__(self, engine: ExecutionEngine):
        super().__init__()
        self.engine = engine
        self.ctx = FugueWorkflowContext(engine)

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.ctx.run(self._spec, {})


def mock_creator(p: int) -> DataFrame:
    return ArrayDataFrame([[p]], "a:int")


def mock_processor(df1: List[List[Any]], df2: List[List[Any]]) -> DataFrame:
    return ArrayDataFrame([[len(df1) + len(df2)]], "a:int")


def mock_processor2(e: ExecutionEngine, dfs: DataFrames) -> DataFrame:
    assert "test" in e.conf
    return ArrayDataFrame([[sum(s.count() for s in dfs.values())]], "a:int")


class MockProcessor3(Processor):
    def process(self, dfs):
        assert "test" in self.workflow_conf
        return ArrayDataFrame([[sum(s.count() for s in dfs.values())]], "a:int")


def mock_outputter(df1: List[List[Any]], df2: List[List[Any]]) -> None:
    assert len(df1) == len(df2)


def mock_outputter2(df: List[List[Any]]) -> None:
    print(df)


class MockOutputter3(Outputter):
    def process(self, dfs):
        assert "3" == self.partition_spec.num_partitions


class MockOutputter4(Outputter):
    def process(self, dfs):
        for k, v in dfs.items():
            print(k)
            v.show()


class MockTransform1(Transformer):
    def get_output_schema(self, df: DataFrame) -> Any:
        assert "test" in self.workflow_conf
        assert "x" in df.metadata
        return [df.schema, "ct:int,p:int"]

    def on_init(self, df: DataFrame) -> None:
        assert "test" in self.workflow_conf
        assert "x" in df.metadata
        self.pn = self.cursor.physical_partition_no
        self.ks = self.key_schema
        if "on_init_called" not in self.__dict__:
            self.on_init_called = 1
        else:  # pragma: no cover
            self.on_init_called += 1

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        assert 1 == self.on_init_called
        assert "test" in self.workflow_conf
        assert "x" in df.metadata
        pdf = df.as_pandas()
        pdf["p"] = self.params.get("p", 1)
        pdf["ct"] = pdf.shape[0]
        return PandasDataFrame(pdf, self.output_schema)


@transformer("*,p:int")
def mock_tf0(df: pd.DataFrame, p=1) -> pd.DataFrame:
    df["p"] = p
    return df


# schema: *,ct:int,p:int
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


class MockCoTransform1(CoTransformer):
    def get_output_schema(self, dfs: DataFrames) -> Any:
        assert "test" in self.workflow_conf
        assert 2 == len(dfs)
        return [self.key_schema, "ct1:int,ct2:int,p:int"]

    def on_init(self, dfs: DataFrames) -> None:
        assert "test" in self.workflow_conf
        assert 2 == len(dfs)
        self.pn = self.cursor.physical_partition_no
        self.ks = self.key_schema
        if "on_init_called" not in self.__dict__:
            self.on_init_called = 1
        else:  # pragma: no cover
            self.on_init_called += 1

    def transform(self, dfs: DataFrames) -> LocalDataFrame:
        assert 1 == self.on_init_called
        assert "test" in self.workflow_conf
        assert 2 == len(dfs)
        row = self.cursor.key_value_array + [
            dfs[0].count(),
            dfs[1].count(),
            self.params.get("p", 1),
        ]
        return ArrayDataFrame([row], self.output_schema)


@cotransformer("a:int,ct1:int,ct2:int,p:int")
def mock_co_tf1(
    df1: List[Dict[str, Any]], df2: List[List[Any]], p=1
) -> List[List[Any]]:
    return [[df1[0]["a"], len(df1), len(df2), p]]


def mock_co_tf2(dfs: DataFrames, p=1) -> List[List[Any]]:
    return [[dfs[0].peek_dict()["a"], dfs[0].count(), dfs[1].count(), p]]


@cotransformer("a:int,ct1:int,p:int")
def mock_co_tf3(df1: List[Dict[str, Any]], p=1) -> List[List[Any]]:
    return [[df1[0]["a"], len(df1), p]]


@cotransformer("a:int,ct1:int,p:int")
def mock_co_tf4_ex(df1: List[Dict[str, Any]], p=1) -> List[List[Any]]:
    k = df1[0]["a"]
    if k == 2:
        raise NotImplementedError
    return [[df1[0]["a"], len(df1), p]]
