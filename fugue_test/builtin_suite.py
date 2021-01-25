import datetime
import os
import pickle
from typing import Any, Callable, Dict, Iterable, List, Optional
from unittest import TestCase

import numpy as np
import pandas as pd
import pytest
from fugue import FileSystem, Schema
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame, PandasDataFrame
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import (
    FugueInterfacelessError,
    FugueWorkflowCompileError,
    FugueWorkflowCompileValidationError,
    FugueWorkflowError,
    FugueWorkflowRuntimeValidationError,
)
from fugue.execution import ExecutionEngine
from fugue.execution.native_execution_engine import SqliteEngine
from fugue.extensions.outputter import Outputter, outputter
from fugue.extensions.processor import Processor, processor
from fugue.extensions.transformer import (
    CoTransformer,
    OutputCoTransformer,
    OutputTransformer,
    Transformer,
    cotransformer,
    output_cotransformer,
    output_transformer,
    transformer,
)
from fugue.workflow.workflow import FugueWorkflow, WorkflowDataFrame
from pytest import raises


class BuiltInTests(object):
    """Workflow level general test suite. It is a more general end to end
    test suite than :class:`~fugue_test.execution_suite.ExecutionEngineTests`.
    Any new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    should also pass this test suite.

    Whenever you add method to FugueWorkflow and WorkflowDataFrame, you should
    add correspondent tests here
    """

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

        def dag(self) -> FugueWorkflow:
            return FugueWorkflow(self.engine)

        def test_workflows(self):
            a = FugueWorkflow().df([[0]], "a:int")
            df_eq(a.compute(self.engine), [[0]], "a:int")

        def test_create_show(self):
            with self.dag() as dag:
                dag.df([[0]], "a:int").persist().partition(num=2).show()
                dag.df(dag.df([[0]], "a:int")).persist().broadcast().show(title="t")

        def test_checkpoint(self):
            with raises(FugueWorkflowError):
                with self.dag() as dag:
                    dag.df([[0]], "a:int").checkpoint()

            self.engine.conf["fugue.workflow.checkpoint.path"] = os.path.join(
                self.tmpdir, "ck"
            )
            with self.dag() as dag:
                a = dag.df([[0]], "a:int").checkpoint()
                dag.df([[0]], "a:int").assert_eq(a)

        def test_deterministic_checkpoint(self):
            self.engine.conf["fugue.workflow.checkpoint.path"] = os.path.join(
                self.tmpdir, "ck"
            )
            temp_file = os.path.join(self.tmpdir, "t.parquet")

            def mock_create(dummy: int = 1) -> pd.DataFrame:
                return pd.DataFrame(np.random.rand(3, 2), columns=["a", "b"])

            # base case without checkpoint, two runs should generate different result
            with self.dag() as dag:
                a = dag.create(mock_create)
                a.save(temp_file)
            with self.dag() as dag:
                a = dag.create(mock_create)
                b = dag.load(temp_file)
                b.assert_not_eq(a)

            # strong checkpoint is not cross execution, so result should be different
            with self.dag() as dag:
                a = dag.create(mock_create).strong_checkpoint()
                a.save(temp_file)
            with self.dag() as dag:
                a = dag.create(mock_create).strong_checkpoint()
                b = dag.load(temp_file)
                b.assert_not_eq(a)

            # with deterministic checkpoint, two runs should generate the same result
            with self.dag() as dag:
                dag.create(
                    mock_create, params=dict(dummy=2)
                )  # this should not affect a because it's not a dependency
                a = dag.create(mock_create).deterministic_checkpoint()
                id1 = a.spec_uuid()
                a.save(temp_file)
            with self.dag() as dag:
                a = dag.create(mock_create).deterministic_checkpoint()
                b = dag.load(temp_file)
                b.assert_eq(a)
            with self.dag() as dag:
                # checkpoint specs itself doesn't change determinism
                # of the previous steps
                a = dag.create(mock_create).deterministic_checkpoint(
                    partition=PartitionSpec(num=2)
                )
                id2 = a.spec_uuid()
                b = dag.load(temp_file)
                b.assert_eq(a)
            with self.dag() as dag:
                # dependency change will affect determinism
                a = dag.create(
                    mock_create, params={"dummy": 2}
                ).deterministic_checkpoint()
                id3 = a.spec_uuid()
                b = dag.load(temp_file)
                b.assert_not_eq(a)

            assert (
                id1 == id2
            )  # different types of checkpoint doesn't change dag determinism
            assert id1 != id3

        def test_deterministic_checkpoint_complex_dag(self):
            self.engine.conf["fugue.workflow.checkpoint.path"] = os.path.join(
                self.tmpdir, "ck"
            )
            temp_file = os.path.join(self.tmpdir, "t.parquet")

            def mock_create(dummy: int = 1) -> pd.DataFrame:
                return pd.DataFrame(np.random.rand(3, 2), columns=["a", "b"])

            # base case without checkpoint, two runs should generate different result
            with self.dag() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b)
                c.save(temp_file)
            with self.dag() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b)
                d = dag.load(temp_file)
                d.assert_not_eq(c)

            # strong checkpoint is not cross execution, so result should be different
            with self.dag() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).strong_checkpoint()
                c.save(temp_file)
            with self.dag() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).strong_checkpoint()
                d = dag.load(temp_file)
                d.assert_not_eq(c)

            # with deterministic checkpoint, two runs should generate the same result
            with self.dag() as dag:
                dag.create(
                    mock_create, params=dict(dummy=2)
                )  # this should not affect a because it's not a dependency
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b)
                c.deterministic_checkpoint()
                c.save(temp_file)
            with self.dag() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).deterministic_checkpoint()
                d = dag.load(temp_file)
                d.assert_eq(c)
            with self.dag() as dag:
                # checkpoint specs itself doesn't change determinism
                # of the previous steps
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).deterministic_checkpoint(partition=PartitionSpec(num=2))
                d = dag.load(temp_file)
                d.assert_eq(c)
            with self.dag() as dag:
                # dependency change will affect determinism
                a = dag.create(mock_create, params={"dummy": 2}).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).deterministic_checkpoint(partition=PartitionSpec(num=2))
                d = dag.load(temp_file)
                d.assert_not_eq(c)

        def test_output(self):
            dag = self.dag()
            dag.df([[0]], "a:int").output_as("k")
            result = dag.run()
            assert "k" in result
            assert not isinstance(result["k"], WorkflowDataFrame)
            assert isinstance(result["k"], DataFrame)
            assert isinstance(result["k"].as_pandas(), pd.DataFrame)
            # TODO: these don't work
            # assert not isinstance(list(result.values())[0], WorkflowDataFrame)
            # assert isinstance(list(result.values())[0], DataFrame)

        def test_yield(self):
            self.engine.conf["fugue.workflow.checkpoint.path"] = os.path.join(
                self.tmpdir, "ck"
            )

            with raises(FugueWorkflowCompileError):
                self.dag().df([[0]], "a:int").checkpoint().yield_as("x")

            with raises(FugueWorkflowCompileError):
                self.dag().df([[0]], "a:int").persist().yield_as("x")

            def run_test(deterministic):
                dag1 = self.dag()
                df = dag1.df([[0]], "a:int")
                if deterministic:
                    df = df.deterministic_checkpoint()
                df.yield_as("x")
                id1 = dag1.spec_uuid()
                dag2 = self.dag()
                dag2.df([[0]], "a:int").assert_eq(dag2.df(dag1.yields["x"]))
                id2 = dag2.spec_uuid()
                dag1.run()
                dag2.run()
                return id1, id2

            id1, id2 = run_test(False)
            id3, id4 = run_test(False)
            assert id1 == id3
            assert id2 != id4  # non deterministic yield (direct yield)

            id1, id2 = run_test(True)
            id3, id4 = run_test(True)
            assert id1 == id3
            assert id2 == id4  # deterministic yield (yield deterministic checkpoint)

        def test_create_process_output(self):
            with self.dag() as dag:
                a = dag.create(mock_creator, params=dict(p=2))
                a.assert_eq(ArrayDataFrame([[2]], "a:int"))
                b = dag.process(a, a, using=mock_processor)
                b.assert_eq(ArrayDataFrame([[2]], "a:int"))
                b = dag.process(dict(df1=a, df2=a), using=mock_processor)
                b.assert_eq(ArrayDataFrame([[2]], "a:int"))
                dag.output(a, b, using=mock_outputter)
                b2 = dag.process(a, a, a, using=mock_processor2)
                b2.assert_eq(ArrayDataFrame([[3]], "a:int"))
                b2 = dag.process(a, a, a, using=MockProcessor3)
                b2.assert_eq(ArrayDataFrame([[3]], "a:int"))
                a.process(mock_processor2).assert_eq(ArrayDataFrame([[1]], "a:int"))
                a.output(mock_outputter2)
                dag.output(dict(df=a), using=mock_outputter2)
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

                c = a.transform(mock_tf0, params=dict(col="x"))
                dag.df([[1, 2, 1], [3, 4, 1]], "a:double,b:int,x:int").assert_eq(c)

                a = dag.df(
                    [[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int", dict(x=1)
                )
                c = a.transform(mock_tf0, params=dict(p="10"))
                dag.df(
                    [[1, 2, 10], [None, 1, 10], [3, 4, 10], [None, 4, 10]],
                    "a:double,b:int,p:int",
                ).assert_eq(c)

        def test_local_instance_as_extension(self):
            class _Mock(object):
                # schema: *
                def t1(self, df: pd.DataFrame) -> pd.DataFrame:
                    return df

                def t2(self, df: pd.DataFrame) -> pd.DataFrame:
                    return df

                def test(self):
                    with FugueWorkflow() as dag_:
                        a = dag_.df([[0], [1]], "a:int")
                        b = a.transform(self.t1)
                        b.assert_eq(a)

            m = _Mock()
            m.test()
            with self.dag() as dag:
                a = dag.df([[0], [1]], "a:int")
                b = a.transform(m.t1).transform(m.t2, schema="*")
                b.assert_eq(a)

        def test_transform_binary(self):
            with self.dag() as dag:
                a = dag.df([[1, pickle.dumps([0, "a"])]], "a:int,b:bytes")
                c = a.transform(mock_tf3)
                b = dag.df([[1, pickle.dumps([1, "ax"])]], "a:int,b:bytes")
                b.assert_eq(c, check_order=True)

        def test_transform_by(self):
            with self.dag() as dag:
                a = dag.df(
                    [[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int", dict(x=1)
                )
                c = a.transform(MockTransform1, pre_partition={"by": ["a"]})
                dag.df(
                    [[None, 1, 2, 1], [None, 4, 2, 1], [1, 2, 1, 1], [3, 4, 1, 1]],
                    "a:double,b:int,ct:int,p:int",
                ).assert_eq(c)

                c = a.transform(
                    mock_tf1, pre_partition={"by": ["a"], "presort": "b DESC"}
                )
                dag.df(
                    [[None, 4, 2, 1], [None, 1, 2, 1], [1, 2, 1, 1], [3, 4, 1, 1]],
                    "a:double,b:int,ct:int,p:int",
                ).assert_eq(c)

                c = a.transform(
                    mock_tf2_except,
                    schema="*",
                    pre_partition={"by": ["a"], "presort": "b DESC"},
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

                c = dag.transform(
                    a.zip(b), using=mock_co_tf1, params=dict(p=10, col="x")
                )
                e = dag.df([[1, 2, 1, 10]], "a:int,ct1:int,ct2:int,x:int")
                e.assert_eq(c)

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

        def test_cotransform_with_key(self):
            with self.dag() as dag:
                a = dag.df([[1, 2], [1, 3], [2, 1]], "a:int,b:int", dict(x=1))
                b = dag.df([[1, 2], [3, 4]], "a:int,c:int", dict(x=1))
                dag.zip(dict(x=a, y=b)).show()
                c = dag.zip(dict(x=a, y=b)).transform(
                    MockCoTransform1, params=dict(named=True)
                )
                e = dag.df([[1, 2, 1, 1]], "a:int,ct1:int,ct2:int,p:int")
                e.assert_eq(c)

                c = dag.zip(dict(df1=a, df2=b)).transform(
                    mock_co_tf1, params=dict(p=10)
                )
                e = dag.df([[1, 2, 1, 10]], "a:int,ct1:int,ct2:int,p:int")
                e.assert_eq(c)

                c = dag.zip(dict(df2=a, df1=b)).transform(
                    mock_co_tf1, params=dict(p=10)
                )
                e = dag.df([[1, 1, 2, 10]], "a:int,ct1:int,ct2:int,p:int")
                e.assert_eq(c)

                c = dag.transform(
                    dag.zip(dict(x=a, y=b)),
                    using=mock_co_tf2,
                    schema="a:int,ct1:int,ct2:int,p:int",
                    params=dict(p=10),
                )
                e = dag.df([[1, 2, 1, 10]], "a:int,ct1:int,ct2:int,p:int")
                e.assert_eq(c)

                c = dag.zip(dict(df1=a)).transform(mock_co_tf3)
                e = dag.df([[1, 3, 1]], "a:int,ct1:int,p:int")
                e.assert_eq(c)

        def test_out_transform(self):  # noqa: C901
            tmpdir = str(self.tmpdir)

            def incr():
                fs = FileSystem().makedirs(tmpdir, recreate=True)
                if fs.exists("a.txt"):
                    n = int(fs.readtext("a.txt"))
                else:
                    n = 0
                fs.writetext("a.txt", str(n + 1))
                return n

            def t1(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
                for row in df:
                    incr()
                    yield row

            # partitionby_has: b
            def t2(df: pd.DataFrame) -> None:
                incr()

            # schema: *
            # partitionby_has: b
            def t3(df: pd.DataFrame) -> pd.DataFrame:
                incr()
                return df

            @transformer("*", partitionby_has=["b"])
            def t4(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
                for row in df:
                    incr()
                    yield row

            @output_transformer(partitionby_has=["b"])
            def t5(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
                for row in df:
                    incr()
                    yield row

            class T6(Transformer):
                def get_output_schema(self, df):
                    return df.schema

                def transform(self, df):
                    incr()
                    return df

            class T7(OutputTransformer):
                @property
                def validation_rules(self):
                    return {"partitionby_has": "b"}

                def process(self, df):
                    incr()

            def t8(df: pd.DataFrame) -> None:
                incr()
                raise NotImplementedError

            def t9(df: pd.DataFrame) -> Iterable[pd.DataFrame]:
                incr()
                yield df

            with self.dag() as dag:
                a = dag.df([[1, 2], [3, 4]], "a:double,b:int")
                a.out_transform(t1)  # +2
                a.partition(by=["b"]).out_transform(t2)  # +1 or +2
                a.partition(by=["b"]).out_transform(t3)  # +1 or +2
                a.partition(by=["b"]).out_transform(t4)  # +2
                a.partition(by=["b"]).out_transform(t5)  # +2
                a.out_transform(T6)  # +1
                a.partition(by=["b"]).out_transform(T7)  # +1
                a.out_transform(t8, ignore_errors=[NotImplementedError])  # +1
                a.out_transform(t9)  # +1
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(t2))
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(t3))
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(t4))
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(t5))
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(T7))

            assert 12 <= incr()

        def test_output_cotransform(self):  # noqa: C901
            tmpdir = str(self.tmpdir)

            def incr():
                fs = FileSystem().makedirs(tmpdir, recreate=True)
                if fs.exists("a.txt"):
                    n = int(fs.readtext("a.txt"))
                else:
                    n = 0
                fs.writetext("a.txt", str(n + 1))
                return n

            def t1(
                df: Iterable[Dict[str, Any]], df2: pd.DataFrame
            ) -> Iterable[Dict[str, Any]]:
                for row in df:
                    incr()
                    yield row

            def t2(dfs: DataFrames) -> None:
                incr()

            def t3(df: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
                incr()
                return df

            @cotransformer("a:double,b:int")
            def t4(
                df: Iterable[Dict[str, Any]], df2: pd.DataFrame
            ) -> Iterable[Dict[str, Any]]:
                for row in df:
                    incr()
                    yield row

            @output_cotransformer()
            def t5(
                df: Iterable[Dict[str, Any]], df2: pd.DataFrame
            ) -> Iterable[Dict[str, Any]]:
                for row in df:
                    incr()
                    yield row

            class T6(CoTransformer):
                def get_output_schema(self, dfs):
                    return dfs[0].schema

                def transform(self, dfs):
                    incr()
                    return dfs[0]

            class T7(OutputCoTransformer):
                def process(self, dfs):
                    incr()

            def t8(df: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
                incr()
                raise NotImplementedError

            def t9(df1: pd.DataFrame, df2: pd.DataFrame) -> Iterable[pd.DataFrame]:
                incr()
                for df in [df1, df2]:
                    yield df

            with self.dag() as dag:
                a0 = dag.df([[1, 2], [3, 4]], "a:double,b:int")
                a1 = dag.df([[1, 2], [3, 4]], "aa:double,b:int")
                a = dag.zip(a0, a1)
                a.out_transform(t1)  # +2
                a.out_transform(t2)  # +1 or +2
                a.out_transform(t3)  # +1 or +2
                a.out_transform(t4)  # +2
                b = dag.zip(dict(df=a0, df2=a1))
                b.out_transform(t5)  # +2
                a.out_transform(T6)  # +1
                a.out_transform(T7)  # +1
                a.out_transform(t8, ignore_errors=[NotImplementedError])  # +1
                a.out_transform(t9)  # +1

            assert 12 <= incr()

        def test_join(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, 20], [3, 30]], "a:int,b:int")
                dag.join(a, how="inner").assert_eq(a)

                b = dag.df([[2, 200], [3, 300]], "a:int,c:int")
                c = dag.df([[2, 2000]], "a:int,d:int")
                d = a.join(b, c, how="inner")  # infer join on
                dag.df([[2, 20, 200, 2000]], "a:int,b:int,c:int,d:int").assert_eq(d)
                d = a.inner_join(b, c)
                dag.df([[2, 20, 200, 2000]], "a:int,b:int,c:int,d:int").assert_eq(d)
                d = a.semi_join(b, c)
                dag.df([[2, 20]], "a:int,b:int").assert_eq(d)
                d = a.left_semi_join(b, c)
                dag.df([[2, 20]], "a:int,b:int").assert_eq(d)
                d = a.anti_join(b, c)
                dag.df([[1, 10]], "a:int,b:int").assert_eq(d)
                d = a.left_anti_join(b, c)
                dag.df([[1, 10]], "a:int,b:int").assert_eq(d)

                # TODO: change these to str type to only test outer features?
                a = dag.df([[1, 10], [2, 20], [3, 30]], "a:int,b:int")
                b = dag.df([[2, 200], [3, 300]], "a:int,c:int")
                c = dag.df([[2, 2000], [4, 4000]], "a:int,d:int")
                d = a.left_outer_join(b, c)
                dag.df(
                    [[1, 10, None, None], [2, 20, 200, 2000], [3, 30, 300, None]],
                    "a:int,b:int,c:int,d:int",
                ).assert_eq(d)
                d = a.right_outer_join(b, c)
                dag.df(
                    [[2, 20, 200, 2000], [4, None, None, 4000]],
                    "a:int,b:int,c:int,d:int",
                ).assert_eq(d)
                d = a.full_outer_join(b, c)
                dag.df(
                    [
                        [1, 10, None, None],
                        [2, 20, 200, 2000],
                        [3, 30, 300, None],
                        [4, None, None, 4000],
                    ],
                    "a:int,b:int,c:int,d:int",
                ).assert_eq(d)

                a = dag.df([[1, 10], [2, 20]], "a:int,b:int")
                b = dag.df([[2], [3]], "c:int")
                c = dag.df([[4]], "d:int")
                d = a.cross_join(b, c)
                dag.df(
                    [[1, 10, 2, 4], [1, 10, 3, 4], [2, 20, 2, 4], [2, 20, 3, 4]],
                    "a:int,b:int,c:int,d:int",
                ).assert_eq(d)

        def test_select(self):
            class MockEngine(SqliteEngine):
                def __init__(self, execution_engine, p: int = 0):
                    super().__init__(execution_engine)
                    self.p = p

                def select(self, dfs, statement):
                    assert 2 == self.p  # assert set p value is working
                    return super().select(dfs, statement)

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

                # specify sql engine and params
                dag.select(
                    "SELECT t1.*,z AS zb FROM ",
                    a,
                    "AS t1 INNER JOIN",
                    b,
                    "AS t2 ON t1.x=t2.x",
                    sql_engine=MockEngine,
                    sql_engine_params={"p": 2},
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

        def test_union(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, None], [2, None]], "x:long,y:double")
                b = dag.df([[2, None], [2, 20]], "x:long,y:double")
                c = dag.df([[1, 10], [2, 20]], "x:long,y:double")
                a.union().assert_eq(a)
                a.union(b, c).assert_eq(
                    ArrayDataFrame(
                        [
                            [1, 10],
                            [2, None],
                            [2, 20],
                        ],
                        "x:long,y:double",
                    )
                )
                a.union(b, c, distinct=False).assert_eq(
                    ArrayDataFrame(
                        [
                            [1, 10],
                            [2, None],
                            [2, None],
                            [2, None],
                            [2, 20],
                            [1, 10],
                            [2, 20],
                        ],
                        "x:long,y:double",
                    )
                )

        def test_intersect(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, None], [2, None]], "x:long,y:double")
                b = dag.df([[2, None], [2, 20]], "x:long,y:double")
                c = dag.df([[1, 10], [2, 20]], "x:long,y:double")
                # d = dag.df([[1, 10], [2, 20], [2, None]], "x:long,y:double")
                a.intersect(b).assert_eq(
                    ArrayDataFrame(
                        [[2, None]],
                        "x:long,y:double",
                    )
                )
                a.intersect(b, c).assert_eq(
                    ArrayDataFrame(
                        [],
                        "x:long,y:double",
                    )
                )
                # TODO: INTERSECT ALL is not implemented (QPD issue)
                # a.intersect(b, distinct=False).assert_eq(
                #     ArrayDataFrame(
                #         [[2, None], [2, None]],
                #         "x:long,y:double",
                #     )
                # )
                # a.intersect(b, d, distinct=False).assert_eq(
                #     ArrayDataFrame(
                #         [[2, None], [2, None]],
                #         "x:long,y:double",
                #     )
                # )

        def test_subtract(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, None], [2, None]], "x:long,y:double")
                b = dag.df([[2, None], [2, 20]], "x:long,y:double")
                c = dag.df([[1, 10], [2, 20]], "x:long,y:double")
                a.subtract(b).assert_eq(
                    ArrayDataFrame(
                        [[1, 10]],
                        "x:long,y:double",
                    )
                )
                a.subtract(c).assert_eq(
                    ArrayDataFrame(
                        [[2, None]],
                        "x:long,y:double",
                    )
                )
                # # TODO: EXCEPT ALL is not implemented (QPD issue)
                # a.subtract(c, distinct=False).assert_eq(
                #     ArrayDataFrame(
                #         [[2, None], [2, None]],
                #         "x:long,y:double",
                #     )
                # )
                a.subtract(b, c).assert_eq(
                    ArrayDataFrame(
                        [],
                        "x:long,y:double",
                    )
                )

        def test_distinct(self):
            with self.dag() as dag:
                a = dag.df([[1, 10], [2, None], [2, None]], "x:long,y:double")
                a.distinct().assert_eq(
                    ArrayDataFrame(
                        [[1, 10], [2, None]],
                        "x:long,y:double",
                    )
                )

        def test_dropna(self):
            with self.dag() as dag:
                a = dag.df(
                    [[1, 10, 10], [None, 2, None], [2, None, 4]],
                    "x:double,y:double,z:double",
                )
                a.dropna().assert_eq(
                    ArrayDataFrame(
                        [[1, 10, 10]],
                        "x:double,y:double,z:double",
                    )
                )
                a.dropna(how="all").assert_eq(a)
                a.dropna(thresh=2).assert_eq(
                    ArrayDataFrame(
                        [[1, 10, 10], [2, None, 4]],
                        "x:double,y:double,z:double",
                    )
                )
                a.dropna(how="any", subset=["x", "z"]).assert_eq(
                    ArrayDataFrame(
                        [[1, 10, 10], [2, None, 4]],
                        "x:double,y:double,z:double",
                    )
                )
                a.dropna(thresh=1, subset=["y", "z"]).assert_eq(a)

        def test_fillna(self):
            with self.dag() as dag:
                a = dag.df(
                    [[1, 10, 10], [None, 2, None], [2, None, 4]],
                    "x:double,y:double,z:double",
                )
                a.fillna(-99).assert_eq(
                    ArrayDataFrame(
                        [[1, 10, 10], [-99, 2, -99], [2, -99, 4]],
                        "x:double,y:double,z:double",
                    )
                )
                a.fillna(-99, subset=["y"]).assert_eq(
                    ArrayDataFrame(
                        [[1, 10, 10], [None, 2, None], [2, -99, 4]],
                        "x:double,y:double,z:double",
                    )
                )
                # Subset is ignored if value is a dict
                a.fillna({"y": 0, "z": -99}, subset=["y"]).assert_eq(
                    ArrayDataFrame(
                        [[1, 10, 10], [None, 2, -99], [2, 0, 4]],
                        "x:double,y:double,z:double",
                    )
                )
            with raises(FugueWorkflowError):
                with self.dag() as dag:
                    dag.df([[None, 1]], "a:int,b:int").fillna({"a": None, "b": 1})

            with raises(FugueWorkflowError):
                with self.dag() as dag:
                    dag.df([[None, 1]], "a:int,b:int").fillna(None)

        def test_sample(self):
            with self.dag() as dag:
                a = dag.df(
                    [[1, 10, 10], [None, 2, None], [2, None, 4]],
                    "x:double,y:double,z:double",
                )
                a.sample(frac=0.5, replace=False, seed=0).show()

            with self.dag() as dag:
                with raises(ValueError):
                    dag.df([[None, 1]], "a:int,b:int").sample(n=1, frac=0.2)

        def test_take(self):
            # Partition by and presort with NULLs
            # Column c needs to be kept even if not in presort or partition
            with self.dag() as dag:
                a = dag.df(
                    [
                        [0, 1, 1],
                        [0, 2, 1],
                        [1, 3, 1],
                        [1, 4, 1],
                        [None, 2, 1],
                        [None, 3, 1],
                    ],
                    "a:double,b:double,c:double",
                )
                a.partition(by=["a"], presort="b desc").take(n=1).assert_eq(
                    ArrayDataFrame(
                        [[0, 2, 1], [1, 4, 1], [None, 3, 1]],
                        "a:double,b:double,c:double",
                    )
                )
            # No partition
            with self.dag() as dag:
                a = dag.df([[0, 1], [0, 2], [1, 3]], "a:int,b:int")
                a.take(1, presort="a,b desc").assert_eq(
                    ArrayDataFrame([[0, 2]], "a:int,b:int")
                )
            # take presort overrides partition presort
            with self.dag() as dag:
                a = dag.df(
                    [[0, 1], [0, 2], [1, 3], [1, 4], [None, 2], [None, 3]],
                    "a:double,b:double",
                )
                a.partition(by=["a"], presort="b desc").take(
                    n=1, presort="b asc"
                ).assert_eq(
                    ArrayDataFrame([[0, 1], [1, 3], [None, 2]], "a:double,b:double")
                )
            # order by with NULL first
            with self.dag() as dag:
                a = dag.df([[0, 1], [0, 2], [None, 3]], "a:double,b:double")
                a.take(1, presort="a desc", na_position="first").assert_eq(
                    ArrayDataFrame([[None, 3]], "a:double,b:double")
                )
            # order by with NULL last
            with self.dag() as dag:
                a = dag.df([[0, 1], [1, 2], [None, 3]], "a:double,b:double")
                a.take(1, presort="a desc", na_position="last").assert_eq(
                    ArrayDataFrame([[1, 2]], "a:double,b:double")
                )
            # Return any row because no presort
            with self.dag() as dag:
                a = dag.df([[0, 1], [0, 2], [1, 3]], "a:int,b:int")
                a.take(1).show()
            # n must be int
            with self.dag() as dag:
                with raises(ValueError):
                    a = dag.df([[0, 1], [0, 2], [1, 3]], "a:int,b:int")
                    a.take(0.5).show()
            # na_position not valid
            with self.dag() as dag:
                with raises(ValueError):
                    a = dag.df([[0, 1], [0, 2], [1, 3]], "a:int,b:int")
                    a.take(1, na_position=["True", "False"]).show()

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
                a.alter_columns("x:str").assert_eq(
                    ArrayDataFrame([["1", 10], ["2", 20]], "x:str,y:long")
                )

        def test_datetime_in_workflow(self):
            # schema: a:date,b:datetime
            def t1(df: pd.DataFrame) -> pd.DataFrame:
                df["b"] = "2020-01-02"
                df["b"] = pd.to_datetime(df["b"])
                return df

            class T2(Transformer):
                def get_output_schema(self, df):
                    return df.schema

                def transform(self, df):
                    # test for issue https://github.com/fugue-project/fugue/issues/92
                    return PandasDataFrame(df.as_pandas())

            with self.dag() as dag:
                a = dag.df([["2020-01-01"]], "a:date").transform(t1)
                b = dag.df(
                    [[datetime.date(2020, 1, 1), datetime.datetime(2020, 1, 2)]],
                    "a:date,b:datetime",
                )
                b.assert_eq(a)
                c = dag.df([["2020-01-01", "2020-01-01 00:00:00"]], "a:date,b:datetime")
                c.transform(T2).assert_eq(c)
                c.partition(by=["a"]).transform(T2).assert_eq(c)

        @pytest.fixture(autouse=True)
        def init_tmpdir(self, tmpdir):
            self.tmpdir = tmpdir

        def test_io(self):
            path = os.path.join(self.tmpdir, "a")
            path2 = os.path.join(self.tmpdir, "b.test.csv")
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

        def test_save_and_use(self):
            path = os.path.join(self.tmpdir, "a")
            with self.dag() as dag:
                b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
                c = b.save_and_use(path, fmt="parquet")
                b.assert_eq(c)

            with self.dag() as dag:
                b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
                d = dag.load(path, fmt="parquet")
                b.assert_eq(d)

        def test_transformer_validation(self):
            # partitionby_has: b
            # input_has: a
            # schema: *
            def t1(df: pd.DataFrame) -> pd.DataFrame:
                return df

            @transformer("*", partitionby_has=["b"], input_has=["a"])
            def t2(df: pd.DataFrame) -> pd.DataFrame:
                return df

            class T3(Transformer):
                @property
                def validation_rules(self):
                    return dict(partitionby_has=["b"], input_has=["a"])

                def get_output_schema(self, df: DataFrame) -> Any:
                    return df.schema

                def transform(self, df: DataFrame) -> LocalDataFrame:
                    return df.as_local()

            for t in [t1, t2, T3]:
                # compile time
                with raises(FugueWorkflowCompileValidationError):
                    self.dag().df([[0, 1]], "a:int,b:int").transform(t)

                # runtime
                with raises(FugueWorkflowRuntimeValidationError):
                    with self.dag() as dag:
                        dag.df([[0, 1]], "c:int,b:int").partition(by=["b"]).transform(t)

                with self.dag() as dag:
                    dag.df([[0, 1]], "a:int,b:int").partition(by=["b"]).transform(
                        t
                    ).assert_eq(dag.df([[0, 1]], "a:int,b:int"))

        def test_processor_validation(self):
            # input_has: a
            def p1(dfs: DataFrames) -> DataFrame:
                return dfs[0]

            @processor(input_has=["a"])
            def p2(dfs: DataFrames) -> DataFrame:
                return dfs[0]

            class P3(Processor):
                @property
                def validation_rules(self):
                    return dict(input_has=["a"])

                def process(self, dfs: DataFrames) -> DataFrame:
                    return dfs[0]

            for p in [p1, p2, P3]:
                # run time
                with raises(FugueWorkflowRuntimeValidationError):
                    with self.dag() as dag:
                        df1 = dag.df([[0, 1]], "a:int,b:int")
                        df2 = dag.df([[0, 1]], "c:int,d:int")
                        dag.process([df1, df2], using=p)

                with self.dag() as dag:
                    df1 = dag.df([[0, 1]], "a:int,b:int")
                    df2 = dag.df([[0, 1]], "a:int,b:int")
                    dag.process([df1, df2], using=p).assert_eq(df1)

            # input_has: a
            # partitionby_has: b
            def p4(dfs: DataFrames) -> DataFrame:
                return dfs[0]

            # compile time
            with raises(FugueWorkflowCompileValidationError):
                dag = self.dag()
                df = dag.df([[0, 1]], "a:int,b:int")
                df.process(p4)

            with self.dag() as dag:
                dag.df([[0, 1]], "a:int,b:int").partition(by=["b"]).process(p4)

        def test_outputter_validation(self):
            # input_has: a
            def o1(dfs: DataFrames) -> None:
                pass

            @outputter(input_has=["a"])
            def o2(dfs: DataFrames) -> None:
                pass

            class O3(Outputter):
                @property
                def validation_rules(self):
                    return dict(input_has=["a"])

                def process(self, dfs: DataFrames) -> None:
                    pass

            for o in [o1, o2, O3]:
                # run time
                with raises(FugueWorkflowRuntimeValidationError):
                    with self.dag() as dag:
                        df1 = dag.df([[0, 1]], "a:int,b:int")
                        df2 = dag.df([[0, 1]], "c:int,d:int")
                        dag.output([df1, df2], using=o)

                with self.dag() as dag:
                    df1 = dag.df([[0, 1]], "a:int,b:int")
                    df2 = dag.df([[0, 1]], "a:int,b:int")
                    dag.output([df1, df2], using=o)

            # input_has: a
            # partitionby_has: b
            def o4(dfs: DataFrames) -> None:
                pass

            # compile time
            with raises(FugueWorkflowCompileValidationError):
                dag = self.dag()
                df = dag.df([[0, 1]], "a:int,b:int")
                df.output(o4)

            with self.dag() as dag:
                dag.df([[0, 1]], "a:int,b:int").partition(by=["b"]).output(o4)

        def test_callback(self):  # noqa: C901
            class Callbacks(object):
                def __init__(self):
                    self.n = 0

                def call(self, value: int) -> int:
                    self.n += value
                    return self.n

            cb = Callbacks()

            class CallbackTransformer(Transformer):
                def get_output_schema(self, df):
                    return df.schema

                def transform(self, df):
                    has = self.params.get_or_throw("has", bool)
                    v = self.cursor.key_value_array[0]
                    assert self.has_callback == has
                    if self.has_callback:
                        print(self.callback(v))
                    return df

            with self.dag() as dag:
                df = dag.df([[1, 1], [1, 2], [2, 3], [5, 6]], "a:int,b:int")
                res = df.partition(by=["a"]).transform(
                    CallbackTransformer, callback=cb.call, params=dict(has=True)
                )
                df.assert_eq(res)
                res = df.partition(by=["a"]).transform(
                    CallbackTransformer, params=dict(has=False)
                )
                df.assert_eq(res)

            assert 8 == cb.n

            # interfaceless tests
            cb2 = Callbacks()

            # schema: *
            def t0(df: pd.DataFrame) -> pd.DataFrame:
                return df

            # schema: *
            def t1(df: pd.DataFrame, c: Callable[[int], int]) -> pd.DataFrame:
                c(1)
                return df

            # schema: *
            def t12(
                df: pd.DataFrame, c: Optional[Callable[[int], int]] = None
            ) -> pd.DataFrame:
                if c is not None:
                    c(1)
                return df

            def t2(df: pd.DataFrame, c: Callable[[int], int]) -> None:
                c(1)

            with self.dag() as dag:
                df = dag.df([[1, 1], [1, 2], [2, 3], [5, 6]], "a:int,b:int")
                df.partition(by=["a"]).transform(
                    t0, callback=cb2.call
                ).persist()  # no effect because t0 ignores callback
                res = df.partition(by=["a"]).transform(t1, callback=cb2.call)  # +3
                df.partition(by=["a"]).out_transform(t2, callback=cb2.call)  # +3
                df.partition(by=["a"]).out_transform(t12, callback=cb2.call)  # +3
                df.partition(by=["a"]).out_transform(t12)
                raises(
                    FugueInterfacelessError,
                    lambda: (df.partition(by=["a"]).out_transform(t1)),
                )  # for t1, callback must be provided
                df.assert_eq(res)

            assert 9 == cb2.n

            cb2 = Callbacks()

            # schema: a:int,b:int
            def t3(
                df1: pd.DataFrame, df2: pd.DataFrame, c: Callable[[int], int]
            ) -> pd.DataFrame:
                c(1)
                return df1

            def t4(
                df1: pd.DataFrame, df2: pd.DataFrame, c: Callable[[int], int]
            ) -> None:
                c(1)

            def t42(
                df1: pd.DataFrame, df2: pd.DataFrame, c: Optional[Callable[[int], int]]
            ) -> None:
                if c is not None:
                    c(1)

            with self.dag() as dag:
                df1 = dag.df([[1, 1], [1, 2], [2, 3], [5, 6]], "a:int,b:int")
                df2 = dag.df([[1, 1], [1, 2], [2, 3], [5, 6]], "a:int,c:int")
                res = df1.zip(df2).transform(t3, callback=cb2.call).persist()  # +3
                df1.zip(df2).out_transform(t4, callback=cb2.call)  # +3
                df1.zip(df2).out_transform(t42, callback=cb2.call)  # +3
                df1.zip(df2).out_transform(t42)
                raises(
                    FugueInterfacelessError,
                    lambda: df1.zip(df2).transform(t3).persist(),
                )  # for t3, callback must be provided

            assert 9 == cb2.n


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


@transformer(lambda df, **kwargs: df.schema + (kwargs.get("col", "p") + ":int"))
def mock_tf0(df: pd.DataFrame, p=1, col="p") -> pd.DataFrame:
    df[col] = p
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


# schema: *
def mock_tf3(df: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    # binary data test
    for row in df:
        obj = pickle.loads(row["b"])
        obj[0] += 1
        obj[1] += "x"
        row["b"] = pickle.dumps(obj)
        yield row


class MockCoTransform1(CoTransformer):
    def get_output_schema(self, dfs: DataFrames) -> Any:
        assert "test" in self.workflow_conf
        assert 2 == len(dfs)
        if self.params.get("named", False):
            assert dfs.has_key
        else:
            assert not dfs.has_key
        return [self.key_schema, "ct1:int,ct2:int,p:int"]

    def on_init(self, dfs: DataFrames) -> None:
        assert "test" in self.workflow_conf
        assert 2 == len(dfs)
        if self.params.get("named", False):
            assert dfs.has_key
        else:
            assert not dfs.has_key
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
        if self.params.get("named", False):
            assert dfs.has_key
        else:
            assert not dfs.has_key
        row = self.cursor.key_value_array + [
            dfs[0].count(),
            dfs[1].count(),
            self.params.get("p", 1),
        ]
        return ArrayDataFrame([row], self.output_schema)


@cotransformer(
    lambda dfs, **kwargs: "a:int,ct1:int,ct2:int," + kwargs.get("col", "p") + ":int"
)
def mock_co_tf1(
    df1: List[Dict[str, Any]], df2: List[List[Any]], p=1, col="p"
) -> List[List[Any]]:
    return [[df1[0]["a"], len(df1), len(df2), p]]


def mock_co_tf2(dfs: DataFrames, p=1) -> List[List[Any]]:
    return [[dfs[0].peek_dict()["a"], dfs[0].count(), dfs[1].count(), p]]


@cotransformer(Schema("a:int,ct1:int,p:int"))
def mock_co_tf3(df1: List[Dict[str, Any]], p=1) -> List[List[Any]]:
    return [[df1[0]["a"], len(df1), p]]


@cotransformer("a:int,ct1:int,p:int")
def mock_co_tf4_ex(df1: List[Dict[str, Any]], p=1) -> List[List[Any]]:
    k = df1[0]["a"]
    if k == 2:
        raise NotImplementedError
    return [[df1[0]["a"], len(df1), p]]
