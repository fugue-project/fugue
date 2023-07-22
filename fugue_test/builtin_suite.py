# pylint: disable-all
try:
    import qpd_pandas  # noqa: F401

    HAS_QPD = True
except ImportError:  # pragma: no cover
    HAS_QPD = False

import datetime
import os
import pickle
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional
from unittest import TestCase
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pytest import raises
from triad import SerializableRLock

import fugue.api as fa
from fugue import (
    AnyDataFrame,
    ArrayDataFrame,
    CoTransformer,
    DataFrame,
    DataFrames,
    ExecutionEngine,
    FileSystem,
    FugueWorkflow,
    LocalDataFrame,
    OutputCoTransformer,
    Outputter,
    OutputTransformer,
    PandasDataFrame,
    PartitionSpec,
    Processor,
    QPDPandasEngine,
    Schema,
    Transformer,
    cotransformer,
    output_cotransformer,
    output_transformer,
    outputter,
    processor,
    register_creator,
    register_default_sql_engine,
    register_output_transformer,
    register_outputter,
    register_processor,
    register_transformer,
    transformer,
)
from fugue.column import col
from fugue.column import functions as ff
from fugue.column import lit
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import (
    FugueInterfacelessError,
    FugueWorkflowCompileError,
    FugueWorkflowCompileValidationError,
    FugueWorkflowError,
    FugueWorkflowRuntimeValidationError,
)


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
            register_default_sql_engine(lambda engine: engine.sql_engine)
            cls._engine = cls.make_engine(cls)

        @property
        def engine(self) -> ExecutionEngine:
            return self._engine  # type: ignore

        @classmethod
        def tearDownClass(cls):
            cls._engine.stop()

        def make_engine(self) -> ExecutionEngine:  # pragma: no cover
            raise NotImplementedError

        def test_workflows(self):
            a = FugueWorkflow().df([[0]], "a:int")
            df_eq(a.compute(self.engine), [[0]], "a:int")

        def test_create_show(self):
            with FugueWorkflow() as dag:
                dag.df([[0]], "a:int").persist().partition(num=2).show()
                dag.df(dag.df([[0]], "a:int")).persist().broadcast().show(title="t")
            dag.run(self.engine)

        def test_create_df_equivalence(self):
            ndf = fa.as_fugue_engine_df(self.engine, pd.DataFrame([[0]], columns=["a"]))
            dag1 = FugueWorkflow()
            dag1.df(ndf).show()
            dag2 = FugueWorkflow()
            dag2.create(ndf).show()
            assert dag1.spec_uuid() == dag2.spec_uuid()

        def test_checkpoint(self):
            with raises(FugueWorkflowError):
                with FugueWorkflow() as dag:
                    dag.df([[0]], "a:int").checkpoint()
                dag.run(self.engine)

            self.engine.conf["fugue.workflow.checkpoint.path"] = os.path.join(
                self.tmpdir, "ck"
            )
            with FugueWorkflow() as dag:
                a = dag.df([[0]], "a:int").checkpoint()
                dag.df([[0]], "a:int").assert_eq(a)
            dag.run(self.engine)

        def test_deterministic_checkpoint(self):
            self.engine.conf["fugue.workflow.checkpoint.path"] = os.path.join(
                self.tmpdir, "ck"
            )
            temp_file = os.path.join(self.tmpdir, "t.parquet")

            def mock_create(dummy: int = 1) -> pd.DataFrame:
                return pd.DataFrame(np.random.rand(3, 2), columns=["a", "b"])

            # base case without checkpoint, two runs should generate different result
            with FugueWorkflow() as dag:
                a = dag.create(mock_create)
                a.save(temp_file)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                a = dag.create(mock_create)
                b = dag.load(temp_file)
                b.assert_not_eq(a)
            dag.run(self.engine)

            # strong checkpoint is not cross execution, so result should be different
            with FugueWorkflow() as dag:
                a = dag.create(mock_create).strong_checkpoint()
                a.save(temp_file)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                a = dag.create(mock_create).strong_checkpoint()
                b = dag.load(temp_file)
                b.assert_not_eq(a)
            dag.run(self.engine)

            # with deterministic checkpoint, two runs should generate the same result
            with FugueWorkflow() as dag:
                dag.create(
                    mock_create, params=dict(dummy=2)
                )  # this should not affect a because it's not a dependency
                a = dag.create(mock_create).deterministic_checkpoint()
                id1 = a.spec_uuid()
                a.save(temp_file)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                a = dag.create(mock_create).deterministic_checkpoint()
                b = dag.load(temp_file)
                b.assert_eq(a)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                # checkpoint specs itself doesn't change determinism
                # of the previous steps
                a = dag.create(mock_create).deterministic_checkpoint(
                    partition=PartitionSpec(num=2)
                )
                id2 = a.spec_uuid()
                b = dag.load(temp_file)
                b.assert_eq(a)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                # dependency change will affect determinism
                a = dag.create(
                    mock_create, params={"dummy": 2}
                ).deterministic_checkpoint()
                id3 = a.spec_uuid()
                b = dag.load(temp_file)
                b.assert_not_eq(a)
            dag.run(self.engine)

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
            with FugueWorkflow() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b)
                c.save(temp_file)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b)
                d = dag.load(temp_file)
                d.assert_not_eq(c)
            dag.run(self.engine)

            # strong checkpoint is not cross execution, so result should be different
            with FugueWorkflow() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).strong_checkpoint()
                c.save(temp_file)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).strong_checkpoint()
                d = dag.load(temp_file)
                d.assert_not_eq(c)
            dag.run(self.engine)

            # with deterministic checkpoint, two runs should generate the same result
            with FugueWorkflow() as dag:
                dag.create(
                    mock_create, params=dict(dummy=2)
                )  # this should not affect a because it's not a dependency
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b)
                c.deterministic_checkpoint()
                c.save(temp_file)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).deterministic_checkpoint()
                d = dag.load(temp_file)
                d.assert_eq(c)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                # checkpoint specs itself doesn't change determinism
                # of the previous steps
                a = dag.create(mock_create).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).deterministic_checkpoint(partition=PartitionSpec(num=2))
                d = dag.load(temp_file)
                d.assert_eq(c)
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                # dependency change will affect determinism
                a = dag.create(mock_create, params={"dummy": 2}).drop(["a"])
                b = dag.create(mock_create).drop(["a"])
                c = a.union(b).deterministic_checkpoint(partition=PartitionSpec(num=2))
                d = dag.load(temp_file)
                d.assert_not_eq(c)
            dag.run(self.engine)

        def test_yield_file(self):
            self.engine.conf["fugue.workflow.checkpoint.path"] = os.path.join(
                self.tmpdir, "ck"
            )

            with raises(FugueWorkflowCompileError):
                FugueWorkflow().df([[0]], "a:int").checkpoint().yield_file_as("x")

            with raises(ValueError):
                FugueWorkflow().df([[0]], "a:int").persist().yield_file_as("x")

            def run_test(deterministic):
                dag1 = FugueWorkflow()
                df = dag1.df([[0]], "a:int")
                if deterministic:
                    df = df.deterministic_checkpoint(storage_type="file")
                df.yield_file_as("x")
                id1 = dag1.spec_uuid()
                dag2 = FugueWorkflow()
                dag2.df([[0]], "a:int").assert_eq(dag2.df(dag1.yields["x"]))
                id2 = dag2.spec_uuid()
                dag1.run(self.engine)
                dag2.run(self.engine)
                return id1, id2

            id1, id2 = run_test(False)
            id3, id4 = run_test(False)
            assert id1 == id3
            assert id2 != id4  # non deterministic yield (direct yield)

            id1, id2 = run_test(True)
            id3, id4 = run_test(True)
            assert id1 == id3
            assert id2 == id4  # deterministic yield (yield deterministic checkpoint)

        def test_yield_table(self):
            with raises(FugueWorkflowCompileError):
                FugueWorkflow().df([[0]], "a:int").checkpoint().yield_table_as("x")

            with raises(ValueError):
                FugueWorkflow().df([[0]], "a:int").persist().yield_table_as("x")

            def run_test(deterministic):
                dag1 = FugueWorkflow()
                df = dag1.df([[0]], "a:int")
                if deterministic:
                    df = df.deterministic_checkpoint(storage_type="table")
                df.yield_table_as("x")
                id1 = dag1.spec_uuid()
                dag2 = FugueWorkflow()
                dag2.df([[0]], "a:int").assert_eq(dag2.df(dag1.yields["x"]))
                id2 = dag2.spec_uuid()
                dag1.run(self.engine)
                dag2.run(self.engine)
                return id1, id2

            id1, id2 = run_test(False)
            id3, id4 = run_test(False)
            assert id1 == id3
            assert id2 != id4  # non deterministic yield (direct yield)

            id1, id2 = run_test(True)
            id3, id4 = run_test(True)
            assert id1 == id3
            assert id2 == id4  # deterministic yield (yield deterministic checkpoint)

        def test_yield_dataframe(self):
            dag = FugueWorkflow()
            df1 = dag.df([[1]], "a:int")
            df2 = dag.df([[1]], "a:int")
            df1.union(df2, distinct=False).yield_dataframe_as("x")
            df1.union(df2, distinct=False).yield_dataframe_as("y", as_local=True)
            result = dag.run()["x"]
            assert [[1], [1]] == result.as_array()
            result = dag.run()["y"]
            assert [[1], [1]] == result.as_array()
            assert result.is_local

        def test_create_process_output(self):
            with FugueWorkflow() as dag:
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

                a = dag.create(mock_creator2, params=dict(p=2))
                b = dag.create(mock_creator2, params=dict(p=2))
                c = dag.process(a, b, using=mock_processor4)
                c.assert_eq(ArrayDataFrame([[2]], "a:int"))
                dag.output(a, b, using=mock_outputter4)
            dag.run(self.engine)

        def test_zip(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [2, 3], [2, 5]], "a:int,b:int")
                b = dag.df([[1, 3]], "a:int,c:int")
                c1 = a.zip(b)
                c2 = dag.zip(a, b)
                c1.assert_eq(c2)

                a = dag.df([[1, 2], [2, 3], [2, 5]], "a:int,b:int")
                b = dag.df([[1, 3]], "a:int,c:int")
                c1 = a.zip(b, how="left_outer", partition=dict(presort="b DESC, c ASC"))
                c2 = dag.zip(
                    a, b, how="left_outer", partition=dict(presort="b DESC, c ASC")
                )
                c1.assert_eq(c2)

                a = dag.df([[1, 2, 0], [1, 3, 1]], "a:int,b:int,c:int")
                b = dag.df([[1, 2, 1], [1, 3, 2]], "a:int,b:int,d:int")
                c = dag.df([[1, 4]], "a:int,e:int")
                e = dag.df([[1, 2], [1, 3]], "a:int,b:int")
                dag.zip(a, b, c)[["a", "b"]].assert_eq(e)
            dag.run(self.engine)

        def test_transform(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [3, 4]], "a:double,b:int")
                c = a.transform(mock_tf0)
                dag.df([[1, 2, 1], [3, 4, 1]], "a:double,b:int,p:int").assert_eq(c)

                c = a.transform(mock_tf0, params=dict(col="x"))
                dag.df([[1, 2, 1], [3, 4, 1]], "a:double,b:int,x:int").assert_eq(c)

                a = dag.df([[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int")
                c = a.transform(mock_tf0, params=dict(p="10"))
                dag.df(
                    [[1, 2, 10], [None, 1, 10], [3, 4, 10], [None, 4, 10]],
                    "a:double,b:int,p:int",
                ).assert_eq(c)
            dag.run(self.engine)

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
                    dag_.run()

            m = _Mock()
            m.test()
            with FugueWorkflow() as dag:
                a = dag.df([[0], [1]], "a:int")
                b = a.transform(m.t1).transform(m.t2, schema="*")
                b.assert_eq(a)
            dag.run(self.engine)

        def test_transform_iterable_dfs(self):
            # this test is important for using mapInPandas in spark

            # schema: *,c:int
            def mt_pandas(
                dfs: Iterable[pd.DataFrame], empty: bool = False
            ) -> Iterator[pd.DataFrame]:
                for df in dfs:
                    if not empty:
                        df = df.assign(c=2)
                        df = df[reversed(list(df.columns))]
                        yield df

            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [3, 4]], "a:int,b:int")
                b = a.transform(mt_pandas)
                dag.df([[1, 2, 2], [3, 4, 2]], "a:int,b:int,c:int").assert_eq(b)
            dag.run(self.engine)

            # when iterable returns nothing
            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [3, 4]], "a:int,b:int")
                # without partitioning
                b = a.transform(mt_pandas, params=dict(empty=True))
                dag.df([], "a:int,b:int,c:int").assert_eq(b)
                # with partitioning
                b = a.partition_by("a").transform(mt_pandas, params=dict(empty=True))
                dag.df([], "a:int,b:int,c:int").assert_eq(b)
            dag.run(self.engine)

            # schema: *
            def mt_arrow(
                dfs: Iterable[pa.Table], empty: bool = False
            ) -> Iterator[pa.Table]:
                for df in dfs:
                    if not empty:
                        df = df.select(reversed(df.schema.names))
                        yield df

            # schema: a:long
            def mt_arrow_2(dfs: Iterable[pa.Table]) -> Iterator[pa.Table]:
                for df in dfs:
                    yield df.drop(["b"])

            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [3, 4]], "a:int,b:int")
                b = a.transform(mt_arrow)
                dag.df([[1, 2], [3, 4]], "a:int,b:int").assert_eq(b)
                b = a.transform(mt_arrow_2)
                dag.df([[1], [3]], "a:long").assert_eq(b)
            dag.run(self.engine)

            # when iterable returns nothing
            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [3, 4]], "a:int,b:int")
                # without partitioning
                b = a.transform(mt_arrow, params=dict(empty=True))
                dag.df([], "a:int,b:int").assert_eq(b)
                # with partitioning
                b = a.partition_by("a").transform(mt_arrow, params=dict(empty=True))
                dag.df([], "a:int,b:int").assert_eq(b)
            dag.run(self.engine)

        def test_transform_binary(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, pickle.dumps([0, "a"])]], "a:int,b:bytes")
                c = a.transform(mock_tf3).persist()
                b = dag.df([[1, pickle.dumps([1, "ax"])]], "a:int,b:bytes")
                b.assert_eq(c, check_order=True)
            dag.run(self.engine)

        def test_transform_by(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int")
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

                c = a.partition(by="a", presort="b DESC").transform(
                    mock_tf2_except, schema="*", ignore_errors=[NotImplementedError]
                )
                dag.df([[1, 2], [3, 4]], "a:double,b:int").assert_eq(c)
            dag.run(self.engine)

        def test_cotransform(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [1, 3], [2, 1]], "a:int,b:int")
                b = dag.df([[1, 2], [3, 4]], "a:int,c:int")
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
                c = a.partition_by("a").zip().transform(mock_co_tf3)
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
            dag.run(self.engine)

        def test_cotransform_with_key(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [1, 3], [2, 1]], "a:int,b:int")
                b = dag.df([[1, 2], [3, 4]], "a:int,c:int")
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
            dag.run(self.engine)

        def test_out_transform(self):  # noqa: C901
            tmpdir = str(self.tmpdir)

            def incr():
                fs = FileSystem(auto_close=False).makedirs(tmpdir, recreate=True)
                fs.writetext(str(uuid4()) + ".txt", "")
                return fs.glob("*.txt").count().files

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

            def t10(df: pd.DataFrame) -> Iterable[pa.Table]:
                incr()
                yield pa.Table.from_pandas(df)

            with FugueWorkflow() as dag:
                a = dag.df([[1, 2], [3, 4]], "a:double,b:int")
                a.out_transform(t1)  # +2
                a.partition_by("b").out_transform(t2)  # +1 or +2
                a.partition_by("b").out_transform(t3)  # +1 or +2
                a.partition_by("b").out_transform(t4)  # +2
                a.partition_by("b").out_transform(t5)  # +2
                a.out_transform(T6)  # +1
                a.partition_by("b").out_transform(T7)  # +1
                a.out_transform(t8, ignore_errors=[NotImplementedError])  # +1
                a.out_transform(t9)  # +1
                a.out_transform(t10)  # +1
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(t2))
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(t3))
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(t4))
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(t5))
                raises(FugueWorkflowCompileValidationError, lambda: a.out_transform(T7))
            dag.run(self.engine)

            assert 13 <= incr()

        def test_out_cotransform(self):  # noqa: C901
            tmpdir = str(self.tmpdir)

            def incr():
                fs = FileSystem(auto_close=False).makedirs(tmpdir, recreate=True)
                fs.writetext(str(uuid4()) + ".txt", "")
                return fs.glob("*.tx" "t").count().files

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

            with FugueWorkflow() as dag:
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
            dag.run(self.engine)

            assert 12 <= incr()

        def test_join(self):
            with FugueWorkflow() as dag:
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
                d.show()
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
            dag.run(self.engine)

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_df_select(self):
            with FugueWorkflow() as dag:
                # wildcard
                a = dag.df([[1, 10], [2, 20], [3, 30]], "x:int,y:int")
                a.select("*").assert_eq(a)

                # column transformation
                b = dag.df(
                    [[1, 10, 11, "x"], [2, 20, 22, "x"], [3, 30, 33, "x"]],
                    "x:int,y:int,c:int,d:str",
                )
                a.select(
                    "*",
                    (col("x") + col("y")).cast("int32").alias("c"),
                    lit("x", "d"),
                ).assert_eq(b)

                # distinct
                a = dag.df([[1, 10], [2, 20], [1, 10]], "x:int,y:int")
                b = dag.df([[1, 10], [2, 20]], "x:int,y:int")
                a.select("*", distinct=True).assert_eq(b)

                # aggregation plus infer alias
                a = dag.df([[1, 10], [1, 20], [3, 30]], "x:int,y:int")
                b = dag.df([[1, 30], [3, 30]], "x:int,y:int")
                a.select("x", ff.sum(col("y")).cast("int32")).assert_eq(b)

                # all together
                a = dag.df([[1, 10], [1, 20], [3, 35], [3, 40]], "x:int,y:int")
                b = dag.df([[3, 35]], "x:int,z:int")
                a.select(
                    "x",
                    ff.sum(col("y")).alias("z").cast("int32"),
                    where=col("y") < 40,
                    having=ff.sum(col("y")) > 30,
                ).assert_eq(b)

                b = dag.df([[65]], "z:long")
                a.select(
                    ff.sum(col("y")).alias("z").cast(int), where=col("y") < 40
                ).show()

                raises(ValueError, lambda: a.select("*", "x"))
            dag.run(self.engine)

            with FugueWorkflow() as dag:
                a = dag.df([[0]], "a:long")
                b = dag.df([[0]], "a:long")
                dag.select("select * from", a).assert_eq(b)
            dag.run(self.engine, {"fugue.sql.compile.ignore_case": True})

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_df_filter(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 10], [2, 20], [3, 30]], "x:int,y:int")
                b = dag.df([[2, 20]], "x:int,y:int")
                a.filter((col("y") > 15) & (col("y") < 25)).assert_eq(b)
            dag.run(self.engine)

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_df_assign(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 10], [2, 20], [3, 30]], "x:int,y:int")
                b = dag.df([[1, "x"], [2, "x"], [3, "x"]], "x:int,y:str")
                a.assign(y="x").assert_eq(b)

                a = dag.df([[1, 10], [2, 20], [3, 30]], "x:int,y:int")
                b = dag.df(
                    [[1, "x", 11], [2, "x", 21], [3, "x", 31]], "x:int,y:str,z:double"
                )
                a.assign(lit("x").alias("y"), z=(col("y") + 1).cast(float)).assert_eq(b)
            dag.run(self.engine)

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_aggregate(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 10], [1, 200], [3, 30]], "x:int,y:int")
                b = dag.df([[1, 200], [3, 30]], "x:int,y:int")
                c = dag.df([[-200, 200, 70]], "y:int,zz:int,ww:int")
                a.partition_by("x").aggregate(ff.max(col("y"))).assert_eq(b)
                a.aggregate(
                    ff.min(-col("y")),
                    zz=ff.max(col("y")),
                    ww=((ff.min(col("y")) + ff.max(col("y"))) / 3).cast("int32"),
                ).assert_eq(c)
            dag.run(self.engine)

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_select(self):
            class MockEngine(QPDPandasEngine):
                def __init__(self, execution_engine, p: int = 0):
                    super().__init__(execution_engine)
                    self.p = p

                def select(self, dfs, statement):
                    assert 2 == self.p  # assert set p value is working
                    return super().select(dfs, statement)

            with FugueWorkflow() as dag:
                a = dag.df([[1, 10], [2, 20], [3, 30]], "x:long,y:long")
                b = dag.df([[2, 20, 40], [3, 30, 90]], "x:long,y:long,z:long")
                dag.select("* FROM", a).assert_eq(a)
                dag.select(a, ".* FROM", a).assert_eq(a)
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
                    sql_engine="qpdpandas",
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
                dag.select("'test' AS a").assert_eq(dag.df([["test"]], "a:str"))

                # make sure transform -> select works
                b = a.transform(mock_tf1)
                a = a.transform(mock_tf1)
                aa = dag.select("* FROM", a)
                dag.select("* FROM", b).assert_eq(aa)
            dag.run(self.engine)

        def test_union(self):
            with FugueWorkflow() as dag:
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
            dag.run(self.engine)

        def test_intersect(self):
            with FugueWorkflow() as dag:
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
            dag.run(self.engine)

        def test_subtract(self):
            with FugueWorkflow() as dag:
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
            dag.run(self.engine)

        def test_distinct(self):
            with FugueWorkflow() as dag:
                a = dag.df([[1, 10], [2, None], [2, None]], "x:long,y:double")
                a.distinct().assert_eq(
                    ArrayDataFrame(
                        [[1, 10], [2, None]],
                        "x:long,y:double",
                    )
                )
            dag.run(self.engine)

        def test_dropna(self):
            with FugueWorkflow() as dag:
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
            dag.run(self.engine)

        def test_fillna(self):
            with FugueWorkflow() as dag:
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
            dag.run(self.engine)
            with raises(FugueWorkflowError):
                with FugueWorkflow() as dag:
                    dag.df([[None, 1]], "a:int,b:int").fillna({"a": None, "b": 1})
                dag.run(self.engine)

            with raises(FugueWorkflowError):
                with FugueWorkflow() as dag:
                    dag.df([[None, 1]], "a:int,b:int").fillna(None)
                dag.run(self.engine)

        def test_sample(self):
            with FugueWorkflow() as dag:
                a = dag.df(
                    [[1, 10, 10], [None, 2, None], [2, None, 4]],
                    "x:double,y:double,z:double",
                )
                a.sample(frac=0.5, replace=False, seed=0).show()
            dag.run(self.engine)

            with FugueWorkflow() as dag:
                with raises(ValueError):
                    dag.df([[None, 1]], "a:int,b:int").sample(n=1, frac=0.2)
            dag.run(self.engine)

        def test_take(self):
            # Test for presort parsing
            with FugueWorkflow() as dag:
                df = dag.df(
                    pd.DataFrame({"aaa": [1, 1, 2, 2], "bbb": ["a", "b", "c", "d"]})
                )
                df = df.partition(by=["aaa"], presort="bbb").take(1)
                df.show()
            dag.run(self.engine)
            # Partition but no presort. Output not deterministic
            with FugueWorkflow() as dag:
                df = dag.df(
                    pd.DataFrame({"aaa": [1, 1, 2, 2], "bbb": ["a", "b", "c", "d"]})
                )
                df = df.partition(by=["aaa"]).take(1)
                df.show()
            dag.run(self.engine)
            # Partition by and presort with NULLs
            # Column c needs to be kept even if not in presort or partition
            with FugueWorkflow() as dag:
                a = dag.df(
                    [
                        ["0", 1, 1],
                        ["0", 2, 1],
                        ["1", 3, 1],
                        ["1", 4, 1],
                        [None, 2, 1],
                        [None, 3, 1],
                    ],
                    "a:str,b:int,c:long",
                )
                a.partition(by=["a"], presort="b desc").take(n=1).assert_eq(
                    ArrayDataFrame(
                        [["0", 2, 1], ["1", 4, 1], [None, 3, 1]],
                        "a:str,b:int,c:long",
                    )
                )
            dag.run(self.engine)
            # No partition
            with FugueWorkflow() as dag:
                a = dag.df([[0, 1], [0, 2], [1, 3]], "a:int,b:int")
                a.take(1, presort="a,b desc").assert_eq(
                    ArrayDataFrame([[0, 2]], "a:int,b:int")
                )
            dag.run(self.engine)
            # take presort overrides partition presort
            with FugueWorkflow() as dag:
                a = dag.df(
                    [["0", 1], ["0", 2], ["1", 3], ["1", 4], [None, 2], [None, 3]],
                    "a:str,b:double",
                )
                a.partition(by=["a"], presort="b desc").take(
                    n=1, presort="b asc"
                ).assert_eq(
                    ArrayDataFrame([["0", 1], ["1", 3], [None, 2]], "a:str,b:double")
                )
            dag.run(self.engine)
            # order by with NULL first
            with FugueWorkflow() as dag:
                a = dag.df([["0", 1], ["0", 2], [None, 3]], "a:str,b:double")
                a.take(1, presort="a desc", na_position="first").assert_eq(
                    ArrayDataFrame([[None, 3]], "a:str,b:double")
                )
            dag.run(self.engine)
            # order by with NULL last
            with FugueWorkflow() as dag:
                a = dag.df([["0", 1], ["1", 2], [None, 3]], "a:str,b:double")
                a.take(1, presort="a desc", na_position="last").assert_eq(
                    ArrayDataFrame([["1", 2]], "a:str,b:double")
                )
            dag.run(self.engine)
            # Return any row because no presort
            with FugueWorkflow() as dag:
                a = dag.df([[0, 1], [0, 2], [1, 3]], "a:int,b:int")
                a.take(1).show()
            dag.run(self.engine)
            # n must be int
            with FugueWorkflow() as dag:
                with raises(ValueError):
                    a = dag.df([[0, 1], [0, 2], [1, 3]], "a:int,b:int")
                    a.take(0.5).show()
            dag.run(self.engine)
            # na_position not valid
            with FugueWorkflow() as dag:
                with raises(ValueError):
                    a = dag.df([[0, 1], [0, 2], [1, 3]], "a:int,b:int")
                    a.take(1, na_position=["True", "False"]).show()
            dag.run(self.engine)
            # Multiple takes
            with FugueWorkflow() as dag:
                df = dag.df(
                    pd.DataFrame({"aaa": [1, 1, 2, 2], "bbb": ["a", "b", "c", "d"]})
                )
                df1 = (
                    df.partition(by=["aaa"], presort="bbb")
                    .take(1)
                    .take(1, presort="aaa")
                )
                df1.show()
                df2 = (
                    df.partition(by=["aaa"], presort="bbb")
                    .take(1)
                    .partition(by=["aaa"], presort="bbb")
                    .take(1, presort="aaa")
                )
                df2.show()
            dag.run(self.engine)

        def test_col_ops(self):
            with FugueWorkflow() as dag:
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
            dag.run(self.engine)

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

            with FugueWorkflow() as dag:
                a = dag.df([["2020-01-01"]], "a:date").transform(t1)
                b = dag.df(
                    [[datetime.date(2020, 1, 1), datetime.datetime(2020, 1, 2)]],
                    "a:date,b:datetime",
                )
                b.assert_eq(a)
                c = dag.df([["2020-01-01", "2020-01-01 00:00:00"]], "a:date,b:datetime")
                c.transform(T2).assert_eq(c)
                c.partition(by=["a"]).transform(T2).assert_eq(c)
            dag.run(self.engine)

        @pytest.fixture(autouse=True)
        def init_tmpdir(self, tmpdir):
            self.tmpdir = tmpdir

        def test_io(self):
            path = os.path.join(self.tmpdir, "a")
            path2 = os.path.join(self.tmpdir, "b.test.csv")
            path3 = os.path.join(self.tmpdir, "c.partition")
            with FugueWorkflow() as dag:
                b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
                b.partition(num=3).save(path, fmt="parquet", single=True)
                b.save(path2, header=True)
            dag.run(self.engine)
            assert FileSystem().isfile(path)
            with FugueWorkflow() as dag:
                a = dag.load(path, fmt="parquet", columns=["a", "c"])
                a.assert_eq(dag.df([[1, 6], [7, 2]], "a:long,c:int"))
                a = dag.load(path2, header=True, columns="c:int,a:long")
                a.assert_eq(dag.df([[6, 1], [2, 7]], "c:int,a:long"))
            dag.run(self.engine)
            with FugueWorkflow() as dag:
                b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
                b.partition(by="c").save(path3, fmt="parquet", single=False)
            dag.run(self.engine)
            assert FileSystem().isdir(path3)
            assert FileSystem().isdir(os.path.join(path3, "c=6"))
            assert FileSystem().isdir(os.path.join(path3, "c=2"))
            # TODO: in test below, once issue #288 is fixed, use dag.load
            #  instead of pd.read_parquet
            pdf = pd.read_parquet(path3).sort_values("a").reset_index(drop=True)
            pdf["c"] = pdf["c"].astype(int)
            pd.testing.assert_frame_equal(
                pdf,
                pd.DataFrame({"c": [6, 2], "a": [1, 7]}).reset_index(drop=True),
                check_like=True,
                check_dtype=False,
            )

        def test_save_and_use(self):
            path = os.path.join(self.tmpdir, "a")
            with FugueWorkflow() as dag:
                b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
                c = b.save_and_use(path, fmt="parquet")
                b.assert_eq(c)
            dag.run(self.engine)

            with FugueWorkflow() as dag:
                b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
                d = dag.load(path, fmt="parquet")
                b.assert_eq(d)
            dag.run(self.engine)

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
                    FugueWorkflow().df([[0, 1]], "a:int,b:int").transform(t)

                # runtime
                with raises(FugueWorkflowRuntimeValidationError):
                    with FugueWorkflow() as dag:
                        dag.df([[0, 1]], "c:int,b:int").partition(by=["b"]).transform(t)
                    dag.run(self.engine)

                with FugueWorkflow() as dag:
                    dag.df([[0, 1]], "a:int,b:int").partition(by=["b"]).transform(
                        t
                    ).assert_eq(dag.df([[0, 1]], "a:int,b:int"))
                dag.run(self.engine)

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
                    with FugueWorkflow() as dag:
                        df1 = dag.df([[0, 1]], "a:int,b:int")
                        df2 = dag.df([[0, 1]], "c:int,d:int")
                        dag.process([df1, df2], using=p)
                    dag.run(self.engine)

                with FugueWorkflow() as dag:
                    df1 = dag.df([[0, 1]], "a:int,b:int")
                    df2 = dag.df([[0, 1]], "a:int,b:int")
                    dag.process([df1, df2], using=p).assert_eq(df1)
                dag.run(self.engine)

            # input_has: a
            # partitionby_has: b
            def p4(dfs: DataFrames) -> DataFrame:
                return dfs[0]

            # compile time
            with raises(FugueWorkflowCompileValidationError):
                dag = FugueWorkflow()
                df = dag.df([[0, 1]], "a:int,b:int")
                df.process(p4)

            with FugueWorkflow() as dag:
                dag.df([[0, 1]], "a:int,b:int").partition(by=["b"]).process(p4)
            dag.run(self.engine)

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
                    with FugueWorkflow() as dag:
                        df1 = dag.df([[0, 1]], "a:int,b:int")
                        df2 = dag.df([[0, 1]], "c:int,d:int")
                        dag.output([df1, df2], using=o)
                    dag.run(self.engine)

                with FugueWorkflow() as dag:
                    df1 = dag.df([[0, 1]], "a:int,b:int")
                    df2 = dag.df([[0, 1]], "a:int,b:int")
                    dag.output([df1, df2], using=o)
                dag.run(self.engine)

            # input_has: a
            # partitionby_has: b
            def o4(dfs: DataFrames) -> None:
                pass

            # compile time
            with raises(FugueWorkflowCompileValidationError):
                dag = FugueWorkflow()
                df = dag.df([[0, 1]], "a:int,b:int")
                df.output(o4)

            with FugueWorkflow() as dag:
                dag.df([[0, 1]], "a:int,b:int").partition(by=["b"]).output(o4)
            dag.run(self.engine)

        def test_extension_registry(self):
            def my_creator() -> pd.DataFrame:
                return pd.DataFrame([[0, 1], [1, 2]], columns=["a", "b"])

            def my_processor(df: pd.DataFrame) -> pd.DataFrame:
                return df

            # schema: *
            def my_transformer(df: pd.DataFrame) -> pd.DataFrame:
                return df

            def my_out_transformer(df: pd.DataFrame) -> None:
                print(df)

            def my_outputter(df: pd.DataFrame) -> None:
                print(df)

            register_creator("mc", my_creator)
            register_processor("mp", my_processor)
            register_transformer("mt", my_transformer)
            register_output_transformer("mot", my_out_transformer)
            register_outputter("mo", my_outputter)

            with FugueWorkflow() as dag:
                df = dag.create("mc").process("mp").transform("mt")
                df.out_transform("mot")
                df.output("mo")
            dag.run(self.engine)

        def test_callback(self):  # noqa: C901
            class Callbacks(object):
                def __init__(self):
                    self.n = 0
                    self._lock = SerializableRLock()

                def call(self, value: int) -> int:
                    with self._lock:
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

            with FugueWorkflow() as dag:
                df = dag.df([[1, 1], [1, 2], [2, 3], [5, 6]], "a:int,b:int")
                res = df.partition(by=["a"]).transform(
                    CallbackTransformer, callback=cb.call, params=dict(has=True)
                )
                df.assert_eq(res)
                res = df.partition(by=["a"]).transform(
                    CallbackTransformer, params=dict(has=False)
                )
                df.assert_eq(res)
            dag.run(self.engine)

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

            with FugueWorkflow() as dag:
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
            dag.run(self.engine)

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

            with FugueWorkflow() as dag:
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
            dag.run(self.engine)

            assert 9 == cb2.n

            def t5(df: pd.DataFrame, c: Callable) -> List[List[Any]]:
                c(df.shape[0])
                return [[pickle.dumps(123)]]

            cb3 = Callbacks()

            with FugueWorkflow() as dag:
                df = dag.df([[0], [1], [2], [3]], "a:int")
                df = df.transform(t5, schema="a:binary", callback=cb3.call)
                df.persist().yield_dataframe_as("x", as_local=True)
            dag.run(self.engine)
            assert dag.yields["x"].result.is_local

            assert 4 == cb3.n

        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_sql_api(self):
            def tr(df: pd.DataFrame, n=1) -> pd.DataFrame:
                return df + n

            with fa.engine_context(self.engine):
                df1 = fa.as_fugue_df([[0, 1], [2, 3], [4, 5]], schema="a:long,b:int")
                df2 = pd.DataFrame([[0, 10], [1, 100]], columns=["a", "c"])
                sdf1 = fa.raw_sql(  # noqa
                    "SELECT ", df1, ".a, b FROM ", df1, " WHERE a<4"
                )
                sdf2 = fa.raw_sql("SELECT * FROM ", df2, " WHERE a<1")  # noqa

                sdf3 = fa.fugue_sql(
                    """
                SELECT sdf1.a,sdf1.b,c FROM sdf1 INNER JOIN sdf2 ON sdf1.a=sdf2.a
                TRANSFORM USING tr SCHEMA *
                """
                )
                res = fa.fugue_sql_flow(
                    """
                TRANSFORM x USING tr(n=2) SCHEMA *
                YIELD LOCAL DATAFRAME AS res
                PRINT sdf1
                """,
                    x=sdf3,
                ).run()
                df_eq(
                    res["res"],
                    [[3, 4, 13]],
                    schema="a:long,b:int,c:long",
                    check_schema=False,
                    throw=True,
                )

                sdf4 = fa.fugue_sql(
                    """
                SELECT sdf1.a,b,c FROM sdf1 INNER JOIN sdf2 ON sdf1.a=sdf2.a
                TRANSFORM USING tr SCHEMA *
                """,
                    as_fugue=False,
                    as_local=True,
                )
                assert not isinstance(sdf4, DataFrame)
                assert fa.is_local(sdf4)

        @pytest.mark.skipif(os.name == "nt", reason="Skip Windows")
        @pytest.mark.skipif(not HAS_QPD, reason="qpd not working")
        def test_any_column_name(self):

            f_parquet = os.path.join(str(self.tmpdir), "a.parquet")
            f_csv = os.path.join(str(self.tmpdir), "a.csv")

            # schema: *,`c *`:long
            def tr(df: pd.DataFrame) -> pd.DataFrame:
                return df.assign(**{"c *": 2})

            with fa.engine_context(self.engine):
                df1 = pd.DataFrame([[0, 1], [2, 3]], columns=["a b", " "])
                df2 = pd.DataFrame([[0, 10], [20, 3]], columns=["a b", "d"])
                r = fa.inner_join(df1, df2, as_fugue=True)
                df_eq(r, [[0, 1, 10]], "`a b`:long,` `:long,d:long", throw=True)
                r = fa.transform(r, tr)
                df_eq(
                    r,
                    [[0, 1, 10, 2]],
                    "`a b`:long,` `:long,d:long,`c *`:long",
                    throw=True,
                )
                r = fa.alter_columns(r, "`c *`:str")
                r = fa.select(
                    r,
                    col("a b").alias("a b "),
                    col(" ").alias("x y"),
                    col("d"),
                    col("c *").cast(int),
                )
                df_eq(
                    r,
                    [[0, 1, 10, 2]],
                    "`a b `:long,`x y`:long,d:long,`c *`:long",
                    throw=True,
                )
                r = fa.rename(r, {"a b ": "a b"})
                fa.save(r, f_csv, header=True, force_single=True)
                fa.save(r, f_parquet)
                df_eq(
                    fa.load(f_parquet, columns=["x y", "d", "c *"], as_fugue=True),
                    [[1, 10, 2]],
                    "`x y`:long,d:long,`c *`:long",
                    throw=True,
                )
                df_eq(
                    fa.load(
                        f_csv,
                        header=True,
                        infer_schema=False,
                        columns=["d", "c *"],
                        as_fugue=True,
                    ),
                    [["10", "2"]],
                    "d:str,`c *`:str",
                    throw=True,
                )
                df_eq(
                    fa.load(
                        f_csv,
                        header=True,
                        columns="`a b`:long,`x y`:long,d:long,`c *`:long",
                        as_fugue=True,
                    ),
                    [[0, 1, 10, 2]],
                    "`a b`:long,`x y`:long,d:long,`c *`:long",
                    throw=True,
                )

                r = fa.fugue_sql(
                    """
                df1 = CREATE [[0, 1], [2, 3]] SCHEMA `a b`:long,` `:long
                df2 = CREATE [[0, 10], [20, 3]] SCHEMA `a b`:long,d:long
                SELECT df1.*,d FROM df1 INNER JOIN df2 ON df1.`a b`=df2.`a b`
                """,
                    as_fugue=True,
                )
                df_eq(r, [[0, 1, 10]], "`a b`:long,` `:long,d:long", throw=True)
                r = fa.fugue_sql(
                    """
                TRANSFORM r USING tr SCHEMA *,`c *`:long
                """,
                    as_fugue=True,
                )
                df_eq(
                    r,
                    [[0, 1, 10, 2]],
                    "`a b`:long,` `:long,d:long,`c *`:long",
                    throw=True,
                )
                r = fa.fugue_sql(
                    """
                ALTER COLUMNS `c *`:long FROM r
                """,
                    as_fugue=True,
                )
                df_eq(
                    r,
                    [[0, 1, 10, 2]],
                    "`a b`:long,` `:long,d:long,`c *`:long",
                    throw=True,
                )
                res = fa.fugue_sql_flow(
                    """
                LOAD "{{f_parquet}}" COLUMNS `x y`,d,`c *`
                YIELD LOCAL DATAFRAME AS r1

                LOAD "{{f_csv}}"(header=TRUE,infer_schema=FALSE) COLUMNS `x y`,d,`c *`
                YIELD LOCAL DATAFRAME AS r2

                LOAD "{{f_csv}}"(header=TRUE,infer_schema=FALSE)
                COLUMNS `a b`:long,`x y`:long,d:long,`c *`:long
                YIELD LOCAL DATAFRAME AS r3
                """,
                    f_parquet=f_parquet,
                    f_csv=f_csv,
                ).run()
                df_eq(
                    res["r1"],
                    [[1, 10, 2]],
                    "`x y`:long,d:long,`c *`:long",
                    throw=True,
                )
                df_eq(
                    res["r2"],
                    [["1", "10", "2"]],
                    "`x y`:str,d:str,`c *`:str",
                    throw=True,
                )
                df_eq(
                    res["r3"],
                    [[0, 1, 10, 2]],
                    "`a b`:long,`x y`:long,d:long,`c *`:long",
                    throw=True,
                )


def mock_creator(p: int) -> DataFrame:
    return ArrayDataFrame([[p]], "a:int")


def mock_creator2(p: int) -> AnyDataFrame:
    return fa.as_fugue_df([[p]], schema="a:int")


def mock_processor(df1: List[List[Any]], df2: List[List[Any]]) -> DataFrame:
    return ArrayDataFrame([[len(df1) + len(df2)]], "a:int")


def mock_processor2(e: ExecutionEngine, dfs: DataFrames) -> DataFrame:
    assert "test" in e.conf
    return ArrayDataFrame([[sum(s.count() for s in dfs.values())]], "a:int")


class MockProcessor3(Processor):
    def process(self, dfs):
        assert "test" in self.workflow_conf
        return ArrayDataFrame([[sum(s.count() for s in dfs.values())]], "a:int")


def mock_processor4(df1: AnyDataFrame, df2: AnyDataFrame) -> AnyDataFrame:
    return ArrayDataFrame([[fa.count(df1) + fa.count(df2)]], "a:int")


def mock_outputter(df1: List[List[Any]], df2: List[List[Any]]) -> None:
    assert len(df1) == len(df2)


def mock_outputter2(df: List[List[Any]]) -> None:
    print(df)


class MockOutputter3(Outputter):
    def process(self, dfs):
        assert "3" == self.partition_spec.num_partitions


def mock_outputter4(df1: AnyDataFrame, df2: AnyDataFrame) -> None:
    assert fa.count(df1) == fa.count(df2)


class MockOutputter4(Outputter):
    def process(self, dfs):
        for k, v in dfs.items():
            print(k)
            v.show()


class MockTransform1(Transformer):
    def get_output_schema(self, df: DataFrame) -> Any:
        assert "test" in self.workflow_conf
        return [df.schema, "ct:int,p:int"]

    def on_init(self, df: DataFrame) -> None:
        assert "test" in self.workflow_conf
        self.pn = self.cursor.physical_partition_no
        self.ks = self.key_schema
        if "on_init_called" not in self.__dict__:
            self.on_init_called = 1
        else:  # pragma: no cover
            self.on_init_called += 1

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        assert 1 == self.on_init_called
        assert "test" in self.workflow_conf
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
    df["p"] = p
    df["ct"] = df.shape[0]
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
