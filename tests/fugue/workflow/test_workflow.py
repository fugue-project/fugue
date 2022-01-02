import copy
from random import randint
from typing import Any, Dict, Iterable, List

import pandas as pd
from adagio.instances import WorkflowResultCache
from fugue import (
    ArrayDataFrame,
    DataFrame,
    ExecutionEngine,
    FugueWorkflow,
    WorkflowDataFrames,
)
from fugue.collections.partition import PartitionSpec
from fugue.constants import (
    FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH,
    FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE,
)
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueWorkflowCompileError
from fugue.execution import NativeExecutionEngine
from fugue.extensions.transformer.convert import transformer
from fugue.workflow._workflow_context import FugueWorkflowContext
from pytest import raises
from triad.exceptions import InvalidOperationError


def test_worflow_dataframes():
    dag1 = FugueWorkflow()
    df1 = dag1.df([[0]], "a:int")
    df2 = dag1.df([[0]], "b:int")
    dag2 = FugueWorkflow()
    df3 = dag2.df([[0]], "a:int")

    dfs1 = WorkflowDataFrames(a=df1, b=df2)
    assert dfs1["a"] is df1
    assert dfs1["b"] is df2

    dfs2 = WorkflowDataFrames(dfs1, aa=df1, bb=df2)
    assert 4 == len(dfs2)

    with raises(ValueError):
        WorkflowDataFrames(a=df1, b=df3)

    with raises(ValueError):
        WorkflowDataFrames(a=df1, b=ArrayDataFrame([[0]], "a:int"))

    dag = FugueWorkflow()
    df = dag.df([[0, 1], [1, 1]], "a:int,b:int")
    assert df.partition_spec.empty
    df2 = df.partition(by=["a"])
    assert df.partition_spec.empty
    assert df2.partition_spec == PartitionSpec(by=["a"])
    df3 = df.partition_by("a", "b")
    assert df.partition_spec.empty
    assert df3.partition_spec == PartitionSpec(by=["a", "b"])
    df4 = df.per_partition_by("a", "b")
    assert df.partition_spec.empty
    assert df4.partition_spec == PartitionSpec(by=["a", "b"], algo="even")
    df4 = df.per_row()
    assert df.partition_spec.empty
    assert df4.partition_spec == PartitionSpec("per_row")


def test_workflow():
    builder = FugueWorkflow()

    a = builder.create_data([[0], [0], [1]], "a:int")
    raises(InvalidOperationError, lambda: a._task.copy())
    raises(InvalidOperationError, lambda: copy.copy(a._task))
    raises(InvalidOperationError, lambda: copy.deepcopy(a._task))
    a.show()
    a.show()

    raises(FugueWorkflowCompileError, lambda: builder.df(123))

    b = a.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"]))
    b.show()
    builder.create_data([[0], [1]], "b:int").show()
    c = ArrayDataFrame([[100]], "a:int")
    builder.show(a, b, c)
    b = a.partition(by=["a"]).transform(mock_tf2).persist().broadcast()
    b.show()

    builder.run()
    df_eq(a.result, [[0], [0], [1]], "a:int")
    raises(TypeError, lambda: builder.run("abc"))
    builder.run(FugueWorkflowContext())
    df_eq(a.result, [[0], [0], [1]], "a:int")
    builder.run("NativeExecutionEngine")
    df_eq(b.result, [[0, 2], [0, 2], [1, 1]], "a:int,b:int")
    df_eq(b.compute(), [[0, 2], [0, 2], [1, 1]], "a:int,b:int")
    df_eq(b.compute(NativeExecutionEngine), [[0, 2], [0, 2], [1, 1]], "a:int,b:int")


def test_yield(tmpdir):
    df = pd.DataFrame([[0, 0]], columns=["a", "b"])

    # schema: *
    def t(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(b=df.b + 1)

    dag = FugueWorkflow()
    dag.df(df).transform(t).yield_dataframe_as("x")
    result = dag.run()["x"]
    assert [[0, 1]] == result.as_array()

    dag1 = FugueWorkflow()
    dag1.df(df).transform(t).yield_file_as("x")
    dag1.run("", {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})

    dag2 = FugueWorkflow()
    dag2.df(dag1.yields["x"]).transform(t).yield_dataframe_as("y")
    result = dag2.run("", {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})["y"]
    assert [[0, 2]] == result.as_array()

    dag3 = FugueWorkflow()
    dag3.df(dag2.yields["y"]).transform(t).yield_dataframe_as("z")
    result = dag3.run()["z"]
    assert [[0, 3]] == result.as_array()


def test_compile_conf():
    def assert_conf(e: ExecutionEngine, **kwargs) -> pd.DataFrame:
        for k, v in kwargs.items():
            assert e.compile_conf[k] == v
        return pd.DataFrame([[0]], columns=["a"])

    dag = FugueWorkflow(conf={"a": 1})
    dag.create(assert_conf, params=dict(a=1))

    dag.run()

    with raises(KeyError):  # non-compile time param doesn't keep in new engine
        dag.run(NativeExecutionEngine())

    dag = FugueWorkflow(conf={FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE: "abc"})
    dag.create(assert_conf, params=dict({FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE: "abc"}))

    dag.run()

    # non-compile time param is kepts
    dag.run(NativeExecutionEngine())

    # non-compile time param can't be changed by new engines
    # new engine compile conf will be overwritten
    dag.run(NativeExecutionEngine({FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE: "def"}))


class MockCache(WorkflowResultCache):
    def __init__(self, ctx=None, dummy=True):
        self.dummy = dummy
        self.tb = dict()
        self.set_called = 0
        self.skip_called = 0
        self.get_called = 0
        self.hit = 0

    def set(self, key: str, value: Any) -> None:
        self.tb[key] = (False, value)
        print("set", key)
        self.set_called += 1

    def skip(self, key: str) -> None:
        self.tb[key] = (True, None)
        self.skip_called += 1

    def get(self, key: str):
        if self.dummy:
            return True, False, ArrayDataFrame([[100]], "a:int")
        self.get_called += 1
        if key not in self.tb:
            print("not get", key)
            return False, False, None
        x = self.tb[key]
        print("get", key)
        self.hit += 1
        return True, x[0], x[1]


def mock_tf1(df: List[Dict[str, Any]], v: int = 1) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = v * len(df)
        yield r


@transformer("*,b:int")
def mock_tf2(df: List[Dict[str, Any]], v: int = 1) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = v * len(df)
        yield r


# schema: a:int
def create_rand() -> List[List[Any]]:
    return [[randint(0, 10)]]


def my_show(df: DataFrame) -> DataFrame:
    df.show()
    return df
