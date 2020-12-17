import copy
from random import randint, seed
from typing import Any, Dict, Iterable, List

from adagio.instances import WorkflowContext, WorkflowResultCache
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueWorkflowError
from fugue.execution import NativeExecutionEngine
from fugue.extensions.transformer.convert import transformer
from fugue.workflow._workflow_context import (
    FugueWorkflowContext,
    _FugueInteractiveWorkflowContext,
)
from fugue.workflow.workflow import (
    FugueWorkflow,
    WorkflowDataFrames,
    _FugueInteractiveWorkflow,
)
from pytest import raises
from triad.collections.schema import Schema
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


def test_workflow():
    builder = FugueWorkflow()

    a = builder.create_data([[0], [0], [1]], "a:int")
    raises(InvalidOperationError, lambda: a._task.copy())
    raises(InvalidOperationError, lambda: copy.copy(a._task))
    raises(InvalidOperationError, lambda: copy.deepcopy(a._task))
    a.show()
    a.show()
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


def test_interactive_workflow():
    # TODO: interactive workflow is not working correctly

    # with statement is not valid
    with raises(FugueWorkflowError):
        with _FugueInteractiveWorkflow():
            pass

    # test basic operations, .result can be directly used
    dag = _FugueInteractiveWorkflow()
    a = dag.create_data([[0]], "a:int")
    df_eq(a.result, [[0]], "a:int")
    df_eq(a.result, [[0]], "a:int")
    a.compute()
    df_eq(a.result, [[0]], "a:int")

    # make sure create_rand is called once
    seed(0)
    dag = _FugueInteractiveWorkflow()
    b = dag.create(using=create_rand)
    b.compute()
    res1 = list(b.result.as_array())
    b.show()
    b.process(my_show)
    dag.run()
    dag.run()
    res2 = list(b.result.as_array())
    assert res1 == res2

    # assertion on underlying cache methods
    cache = MockCache(dummy=False)
    dag = _FugueInteractiveWorkflow(cache=cache)
    a = dag.create_data([[0]], "a:int")
    assert 1 == cache.get_called
    assert 1 == cache.set_called
    dag.run()  # for second run, this cache is not used at all
    assert 1 == cache.get_called
    assert 1 == cache.set_called
    a.show()  # new task will trigger
    assert 2 == cache.get_called
    assert 2 == cache.set_called
    dag.run()
    assert 2 == cache.get_called
    assert 2 == cache.set_called

    # cache returns dummy data
    dag = _FugueInteractiveWorkflow(_FugueInteractiveWorkflowContext(cache=MockCache))
    a = dag.create_data([[0]], "a:int")
    b = dag.create_data([[50]], "a:int")
    a.assert_eq(b)  # dummy value from cache makes them equal


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
