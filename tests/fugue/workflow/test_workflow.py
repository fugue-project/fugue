import copy
from typing import Any, Dict, Iterable, List

from adagio.instances import WorkflowContext
from fugue.collections.partition import PartitionSpec
from fugue.workflow.workflow import FugueWorkflow
from fugue.workflow.workflow_context import FugueWorkflowContext
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueWorkflowError
from fugue.execution import NativeExecutionEngine
from fugue.extensions.transformer.convert import transformer
from pytest import raises
from triad.exceptions import InvalidOperationError


def test_builder():
    builder = FugueWorkflow()

    a = builder.create_data([[0], [0], [1]], "a:int")
    raises(InvalidOperationError, lambda: a._task.copy())
    raises(InvalidOperationError, lambda: copy.copy(a._task))
    raises(InvalidOperationError, lambda: copy.deepcopy(a._task))
    a.show()
    a.show()
    b = a.transform(mock_tf1, "*,b:int", partition=dict(by=["a"]))
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


def mock_tf1(df: List[Dict[str, Any]], v: int = 1) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = v * len(df)
        yield r


@transformer("*,b:int")
def mock_tf2(df: List[Dict[str, Any]], v: int = 1) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = v * len(df)
        yield r
