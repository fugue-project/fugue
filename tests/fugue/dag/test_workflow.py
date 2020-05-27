from typing import Any, Dict, Iterable, List

from adagio.instances import WorkflowContext
from fugue.collections.partition import PartitionSpec
from fugue.dag.workflow import FugueWorkflow
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.execution import NaiveExecutionEngine
from fugue.extensions.transformer.convert import transformer
from triad.exceptions import InvalidOperationError
from pytest import raises
import copy


def test_builder():
    e = NaiveExecutionEngine()
    builder = FugueWorkflow(e)
    ctx = WorkflowContext()

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
    ctx.run(builder._spec, {})


def mock_tf1(df: List[Dict[str, Any]], v: int = 1) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = v * len(df)
        yield r


@transformer("*,b:int")
def mock_tf2(df: List[Dict[str, Any]], v: int = 1) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = v * len(df)
        yield r
