from typing import Any, Dict, Iterable, List

from adagio.instances import WorkflowContext
from fugue.collections.partition import PartitionSpec
from fugue.dag.workflow import WorkflowBuilder
from fugue.execution import NaiveExecutionEngine
from fugue.transformer.convert import transformer


def test_builder():
    e = NaiveExecutionEngine()
    builder = WorkflowBuilder(e)
    ctx = WorkflowContext()

    a = builder.create_data([[0], [0], [1]], "a:int")
    a.show()
    a.show()
    b = a.transform(mock_tf1, "*,b:int", partition=dict(by=["a"]))
    b.show()
    builder.create_data([[0], [1]], "b:int").show()
    builder.show(a, b)
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
