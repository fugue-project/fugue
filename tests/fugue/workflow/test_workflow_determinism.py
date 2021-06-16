from typing import Any, Dict, Iterable, List

from fugue import FileSystem, FugueWorkflow, Schema
from fugue.execution.native_execution_engine import NativeExecutionEngine
from triad import to_uuid


def test_create():
    # by default, input data does not affect determinism
    id0 = FugueWorkflow().df([[0]], "a:int32").workflow.spec_uuid()
    id1 = FugueWorkflow().df([[1]], "a:int32").workflow.spec_uuid()
    id2 = FugueWorkflow().df([[1]], "a:int32").workflow.spec_uuid()

    assert id1 == id0
    assert id1 == id2

    # unless we use data_determiner
    id0 = (
        FugueWorkflow()
        .df([[0]], "a:int32", data_determiner=to_uuid)
        .workflow.spec_uuid()
    )
    id1 = (
        FugueWorkflow()
        .df([[1]], "a:int32", data_determiner=to_uuid)
        .workflow.spec_uuid()
    )
    id2 = (
        FugueWorkflow()
        .df([[1]], "a:int32", data_determiner=to_uuid)
        .workflow.spec_uuid()
    )

    assert id1 != id0
    assert id1 == id2


def test_checkpoint():
    id0 = FugueWorkflow().df([[0]], "a:int32").workflow.spec_uuid()
    id1 = FugueWorkflow().df([[0]], "a:int32").checkpoint().workflow.spec_uuid()
    # checkpoint doesn't change determinism
    assert id0 == id1


def test_yield():
    dag = FugueWorkflow()
    dag.df([[0]], "a:int32").show()
    id0 = dag.spec_uuid()
    x = FugueWorkflow().df([[0]], "a:int32")
    x.yield_file_as("x")
    x.show()
    id1 = x.workflow.spec_uuid()
    x = FugueWorkflow().df([[0]], "a:int32")
    x.deterministic_checkpoint().yield_file_as("y")
    x.show()
    id2 = x.workflow.spec_uuid()
    x = FugueWorkflow().df([[0]], "a:int32")
    x.deterministic_checkpoint().yield_dataframe_as("z")
    x.show()
    id3 = x.workflow.spec_uuid()
    # yield doesn't change determinism
    assert id0 == id1
    assert id0 == id2
    assert id0 == id3


def test_auto_persist():
    dag1 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag1.df([[0]], "a:int")
    df1.show()
    df1.show()
    id1 = dag1.spec_uuid()

    dag2 = FugueWorkflow(NativeExecutionEngine({"fugue.workflow.auto_persist": True}))
    df1 = dag2.df([[0]], "a:int")
    df1.show()
    df1.show()
    id2 = dag2.spec_uuid()

    dag3 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag3.df([[0]], "a:int").weak_checkpoint(level=None)
    df1.show()
    df1.show()
    id3 = dag3.spec_uuid()

    assert id1 == id2
    assert id2 == id3

    dag2 = FugueWorkflow(
        NativeExecutionEngine(
            {
                "fugue.workflow.auto_persist": True,
                "fugue.workflow.auto_persist_value": "abc",
            }
        )
    )
    df1 = dag2.df([[0]], "a:int")
    df1.show()
    df1.show()
    id2 = dag2.spec_uuid()

    dag3 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag3.df([[0]], "a:int").weak_checkpoint(level="abc")
    df1.show()
    df1.show()
    id3 = dag3.spec_uuid()

    assert id2 == id3

    dag1 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag1.df([[0]], "a:int")
    df1.show()
    id1 = dag1.spec_uuid()

    dag2 = FugueWorkflow(NativeExecutionEngine({"fugue.workflow.auto_persist": True}))
    df1 = dag2.df([[0]], "a:int")
    df1.show()  # auto persist will not trigger
    id2 = dag2.spec_uuid()

    dag3 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag3.df([[0]], "a:int").weak_checkpoint(level=None)
    df1.show()
    id3 = dag3.spec_uuid()

    assert id1 == id2
    assert id2 == id3  # checkpoint, including auto_persist doesn't change determinism


def test_workflow_determinism_1():
    dag1 = FugueWorkflow()
    a1 = dag1.create_data([[0], [0], [1]], "a:int32")
    b1 = a1.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    a1.show()

    dag2 = FugueWorkflow()
    a2 = dag2.create_data([[0], [0], [1]], "a:int32")
    b2 = a2.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    a2.show()

    assert dag1.spec_uuid() == dag1.spec_uuid()
    assert a1.spec_uuid() == a2.spec_uuid()
    assert b1.spec_uuid() == b2.spec_uuid()
    assert dag1.spec_uuid() == dag2.spec_uuid()


def test_workflow_determinism_2():
    dag1 = FugueWorkflow()
    dag1.create_data([[0], [0], [1]], "a:int32")  # <----
    a1 = dag1.create_data([[0], [0], [1]], "a:int32")
    b1 = a1.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    a1.show()

    dag2 = FugueWorkflow()
    a2 = dag2.create_data([[0], [0], [1]], "a:int32")
    b2 = a2.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    a2.show()

    assert a1.spec_uuid() == a2.spec_uuid()
    assert b1.spec_uuid() == b2.spec_uuid()
    assert dag1.spec_uuid() != dag2.spec_uuid()


def test_workflow_determinism_3():
    dag1 = FugueWorkflow()
    data = [[0], [0], [1]]
    a1 = dag1.create_data(data, "a:int32", data_determiner=to_uuid)
    b1 = a1.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    a1.show()

    dag2 = FugueWorkflow()
    data = [[1], [10], [20]]
    a2 = dag2.create_data(data, "a:int32", data_determiner=to_uuid)  # <---
    b2 = a2.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    a2.show()

    assert a1.spec_uuid() != a2.spec_uuid()
    assert b1.spec_uuid() != b2.spec_uuid()
    assert dag1.spec_uuid() != dag2.spec_uuid()


def test_workflow_determinism_4():
    dag1 = FugueWorkflow()
    a1 = dag1.create_data([[0], [0], [1]], "a:int32")
    b1 = a1.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    b1.out_transform(mock_tf1)
    a1.show()

    dag2 = FugueWorkflow()
    a2 = dag2.create_data([[0], [0], [1]], "a:int32")
    b2 = a2.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=20))  # <---
    b2.out_transform(mock_tf1)
    a2.show()

    assert a1.spec_uuid() == a2.spec_uuid()
    assert b1.spec_uuid() != b2.spec_uuid()
    assert dag1.spec_uuid() != dag2.spec_uuid()


def test_workflow_determinism_5():
    dag1 = FugueWorkflow()
    a1 = dag1.create_data([[0], [0], [1]], "a:int32")
    b1 = a1.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    a1.show()

    dag2 = FugueWorkflow()
    a2 = dag2.create_data([[0], [0], [1]], "a:int32")
    b2 = a2.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"], num=2))
    a2.show(rows=22)  # <---

    assert a1.spec_uuid() == a2.spec_uuid()
    assert b1.spec_uuid() == b2.spec_uuid()
    assert dag1.spec_uuid() != dag2.spec_uuid()


def test_workflow_determinism_6():
    dag1 = FugueWorkflow()
    dag1.create_data([[0], [0], [1]], "a:int32")
    a1 = dag1.create_data([[0], [0], [1]], "a:int32")
    b1 = dag1.create_data([[1], [0], [1]], "a:int32")
    c1 = a1.union(b1)

    dag2 = FugueWorkflow()
    a2 = dag1.create_data([[0], [0], [1]], "a:int32")
    b2 = dag1.create_data([[1], [0], [1]], "a:int32")
    c2 = a1.union(b2)

    assert a1.spec_uuid() == a2.spec_uuid()
    assert b1.spec_uuid() == b2.spec_uuid()
    assert c1.spec_uuid() == c2.spec_uuid()
    assert dag1.spec_uuid() != dag2.spec_uuid()


def test_workflow_determinism_7():
    dag1 = FugueWorkflow()
    a1 = dag1.create_data([[0], [0], [1]], "a:int32")
    a1.out_transform(mock_tf1)
    a1.show()

    dag2 = FugueWorkflow()
    a2 = dag2.create_data([[0], [0], [1]], "a:int32")
    a2.out_transform(mock_tf1)
    a2.show()

    dag3 = FugueWorkflow()
    a3 = dag3.create_data([[0], [0], [1]], "a:int32")
    a3.show()

    assert a1.spec_uuid() == a2.spec_uuid()
    assert dag1.spec_uuid() == dag2.spec_uuid()

    assert a1.spec_uuid() == a3.spec_uuid()
    assert dag1.spec_uuid() != dag3.spec_uuid()


def test_workflow_determinism_8():
    dag1 = FugueWorkflow()
    a1 = dag1.create_data([[0], [0], [1]], "a:int32")
    a1.select("a", "b")
    a1.show()

    dag2 = FugueWorkflow()
    a2 = dag2.create_data([[0], [0], [1]], "a:int32")
    a2.select("a", "b")
    a2.show()

    dag3 = FugueWorkflow()
    a3 = dag3.create_data([[0], [0], [1]], "a:int32")
    a3.select("b", "a")
    a3.show()

    dag4 = FugueWorkflow()
    a4 = dag4.create_data([[0], [0], [1]], "a:int32")
    a4.select("a", "b", distinct=True)
    a4.show()

    assert a1.spec_uuid() == a2.spec_uuid()
    assert dag1.spec_uuid() == dag2.spec_uuid()

    assert a1.spec_uuid() == a3.spec_uuid()
    assert dag1.spec_uuid() != dag3.spec_uuid()

    assert a1.spec_uuid() == a4.spec_uuid()
    assert dag1.spec_uuid() != dag4.spec_uuid()


def mock_tf1(df: List[Dict[str, Any]], v: int = 1) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = v * len(df)
        yield r
