from typing import Any, Dict, Iterable, List

from fugue import FileSystem, FugueWorkflow, Schema
from fugue.execution.native_execution_engine import NativeExecutionEngine


def test_create():
    id0 = FugueWorkflow().df(
        [[0]], "a:int32").workflow.spec_uuid()
    id1 = FugueWorkflow().df(
        [[1]], "a:int32").workflow.spec_uuid()
    id2 = FugueWorkflow().df(
        [[1]], "a:int32").workflow.spec_uuid()

    assert id1 != id0
    assert id1 == id2


def test_checkpoint():
    id0 = FugueWorkflow().df(
        [[0]], "a:int32").workflow.spec_uuid()
    id1 = FugueWorkflow().df(
        [[0]], "a:int32").checkpoint().workflow.spec_uuid()
    id2 = FugueWorkflow().df(
        [[0]], "a:int32").checkpoint("1").workflow.spec_uuid()
    id3 = FugueWorkflow().df(
        [[0]], "a:int32").checkpoint(1).workflow.spec_uuid()

    assert id1 != id0
    assert id1 != id2
    assert id2 != id0
    assert id2 == id3


def test_auto_persist():
    dag1 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag1.df([[0]], "a:int")
    df1.show()
    df1.show()
    id1 = dag1.spec_uuid()

    dag2 = FugueWorkflow(NativeExecutionEngine(
        {"fugue.workflow.auto_persist": True}))
    df1 = dag2.df([[0]], "a:int")
    df1.show()
    df1.show()
    id2 = dag2.spec_uuid()

    dag3 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag3.df([[0]], "a:int").persist()
    df1.show()
    df1.show()
    id3 = dag3.spec_uuid()

    assert id1 != id2
    assert id2 == id3

    dag2 = FugueWorkflow(NativeExecutionEngine(
        {"fugue.workflow.auto_persist": True,
         "fugue.workflow.auto_persist_value": "abc"}))
    df1 = dag2.df([[0]], "a:int")
    df1.show()
    df1.show()
    id2 = dag2.spec_uuid()

    dag3 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag3.df([[0]], "a:int").persist("abc")
    df1.show()
    df1.show()
    id3 = dag3.spec_uuid()

    assert id2 == id3

    dag1 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag1.df([[0]], "a:int")
    df1.show()
    id1 = dag1.spec_uuid()

    dag2 = FugueWorkflow(NativeExecutionEngine(
        {"fugue.workflow.auto_persist": True}))
    df1 = dag2.df([[0]], "a:int")
    df1.show()  # auto persist will not trigger
    id2 = dag2.spec_uuid()

    dag3 = FugueWorkflow(NativeExecutionEngine())
    df1 = dag3.df([[0]], "a:int").persist()
    df1.show()
    id3 = dag3.spec_uuid()

    assert id1 == id2
    assert id2 != id3


def test_workflow_determinism():
    # TODO: need more thorough test, separate this to small ones and remove it
    builder1 = FugueWorkflow()
    a1 = builder1.create_data([[0], [0], [1]], "a:int32")
    b1 = a1.transform("mock_tf1", "*,b:int", pre_partition=dict(by=["a"], num=2))
    a1.show()

    builder2 = FugueWorkflow()
    a2 = builder2.create_data([[0], [0], [1]], Schema("a:int"))
    b2 = a2.transform("mock_tf1", "*,b:int", pre_partition=dict(num="2", by=["a"]))
    a2.show()

    assert builder1.spec_uuid() == builder1.spec_uuid()
    assert a1.spec_uuid() == a2.spec_uuid()
    assert b1.spec_uuid() == b2.spec_uuid()
    assert builder1.spec_uuid() == builder2.spec_uuid()

    builder3 = FugueWorkflow()
    a3 = builder2.create_data([[0], [0], [1]], Schema("a:int"))
    b3 = a2.transform("mock_tf1", "*,b:str", pre_partition=dict(num="2", by=["a"]))
    a3.show()

    assert a1.spec_uuid() == a3.spec_uuid()
    assert b1.spec_uuid() != b3.spec_uuid()
    assert builder1.spec_uuid() != builder3.spec_uuid()

    builder3 = FugueWorkflow()
    a3 = builder2.create_data([[0], [0], [1]], Schema("a:int"))
    b3 = a2.transform("mock_tf1", "*,b:int", pre_partition=dict(num="200", by=["a"]))
    a3.show()

    assert a1.spec_uuid() == a3.spec_uuid()
    assert b1.spec_uuid() != b3.spec_uuid()
    assert builder1.spec_uuid() != builder3.spec_uuid()


def mock_tf1(df: List[Dict[str, Any]], v: int = 1) -> Iterable[Dict[str, Any]]:
    for r in df:
        r["b"] = v * len(df)
        yield r
