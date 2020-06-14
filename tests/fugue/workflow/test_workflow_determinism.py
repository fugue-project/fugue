from fugue import FugueWorkflow, Schema, FileSystem
from typing import List, Any, Dict, Iterable


def test_checkpoint():
    dag = FugueWorkflow()
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
