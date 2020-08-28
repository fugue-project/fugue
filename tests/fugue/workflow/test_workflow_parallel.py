from fugue import FugueWorkflow, DataFrame, NativeExecutionEngine
from typing import List, Any
from time import sleep
from timeit import timeit
from pytest import raises


def test_parallel():
    dag = FugueWorkflow()
    dag.create(create).process(process).output(display)
    dag.create(create).process(process).output(display)

    t = timeit(
        lambda: dag.run(NativeExecutionEngine({"fugue.workflow.concurrency": 10})),
        number=1,
    )  # warmup
    t = timeit(
        lambda: dag.run(NativeExecutionEngine({"fugue.workflow.concurrency": 10})),
        number=1,
    )
    assert t < 0.4


def test_parallel_exception():
    dag = FugueWorkflow()
    dag.create(create).process(process).process(process, params=dict(sec=0.5)).output(
        display
    )
    dag.create(create_e).process(process).output(display)

    def run(dag, *args):
        with raises(NotImplementedError):
            dag.run(*args)

    t = timeit(
        lambda: run(dag, NativeExecutionEngine({"fugue.workflow.concurrency": 2})),
        number=1,
    )  # warmup
    t = timeit(
        lambda: run(dag, NativeExecutionEngine({"fugue.workflow.concurrency": 2})),
        number=1,
    )
    assert t < 0.5


# schema: a:int
def create(sec: float = 0.1) -> List[List[Any]]:
    sleep(sec)
    return [[0]]


# schema: a:int
def create_e(sec: float = 0.1) -> List[List[Any]]:
    raise NotImplementedError


def process(df: DataFrame, sec: float = 0.1) -> DataFrame:
    sleep(sec)
    return df


def display(df: DataFrame, sec: float = 0.1) -> None:
    sleep(sec)
    df.show()
