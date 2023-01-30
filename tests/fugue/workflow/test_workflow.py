import copy
import os
from random import randint
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from adagio.instances import WorkflowResultCache
from pytest import raises
from triad.exceptions import InvalidOperationError

from fugue import (
    ArrayDataFrame,
    DataFrame,
    FugueWorkflow,
    NativeExecutionEngine,
    QPDPandasEngine,
    PandasDataFrame,
    WorkflowDataFrames,
)
from fugue.collections.partition import PartitionSpec
from fugue.collections.yielded import PhysicalYielded
from fugue.constants import (
    FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH,
    FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE,
)
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FuguePluginsRegistrationError, FugueWorkflowCompileError
from fugue.execution import NativeExecutionEngine
from fugue.extensions.transformer.convert import transformer


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
    assert builder.last_df is a
    raises(InvalidOperationError, lambda: a._task.copy())
    raises(InvalidOperationError, lambda: copy.copy(a._task))
    raises(InvalidOperationError, lambda: copy.deepcopy(a._task))
    a.show()
    a.show()

    raises(FugueWorkflowCompileError, lambda: builder.df(123))

    b = a.transform(mock_tf1, "*,b:int", pre_partition=dict(by=["a"]))
    b.show()
    assert builder.last_df is b
    builder.create_data([[0], [1]], "b:int").show()
    assert builder.last_df is not b
    c = ArrayDataFrame([[100]], "a:int")
    builder.show(a, b, c)
    b = a.partition(by=["a"]).transform(mock_tf2).persist().broadcast()
    b.show()
    assert builder.last_df is b
    c = builder.df(a)
    assert builder.last_df is a

    builder.run()
    df_eq(a.result, [[0], [0], [1]], "a:int")
    raises(FuguePluginsRegistrationError, lambda: builder.run("abc"))

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
    res = dag1.run("", {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})
    assert isinstance(res.yields["x"], PhysicalYielded)
    assert res.yields["x"].storage_type == "file"

    dag2 = FugueWorkflow()
    dag2.df(dag1.yields["x"]).transform(t).yield_dataframe_as("y")
    result = dag2.run("", {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})["y"]
    assert [[0, 2]] == result.as_array()

    dag3 = FugueWorkflow()
    dag3.df(dag2.yields["y"]).transform(t).yield_dataframe_as("z")
    result = dag3.run()["z"]
    assert [[0, 3]] == result.as_array()

    dag4 = FugueWorkflow()
    dag4.df(df).transform(t).yield_table_as("x")
    res = dag4.run(MockEngine, {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})
    assert isinstance(res.yields["x"], PhysicalYielded)
    assert res.yields["x"].storage_type == "table"

    dag5 = FugueWorkflow()
    dag5.df(dag4.yields["x"]).transform(t).yield_dataframe_as("y")
    result = dag5.run(MockEngine, {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})[
        "y"
    ]
    assert [[0, 2]] == result.as_array()


def test_compile_conf():
    dag = FugueWorkflow(compile_conf={"a": 1})
    assert dag.conf["a"] == 1

    dag = FugueWorkflow()  # default conf test
    assert dag.conf.get_or_throw(FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE, str) != ""


class MockSQLEngine(QPDPandasEngine):
    def table_exists(self, table: str) -> bool:
        path = os.path.join(
            self.conf[FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH], table + ".parquet"
        )
        return os.path.exists(path)

    def save_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs: Any
    ) -> None:
        path = os.path.join(
            self.conf[FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH], table + ".parquet"
        )
        df.as_pandas().to_parquet(path)

    def load_table(self, table: str, **kwargs: Any) -> DataFrame:
        path = os.path.join(
            self.conf[FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH], table + ".parquet"
        )
        return PandasDataFrame(pd.read_parquet(path))


class MockEngine(NativeExecutionEngine):
    def create_default_sql_engine(self):
        return MockSQLEngine(self)


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
