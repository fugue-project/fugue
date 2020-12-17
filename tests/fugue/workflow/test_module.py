import pandas as pd
from fugue import FugueWorkflow, WorkflowDataFrame, WorkflowDataFrames, module
from fugue.workflow.module import _to_module, _ModuleFunctionWrapper
from fugue_sql import FugueSQLWorkflow
from pytest import raises
from fugue.exceptions import FugueInterfacelessError


def test_input_module():
    # pylint: disable=no-value-for-parameter
    @module()
    def input1(wf: FugueWorkflow) -> WorkflowDataFrame:
        return wf.df([[0]], "a:int")

    @module()
    def input2(wf: FugueWorkflow, a: int) -> WorkflowDataFrame:
        return wf.df([[a]], "a:int")

    @module()
    def input3(wf: FugueWorkflow, a: int, b: int) -> WorkflowDataFrames:
        return WorkflowDataFrames(a=wf.df([[a]], "a:int"), b=wf.df([[b]], "b:int"))

    assert not input1.has_input
    assert input1.has_single_output
    assert not input1.has_no_output

    assert input3.has_multiple_output

    with FugueWorkflow() as dag:
        input1(dag).assert_eq(dag.df([[0]], "a:int"))

    with FugueWorkflow() as dag:
        input2(a=10, wf=dag).assert_eq(dag.df([[10]], "a:int"))

    with FugueWorkflow() as dag:
        dfs = input3(dag, 10, 11)
        dfs["a"].assert_eq(dag.df([[10]], "a:int"))
        dfs["b"].assert_eq(dag.df([[11]], "b:int"))


def test_process_module():
    # pylint: disable=no-value-for-parameter
    def process(df: pd.DataFrame, d: int = 1) -> pd.DataFrame:
        df["a"] += d
        return df

    @module()
    def p1(wf: FugueSQLWorkflow, df: WorkflowDataFrame) -> WorkflowDataFrame:
        return df.process(process)

    @module()
    def p2(wf: FugueWorkflow, dfs: WorkflowDataFrames, d: int) -> WorkflowDataFrames:
        return WorkflowDataFrames(
            {k: v.process(process, params={"d": d}) for k, v in dfs.items()}
        )

    @module()
    def p3(df: WorkflowDataFrame) -> WorkflowDataFrame:
        return df.process(process)

    assert p1.has_input
    assert not p1.has_dfs_input
    assert p2.has_dfs_input

    with FugueSQLWorkflow() as dag:
        df = dag.df([[0]], "a:int")
        p1(df).assert_eq(dag.df([[1]], "a:int"))
        p1(dag, df).assert_eq(dag.df([[1]], "a:int"))
        p1(df=df).assert_eq(dag.df([[1]], "a:int"))
        p1(df=df, wf=dag).assert_eq(dag.df([[1]], "a:int"))

    with FugueWorkflow() as dag:
        dfs = WorkflowDataFrames(aa=dag.df([[0]], "a:int"), bb=dag.df([[10]], "a:int"))
        r = p2(dag, dfs, 1)
        r["aa"].assert_eq(dag.df([[1]], "a:int"))
        r["bb"].assert_eq(dag.df([[11]], "a:int"))

        r = p2(dfs, 1)
        r["aa"].assert_eq(dag.df([[1]], "a:int"))
        r["bb"].assert_eq(dag.df([[11]], "a:int"))

        r = p2(d=1, dfs=dfs, wf=dag)
        r["aa"].assert_eq(dag.df([[1]], "a:int"))
        r["bb"].assert_eq(dag.df([[11]], "a:int"))

        r = p2(d=1, dfs=dfs)
        r["aa"].assert_eq(dag.df([[1]], "a:int"))
        r["bb"].assert_eq(dag.df([[11]], "a:int"))

    with FugueWorkflow() as dag:
        df = dag.df([[0]], "a:int")
        p3(df).assert_eq(dag.df([[1]], "a:int"))
        p3(df=df).assert_eq(dag.df([[1]], "a:int"))


def test_output_module():
    # pylint: disable=no-value-for-parameter

    @module()
    def o1(wf: FugueWorkflow, df: WorkflowDataFrame) -> None:
        pass

    @module()
    def o2(wf: FugueWorkflow, df: WorkflowDataFrame):
        pass

    @module()
    def o3(df: WorkflowDataFrame):
        pass

    assert o1.has_input
    assert o1.has_no_output
    assert o2.has_no_output
    assert o3.has_no_output

    with FugueWorkflow() as dag:
        df = dag.df([[0]], "a:int")
        o1(df)
        o1(dag, df)
        o2(df=df)
        o2(df=df, wf=dag)
        o3(df)


def test_invalid_module():
    # pylint: disable=no-value-for-parameter

    @module()
    def o1(wf: FugueWorkflow, df1: WorkflowDataFrame, df2: WorkflowDataFrame) -> None:
        pass

    @module()
    def o2(wf: FugueWorkflow, dfs: WorkflowDataFrames) -> None:
        pass

    dag1 = FugueWorkflow()
    df1 = dag1.df([[0]], "a:int")
    dag2 = FugueWorkflow()
    df2 = dag2.df([[1]], "a:int")

    with raises(ValueError):
        o1(df1, df2)

    with raises(ValueError):
        o2(WorkflowDataFrames(a=df1, b=df2))


def test_interfaceless():
    def i1(wf: FugueSQLWorkflow) -> WorkflowDataFrame:
        return wf.df([[0]], "a:int")

    def i2(df: WorkflowDataFrame) -> WorkflowDataFrame:
        return df

    @module()
    def i3(wf: FugueSQLWorkflow, df: WorkflowDataFrame) -> WorkflowDataFrame:
        return df

    assert isinstance(_to_module(i1), _ModuleFunctionWrapper)
    assert isinstance(_to_module("i2"), _ModuleFunctionWrapper)
    assert isinstance(_to_module(i3), _ModuleFunctionWrapper)
    assert isinstance(_to_module("i3"), _ModuleFunctionWrapper)

    def i4():
        pass

    def i5(df: WorkflowDataFrame, wf: FugueWorkflow) -> None:
        pass

    raises(FugueInterfacelessError, lambda: _to_module(i4))
    raises(FugueInterfacelessError, lambda: _to_module(i5))