import os
from typing import Any, Dict, Iterable

import pandas as pd

from fugue import (
    DataFrame,
    FugueWorkflow,
    PandasDataFrame,
    out_transform,
    transform,
    make_execution_engine,
)
from fugue.constants import FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH
from fugue.exceptions import FugueInterfacelessError, FugueWorkflowCompileError
from pytest import raises


def test_transform():
    pdf = pd.DataFrame([[1, 10], [0, 0], [1, 1], [0, 20]], columns=["a", "b"])

    def f1(df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values("b").head(1)

    result = transform(pdf, f1, schema="*")
    assert isinstance(result, pd.DataFrame)
    assert result.values.tolist() == [[0, 0]]

    # schema: *
    def f2(df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values("b").head(1)

    result = transform(pdf, f2)
    assert isinstance(result, pd.DataFrame)
    assert result.values.tolist() == [[0, 0]]

    result = transform(pdf, f2, persist=True)
    assert isinstance(result, pd.DataFrame)
    assert result.values.tolist() == [[0, 0]]

    result = transform(pdf, f2, partition=dict(by=["a"]))
    assert isinstance(result, pd.DataFrame)
    assert sorted(result.values.tolist(), key=lambda x: x[0]) == [[0, 0], [1, 1]]
    result = transform(
        pdf, f2, partition=dict(by=["a"]), as_fugue=True
    )
    assert isinstance(result, DataFrame)

    ppdf = PandasDataFrame(pdf)
    assert isinstance(transform(ppdf, f2), DataFrame)

    # schema: *
    def f3(df: pd.DataFrame, called: callable) -> pd.DataFrame:
        called()
        return df

    cb = Callback()
    result = transform(pdf, f3, callback=cb.called)
    assert 1 == cb.ct

    with raises(FugueWorkflowCompileError):
        transform(123, f2)


def test_transform_from_yield(tmpdir):
    # schema: *,x:int
    def f(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(x=1)

    dag = FugueWorkflow()
    dag.df([[0]], "a:int").yield_dataframe_as("x1")
    dag.df([[1]], "b:int").yield_file_as("x2")
    dag.run("", {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})

    result = transform(dag.yields["x1"], f)
    assert isinstance(result, DataFrame)
    assert result.as_array(type_safe=True) == [[0, 1]]

    result = transform(
        dag.yields["x2"],
        f,
        engine_conf={FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)},
    )
    assert isinstance(result, DataFrame)
    assert result.as_array(type_safe=True) == [[1, 1]]


def test_transform_from_file(tmpdir):
    fp = os.path.join(tmpdir, "t.parquet")
    pd.DataFrame(dict(a=[2])).to_parquet(fp)

    # schema: *,x:int
    def f(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(x=1)

    result = transform(fp, f, as_fugue=True)
    assert result.as_array(type_safe=True) == [[2, 1]]

    with raises(FugueInterfacelessError):
        transform("t.csv", f)

    with raises(FugueInterfacelessError):
        transform("t.json", f)


def test_transform_to_file(tmpdir):
    fp = os.path.join(tmpdir, "t.parquet")
    tdf = pd.DataFrame(dict(a=[2]))
    # schema: *,x:int
    def f(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(x=1)

    engine = make_execution_engine(
        None, {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)}
    )

    # checkpoint is True, save_path is None
    result = transform(
        tdf, f, as_fugue=True, checkpoint=True, engine=engine
    )
    assert result.as_array() == [[2, 1]]

    # checkpoint is True, save_path is not None
    result = transform(
        tdf,
        f,
        as_fugue=True,
        checkpoint=True,
        save_path=fp,
        engine=engine,
    )
    assert result.as_array() == [[2, 1]]
    os.remove(fp)

    # checkpoint is False, save_path is not None
    result = transform(
        tdf,
        f,
        as_fugue=True,
        save_path=fp,
        engine=engine,
    )
    assert os.path.exists(fp)
    assert result == fp
    assert pd.read_parquet(fp).values.tolist() == [[2, 1]]
    os.remove(fp)

    # test that parquet format is used for saving when
    # no file extension is provided
    fp = os.path.join(tmpdir, "test")
    transform(
        tdf,
        f,
        save_path=fp,
        engine=engine,
    )
    loaded = pd.read_parquet(fp)
    assert pd.read_parquet(fp).values.tolist() == [[2, 1]]
    os.remove(fp)

    # catch invalid file paths
    with raises(FugueInterfacelessError):
        transform(
            tdf,
            f,
            as_fugue=True,
            save_path="f.csv",
            engine=engine,
        )
    with raises(FugueInterfacelessError):
        transform(
            tdf,
            f,
            as_fugue=True,
            save_path="f.json",
            engine=engine,
        )


def test_out_transform(tmpdir):
    pdf = pd.DataFrame([[1, 10], [0, 0], [1, 1], [0, 20]], columns=["a", "b"])

    class T:
        def __init__(self):
            self.n = 0

        def f(self, df: Iterable[Dict[str, Any]]) -> None:
            self.n += 1

    t = T()
    out_transform(pdf, t.f)
    assert 1 == t.n

    t = T()
    out_transform(pdf, t.f, partition=dict(by=["a"]))
    assert 2 == t.n

    dag = FugueWorkflow()
    dag.df(pdf).yield_dataframe_as("x1")
    dag.df(pdf).yield_dataframe_as("x2")
    dag.run("", {FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)})

    t = T()
    out_transform(dag.yields["x1"], t.f)
    assert 1 == t.n

    t = T()
    out_transform(
        dag.yields["x2"],
        t.f,
        partition=dict(by=["a"]),
        engine_conf={FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: str(tmpdir)},
    )
    assert 2 == t.n

    # schema: *
    def f3(df: pd.DataFrame, called: callable) -> pd.DataFrame:
        called()
        return df

    cb = Callback()
    out_transform(pdf, f3, callback=cb.called)
    assert 1 == cb.ct

    fp = os.path.join(tmpdir, "t.parquet")
    pdf.to_parquet(fp)
    cb = Callback()
    out_transform(fp, f3, callback=cb.called)
    assert 1 == cb.ct

    with raises(FugueWorkflowCompileError):
        out_transform(123, t.f)


class Callback:
    def __init__(self):
        self.ct = 0

    def called(self) -> None:
        self.ct += 1
