import pandas as pd
from fugue import FugueWorkflow
import sys
import traceback
from fugue.constants import (
    FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE,
    FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE,
)


def test_runtime_exception():
    if sys.version_info < (3, 7):
        return

    def tr(df: pd.DataFrame) -> pd.DataFrame:
        raise Exception

    def show(df):
        df.show()

    dag = FugueWorkflow()
    df = dag.df([[0]], "a:int")
    df = df.transform(tr, schema="*")
    show(df)

    try:
        dag.run()
    except:
        assert len(traceback.extract_tb(sys.exc_info()[2])) < 10

    try:
        dag.run("native", {FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE: False})
    except:
        assert len(traceback.extract_tb(sys.exc_info()[2])) > 10

    try:
        dag.run("native", {FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE: ""})
    except:
        assert len(traceback.extract_tb(sys.exc_info()[2])) > 10


def test_modified_exception():
    if sys.version_info < (3, 7):
        return

    def tr(df: pd.DataFrame) -> pd.DataFrame:
        raise Exception

    def show(df):
        df.show()

    def tt(df):
        __modified_exception__ = NotImplementedError()
        return df.transform(tr, schema="*")

    dag = FugueWorkflow()
    df = dag.df([[0]], "a:int")
    df = tt(df)
    show(df)

    try:
        dag.run()
    except Exception as ex:
        assert isinstance(ex.__cause__, NotImplementedError)
