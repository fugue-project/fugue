import pandas as pd
from fugue import FugueWorkflow
import sys
import traceback
from fugue.constants import (
    FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE,
    FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE,
)


def test_runtime_exception():
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
