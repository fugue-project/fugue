import pytest

pytest.importorskip("fugue_sql_antlr")
import dask.dataframe as dd
import pandas as pd

from fugue import FugueSQLWorkflow, register_execution_engine
from fugue_dask import DaskExecutionEngine


def test_sql(fugue_dask_client):
    register_execution_engine(
        "da",
        lambda conf, **kwargs: DaskExecutionEngine(conf=conf, dask_client=fugue_dask_client),
    )
    df = dd.from_pandas(pd.DataFrame([[0], [1]], columns=["a"]), npartitions=2)
    dag = FugueSQLWorkflow()
    dag(
        """
    SELECT * FROM df WHERE a>0
    PRINT
    """,
        df=df,
    )
    dag.run("da")
