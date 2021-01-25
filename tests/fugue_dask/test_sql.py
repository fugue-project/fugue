import dask.dataframe as dd
import pandas as pd
from fugue import register_execution_engine
from fugue_sql import FugueSQLWorkflow

from fugue_dask import DaskExecutionEngine


def test_sql():
    register_execution_engine(
        "da", lambda conf, **kwargs: DaskExecutionEngine(conf=conf)
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
