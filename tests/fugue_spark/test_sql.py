import pandas as pd
from fugue import register_execution_engine
from fugue_sql import FugueSQLWorkflow
from pyspark.sql import SparkSession

from fugue_spark import SparkExecutionEngine


def test_sql():
    session = SparkSession.builder.getOrCreate()
    register_execution_engine(
        "s",
        lambda conf, **kwargs: SparkExecutionEngine(conf=conf, spark_session=session),
    )
    df = session.createDataFrame(pd.DataFrame([[0], [1]], columns=["a"]))
    dag = FugueSQLWorkflow()
    dag(
        """
    SELECT * FROM df WHERE a>0
    PRINT
    """,
        df=df,
    )
    dag.run("s")
