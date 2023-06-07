import pandas as pd
import pytest
from pyspark.sql import SparkSession

from fugue import FugueSQLWorkflow, register_execution_engine
from fugue_spark import SparkExecutionEngine


def test_sql(spark_session):
    pytest.importorskip("fugue_sql_antlr")
    register_execution_engine(
        "_spark",
        lambda conf, **kwargs: SparkExecutionEngine(
            conf=conf, spark_session=spark_session
        ),
    )
    df = spark_session.createDataFrame(pd.DataFrame([[0], [1]], columns=["a"]))
    dag = FugueSQLWorkflow()
    dag(
        """
    SELECT * FROM df WHERE a>0
    PRINT
    """,
        df=df,
    )
    dag.run("_spark")
