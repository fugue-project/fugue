import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from fugue import FugueWorkflow, fsql, transform
from fugue_spark._utils.convert import to_pandas
from fugue_spark.registry import _is_sparksql


def test_importless(spark_session):
    for engine in [spark_session, "spark"]:
        dag = FugueWorkflow()
        dag.df([[0]], "a:int").show()

        dag.run(engine)

        fsql(
            """
        CREATE [[0],[1]] SCHEMA a:int
        SELECT * WHERE a<1
        PRINT
        """
        ).run(engine)

        dag = FugueWorkflow()
        idf = dag.df([[0], [1]], "a:int").as_ibis()
        idf[idf.a < 1].as_fugue().show()

        dag.run(engine)


def test_is_sparksql():
    assert _is_sparksql(("sparksql", "abc"))
    assert not _is_sparksql(123)
    assert not _is_sparksql("SELECT *")


def test_transform_from_sparksql(spark_session):
    # schema: *
    def t(df: pd.DataFrame) -> pd.DataFrame:
        return df

    res = transform(("sparksql", "SELECT 1 AS a, 'b' AS aa"), t)
    assert isinstance(res, DataFrame)  # engine inference
    assert to_pandas(res).to_dict("records") == [{"a": 1, "aa": "b"}]
