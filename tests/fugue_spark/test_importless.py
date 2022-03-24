from fugue import FugueWorkflow
from fugue_sql import fsql
from pyspark.sql import SparkSession


def test_importless():
    spark = SparkSession.builder.getOrCreate()
    for engine in [spark, "spark"]:
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
