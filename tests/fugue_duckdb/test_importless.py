from fugue import FugueWorkflow
from fugue_sql import fsql


def test_importless():
    for engine in ["duck", "duckdb"]:
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

        dag = FugueWorkflow()
        tdf = dag.df([[0], [1]], "a:int")
        dag.select("SELECT * FROM ", tdf, " WHERE a<1", sql_engine=engine)

        dag.run()
