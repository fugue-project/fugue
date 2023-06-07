from fugue import FugueWorkflow
from fugue import fsql
from dask.distributed import Client
import pytest


def test_importless():
    pytest.importorskip("fugue_sql_antlr")
    client = Client()
    for engine in ["dask", client]:
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
