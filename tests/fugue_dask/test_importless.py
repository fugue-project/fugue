import pytest

from fugue import FugueWorkflow, fsql
import fugue.test as ft

@ft.with_backend("dask")
def test_importless(backend_context):
    pytest.importorskip("fugue_sql_antlr")
    pytest.importorskip("ibis")
    for engine in ["dask", backend_context.session]:
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
