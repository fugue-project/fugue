from fugue import FugueWorkflow


def test_importless():
    for engine in [None]:
        dag = FugueWorkflow()
        idf = dag.df([[0], [1]], "a:int").as_ibis()
        idf[idf.a < 1].as_fugue().show()

        dag.run(engine)
