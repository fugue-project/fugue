import pytest


@pytest.fixture(scope="session")
def fugue_dask_client():
    from dask.distributed import Client
    import dask

    with Client(processes=True, n_workers=3, threads_per_worker=1) as client:
        dask.config.set({"dataframe.shuffle.method": "tasks"})
        dask.config.set({"dataframe.convert-string": False})
        yield client
