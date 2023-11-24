import pytest


@pytest.fixture(scope="session")
def dask_session():
    from .tester import DaskTestBackend

    with DaskTestBackend.generate_session_fixture() as session:
        yield session
