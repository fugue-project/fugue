import pytest

_DEFAULT_SCOPE = "module"


@pytest.fixture(scope=_DEFAULT_SCOPE)
def pandas_session():
    yield "pandas"


@pytest.fixture(scope=_DEFAULT_SCOPE)
def native_session():
    yield "native"


@pytest.fixture(scope=_DEFAULT_SCOPE)
def dask_session():
    from fugue_dask.tester import DaskTestBackend

    with DaskTestBackend.generate_session_fixture() as session:
        yield session


@pytest.fixture(scope=_DEFAULT_SCOPE)
def duckdb_session():
    from fugue_duckdb.tester import DuckDBTestBackend

    with DuckDBTestBackend.generate_session_fixture() as session:
        yield session


@pytest.fixture(scope=_DEFAULT_SCOPE)
def duckdask_session():
    from fugue_duckdb.tester import DuckDaskTestBackend

    with DuckDaskTestBackend.generate_session_fixture() as session:
        yield session
