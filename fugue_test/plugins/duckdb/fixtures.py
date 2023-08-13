import pytest


@pytest.fixture(scope="session")
def fugue_duckdb_connection():
    import duckdb

    with duckdb.connect() as connection:
        yield connection
