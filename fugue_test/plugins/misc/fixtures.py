import uuid

import pytest
from triad.utils.io import makedirs, rm


@pytest.fixture
def tmp_mem_dir():
    uuid_str = str(uuid.uuid4())[:5]
    path = "memory://test_" + uuid_str
    makedirs(path)
    try:
        yield path
    finally:
        try:
            rm(path, recursive=True)
        except Exception:  # pragma: no cover
            pass
