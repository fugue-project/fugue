from contextlib import contextmanager
from typing import Any, Dict, Iterator

import pytest

import fugue.test as ft
from .registry import *  # noqa: F401, F403  # pylint: disable-all


@ft.fugue_test_backend
class _MockIbisDuckDBTestBackend(ft.FugueTestBackend):
    name = "mockibisduck"

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        yield "mockibisduck"


@pytest.fixture(scope="module")
def mockibisduck_session():
    with _MockIbisDuckDBTestBackend.generate_session_fixture() as session:
        yield session
