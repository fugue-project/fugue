from contextlib import contextmanager
from typing import Any, Dict, Iterator

from .plugins import FugueTestBackend, fugue_test_backend


@fugue_test_backend
class PandasTestBackend(FugueTestBackend):
    name = "pandas"

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        yield "pandas"  # pragma: no cover


@fugue_test_backend
class NativeTestBackend(FugueTestBackend):
    name = "native"

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        yield "native"  # pragma: no cover
