from contextlib import contextmanager
from typing import Any, Dict, Iterator

import pytest

import fugue.test as ft
import fugue.api as fa
from fugue import NativeExecutionEngine

from fugue.test.pandas_backend import PandasTestBackend
from fugue_test import _parse_line


@ft.fugue_test_backend
class _MockPandasTestBackend(PandasTestBackend):
    name = "pandas_test_test"
    default_session_conf = {"a": 1, "aa": 2}
    default_fugue_conf = {"fugue.b": 2}

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        assert "a" in session_conf and session_conf["a"] == 3
        assert "aa" in session_conf and session_conf["aa"] == 2
        assert "b" not in session_conf
        yield "pandas"


@pytest.fixture(scope="session")
def f_dummy():
    return 1


@ft.with_backend("pandas")
def test_with_backend_0():
    pass


@ft.with_backend(("pandas_test_test", {"pandas_test_test.a": 3}), "pandas")
def test_with_backend_1(f_dummy):
    assert f_dummy == 1


@ft.with_backend(
    ("pandas_test_test", {"pandas_test_test.a": 3}), "pandas", "xyz", skip_missing=True
)
def test_with_backend_with_skip():
    pass


def test_with_backend_no_skip():
    with pytest.raises(ValueError):

        @ft.with_backend("xyz", skip_missing=False)
        def test_with_backend_with_skip():
            pass


@ft.with_backend(
    ("pandas_test_test", {"pandas_test_test.a": 3, "fugue.test.dummy": "dummy2"}),
    "pandas",
)
def test_with_backend_with_context(backend_context: ft.FugueTestContext, f_dummy):
    assert f_dummy == 1
    assert backend_context.name.startswith("pandas")
    if backend_context.name == "pandas":
        assert backend_context.engine.conf["fugue.test.dummy"] == "dummy"  # from ini
    else:  # pandas_test_test
        assert backend_context.engine.conf["fugue.test.dummy"] == "dummy2"  # from deco
        assert backend_context.engine.conf["fugue.b"] == 2  # from default


@ft.with_backend(("pandas_test_test", {"pandas_test_test.a": 3}), "pandas")
@pytest.mark.parametrize("x", [1, 2])
def test_with_backend_multiple_parameterize(x):
    assert x in (1, 2)


@pytest.mark.parametrize("x", [1, 2])
@ft.with_backend(("pandas_test_test", {"pandas_test_test.a": 3}), "pandas")
def test_with_backend_multiple_parameterize2(x):
    assert x in (1, 2)


def test_parse_line():
    assert _parse_line("a=1") == ("a", "1")
    assert _parse_line("a:bool=True") == ("a", True)
    assert _parse_line("a:bool=true") == ("a", True)
    assert _parse_line(" ab : bool = false ") == ("ab", False)
    assert _parse_line("a= ") == ("a", "")
    assert _parse_line(" \ta : int = 1 ") == ("a", 1)
    with pytest.raises(ValueError):
        _parse_line("a")
    with pytest.raises(ValueError):
        _parse_line("a : int")
    with pytest.raises(ValueError):
        _parse_line("a : int = a")
    with pytest.raises(ValueError):
        _parse_line(": int = 1")


@ft.fugue_test_suite("pandas", mark_test=True)
class TestTestSuite(ft.FugueTestSuite):
    def test_properties(self):
        assert self.context.name.startswith("pandas")
        assert self.engine.conf["fugue.test.dummy"] == "dummy"
        assert self.tmp_path is not None
        assert isinstance(fa.get_context_engine(), NativeExecutionEngine)
