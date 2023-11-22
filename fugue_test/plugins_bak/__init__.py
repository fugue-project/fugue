from contextlib import contextmanager
from typing import Any, Dict, Iterator, Type
from fugue import ExecutionEngine
import fugue.api as fa
import pytest

_FUGUE_TEST_CONF_NAME = "fugue_test_conf"
_FUGUE_TEST_PARSERS: Dict[str, Type["FugueTester"]] = {}


def register_fugue_tester(name: str) -> Any:
    def _register(cls: Type[FugueTester]):
        _FUGUE_TEST_PARSERS[name] = cls
        return cls

    return _register


@pytest.fixture(scope="session")
def fugue_engine_context(request):
    key = request.param
    if key not in _FUGUE_TEST_PARSERS:
        raise ValueError(
            f"Unknown Fugue Tester: {key}, "
            f"available testers: {list(_FUGUE_TEST_PARSERS.keys())}"
        )
    with _FUGUE_TEST_PARSERS[key].engine_context() as engine:
        yield engine


def with_fugue_engine_context(ctx: str, *other: str) -> Any:
    _ctx = list(set([ctx] + list(other)))
    for c in _ctx:
        if c not in _FUGUE_TEST_PARSERS:
            raise ValueError(
                f"Unknown Fugue Tester: {c}, "
                f"available testers: {list(_FUGUE_TEST_PARSERS.keys())}"
            )
    return lambda f: pytest.mark.parametrize(
        "fugue_engine_context", _ctx, indirect=True
    )(pytest.mark.usefixtures("fugue_engine_context")(f))


class FugueTester:
    default_conf: Dict[str, Any] = {}
    conf: Dict[str, Any] = {}

    @classmethod
    def transform_conf(cls, conf: Dict[str, Any]) -> Dict[str, Any]:
        return conf

    @classmethod
    @contextmanager
    def context(cls) -> Iterator[Any]:
        raise NotImplementedError

    @classmethod
    def parse_conf(cls, conf: Dict[str, Any]) -> None:
        cls.conf = cls.default_conf.copy()
        cls.conf.update(cls.transform_conf(conf))

    @classmethod
    @contextmanager
    def engine_context(cls) -> Iterator[ExecutionEngine]:
        with cls.context() as session:
            with fa.engine_context(session) as engine:
                yield engine


def pytest_addoption(parser: Any):
    parser.addini(
        _FUGUE_TEST_CONF_NAME,
        help="Configs for fugue testing execution engine",
        type="linelist",
    )


def pytest_configure(config: Any):
    options = config.getini(_FUGUE_TEST_CONF_NAME)
    conf: Dict[str, Any] = {}
    if options:
        for line in options:
            kv = line.split(":", 1)
            conf[kv[0].strip()] = _infer(kv[1].strip())
    for _, v in _FUGUE_TEST_PARSERS.items():
        v.parse_conf(conf)


def pytest_report_header(config, start_path):
    header_lines = []
    for k, v in _FUGUE_TEST_PARSERS.items():
        header_lines.append(f"'{k}' will be initialized with options:")
        for k in sorted(v.conf.keys()):
            header_lines.append("  %s: %s" % (k, v.conf[k]))
        header_lines.append("")

    return "\n".join(header_lines)


def _infer(v: str) -> Any:
    if v.lower() in ["true", "false"]:
        return bool(v.lower())
    if "." in v:
        try:
            return float(v)
        except Exception:
            return v
    else:
        try:
            return int(v)
        except Exception:
            return v
