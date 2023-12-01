from typing import Any, Dict, Tuple

import pyarrow as pa
import pytest
from triad.utils.pyarrow import to_pa_datatype

_FUGUE_TEST_CONF_NAME = "fugue_test_conf"


def pytest_addoption(parser: Any):  # pragma: no cover
    parser.addini(
        _FUGUE_TEST_CONF_NAME,
        help="Configs for fugue testing execution engines",
        type="linelist",
    )


def pytest_configure(config: Any):
    from fugue.test.plugins import _set_global_conf

    options = config.getini(_FUGUE_TEST_CONF_NAME)
    conf: Dict[str, Any] = {}
    if options:
        for line in options:
            line = line.strip()
            if not line.startswith("#"):
                k, v = _parse_line(line)
                conf[k] = v
    _set_global_conf(conf)


def pytest_report_header(config, start_path):
    from fugue.test.plugins import _get_all_ini_conf

    header_lines = []
    header_lines.append("Fugue tests will be initialized with options:")
    for k, v in _get_all_ini_conf().items():
        header_lines.append(f"\t{k} = {v}")
    return "\n".join(header_lines)


def _parse_line(line: str) -> Tuple[str, Any]:
    try:
        kv = line.split("=", 1)
        if len(kv) == 1:
            raise ValueError()
        kt = kv[0].split(":", 1)
        if len(kt) == 1:
            tp = pa.string()
        else:
            tp = to_pa_datatype(kt[1].strip())
        key = kt[0].strip()
        if key == "":
            raise ValueError()
        value = pa.compute.cast([kv[1].strip()], tp).to_pylist()[0]
        return key, value
    except Exception:
        raise ValueError(
            f"Invalid config line: {line}, it must be in format: key[:type]=value"
        )


@pytest.fixture(scope="class")
def backend_context(request: Any):
    from fugue.test.plugins import _make_backend_context, _parse_backend

    c, _ = _parse_backend(request.param)
    session = request.getfixturevalue(c + "_session")
    with _make_backend_context(request.param, session) as ctx:
        yield ctx


@pytest.fixture(scope="class")
def _class_backend_context(request, backend_context):
    from fugue.test.plugins import FugueTestContext

    request.cls._test_context = FugueTestContext(
        engine=backend_context.engine,
        session=backend_context.session,
        name=backend_context.name,
    )
    yield
