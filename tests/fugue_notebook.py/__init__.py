from fugue_notebook.env import FugueSQLMagics
from fugue import register_execution_engine, register_default_execution_engine
from fugue import NativeExecutionEngine
from pytest import raises


class MockEngine(NativeExecutionEngine):
    def __init__(self, conf=None):
        super().__init__(conf=conf)


def test_fugue_sql_magic():
    register_execution_engine("m", lambda conf, **kwargs: MockEngine(conf=conf))
    m = FugueSQLMagics(None, {"a": 1}, {"b": 2})
    e = m.get_engine("   ", {})
    assert isinstance(e, NativeExecutionEngine)
    assert 1 == e.conf["a"]
    assert 2 == e.conf["b"]

    e = m.get_engine(' {"a":2,"c":3, "b":2}  ', {})
    assert isinstance(e, NativeExecutionEngine)
    assert 2 == e.conf["a"]
    assert 2 == e.conf["b"]
    assert 3 == e.conf["c"]

    with raises(ValueError):
        e = m.get_engine(' {"a":2,"b":1}  ', {})

    e = m.get_engine(" m   ", {})
    assert isinstance(e, MockEngine)
    assert 1 == e.conf["a"]
    assert 2 == e.conf["b"]

    e = m.get_engine(' m  {"a":2,"c":3, "b":2}  ', {})
    assert isinstance(e, MockEngine)
    assert 2 == e.conf["a"]
    assert 2 == e.conf["b"]
    assert 3 == e.conf["c"]