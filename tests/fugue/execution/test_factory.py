from typing import Any

from fugue import (
    NativeExecutionEngine,
    SqliteEngine,
    make_execution_engine,
    make_sql_engine,
    register_default_execution_engine,
    register_default_sql_engine,
    register_execution_engine,
    register_sql_engine,
)
from fugue.constants import FUGUE_CONF_SQL_IGNORE_CASE
from fugue.exceptions import FuguePluginsRegistrationError
from fugue.execution.factory import (
    make_execution_engine,
    make_sql_engine,
    parse_execution_engine,
    parse_sql_engine,
    register_default_execution_engine,
    register_default_sql_engine,
    register_execution_engine,
    register_sql_engine,
)
from pytest import raises


class _MockExecutionEngine(NativeExecutionEngine):
    def __init__(self, conf: Any, other: int = 0):
        super().__init__(conf=conf)
        self.other = other


class Dummy1:
    pass


class Dummy2(Dummy1):
    pass


class _MockExecutionEngine2(NativeExecutionEngine):
    def __init__(self, obj: Any, conf: Any, other: int = 0):
        assert isinstance(obj, Dummy2)
        super().__init__(conf=conf)
        self.other = other


class _MockSQlEngine(SqliteEngine):
    def __init__(self, execution_engine, other: int = 1):
        super().__init__(execution_engine)
        self.other = other


def test_execution_engine_alias():
    assert isinstance(make_execution_engine(), NativeExecutionEngine)

    c = make_execution_engine(conf={"a": 2})
    assert 2 == c.conf.get_or_throw("a", int)

    c = make_execution_engine(_MockExecutionEngine(conf={"a": 3}, other=4))
    assert isinstance(c, _MockExecutionEngine)
    assert 3 == c.conf.get_or_throw("a", int)
    assert 4 == c.other

    raises(FuguePluginsRegistrationError, lambda: make_execution_engine("xyz"))

    register_execution_engine(
        "__xyz", lambda conf, **kwargs: _MockExecutionEngine(conf, **kwargs)
    )
    c = make_execution_engine("__xyz")
    assert isinstance(c, _MockExecutionEngine)

    raises(
        ValueError,
        lambda: register_execution_engine(
            "__xyz",
            lambda conf, **kwargs: _MockExecutionEngine(conf, **kwargs),
            on_dup="dummy",
        ),
    )

    c = make_execution_engine("__xyz", conf={"a": 3}, other=4)
    assert isinstance(c, _MockExecutionEngine)
    assert 3 == c.conf.get_or_throw("a", int)
    assert 4 == c.other

    assert isinstance(make_execution_engine(), NativeExecutionEngine)

    register_default_execution_engine(
        lambda conf, **kwargs: _MockExecutionEngine(conf, **kwargs)
    )
    assert isinstance(make_execution_engine(), _MockExecutionEngine)

    c = make_execution_engine(conf={"a": 3}, other=4)
    assert isinstance(c, _MockExecutionEngine)
    assert 3 == c.conf.get_or_throw("a", int)
    assert 4 == c.other

    # MUST HAVE THIS STEP, or other tests will fail
    _reset()


def test_execution_engine_type():
    register_execution_engine(
        Dummy1,
        lambda engine, conf, **kwargs: _MockExecutionEngine2(engine, conf, **kwargs),
    )
    e = make_execution_engine(Dummy2(), {"b": 1}, other=2)
    assert isinstance(e, _MockExecutionEngine2)
    assert 1 == e.conf.get_or_throw("b", int)
    assert 2 == e.other


def test_sql_engine():
    assert not isinstance(
        make_sql_engine(None, make_execution_engine()), _MockSQlEngine
    )
    assert isinstance(
        make_sql_engine(_MockSQlEngine, make_execution_engine()), _MockSQlEngine
    )

    with raises(FuguePluginsRegistrationError):
        make_sql_engine("__dummy_no")

    register_execution_engine("__a", lambda conf: _MockExecutionEngine(conf))
    register_sql_engine("__aq", lambda engine: _MockSQlEngine(engine, other=11))
    e = make_execution_engine(("__a", "__aq"))
    assert isinstance(e, _MockExecutionEngine)
    assert isinstance(e.sql_engine, _MockSQlEngine)
    assert 0 == e.other
    assert 11 == e.sql_engine.other

    register_default_execution_engine(lambda conf: _MockExecutionEngine(conf))
    e = make_execution_engine()
    assert isinstance(e, _MockExecutionEngine)
    assert not isinstance(e.sql_engine, _MockSQlEngine)
    register_default_sql_engine(lambda engine: _MockSQlEngine(engine))
    e = make_execution_engine()
    assert isinstance(e, _MockExecutionEngine)
    assert isinstance(e.sql_engine, _MockSQlEngine)

    # SQL Engine override
    e = make_execution_engine(NativeExecutionEngine)
    assert type(e) == NativeExecutionEngine
    assert isinstance(e.sql_engine, _MockSQlEngine)

    # conditional override
    def to_sql_engine(engine):
        if isinstance(engine, _MockExecutionEngine):
            return _MockSQlEngine(engine)
        else:
            return engine.sql_engine

    register_default_sql_engine(to_sql_engine)
    e = make_execution_engine(NativeExecutionEngine)
    assert type(e) == NativeExecutionEngine
    assert type(e.sql_engine) != _MockSQlEngine

    e = make_execution_engine(_MockExecutionEngine)
    assert type(e) == _MockExecutionEngine
    assert type(e.sql_engine) == _MockSQlEngine

    # MUST HAVE THIS STEP, or other tests will fail
    _reset()


def test_global_funcs():
    assert isinstance(make_execution_engine(), NativeExecutionEngine)
    register_execution_engine(
        "xyz", lambda conf, **kwargs: _MockExecutionEngine(conf, **kwargs)
    )
    assert isinstance(make_execution_engine("xyz"), _MockExecutionEngine)
    register_default_execution_engine(
        lambda conf, **kwargs: _MockExecutionEngine(conf, **kwargs), on_dup="ignore"
    )
    assert not isinstance(make_execution_engine(), _MockExecutionEngine)
    register_default_execution_engine(
        lambda conf, **kwargs: _MockExecutionEngine(conf, **kwargs), on_dup="overwrite"
    )
    assert isinstance(make_execution_engine(), _MockExecutionEngine)

    se = SqliteEngine(make_execution_engine)
    assert make_sql_engine(se) is se
    assert not isinstance(
        make_sql_engine(None, make_execution_engine()), _MockSQlEngine
    )
    register_sql_engine("x", lambda engine: _MockSQlEngine(engine))
    assert isinstance(make_sql_engine("x", make_execution_engine()), _MockSQlEngine)
    register_default_sql_engine(lambda engine: _MockSQlEngine(engine, other=10))
    e = make_execution_engine()
    assert isinstance(e, _MockExecutionEngine)
    assert isinstance(e.sql_engine, _MockSQlEngine)
    assert 10 == e.sql_engine.other

    # MUST HAVE THIS STEP, or other tests will fail
    _reset()


def test_make_execution_engine():
    e = make_execution_engine(None, {FUGUE_CONF_SQL_IGNORE_CASE: True})
    assert isinstance(e, NativeExecutionEngine)
    assert e.compile_conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool)

    e = make_execution_engine(NativeExecutionEngine, {FUGUE_CONF_SQL_IGNORE_CASE: True})
    assert isinstance(e, NativeExecutionEngine)
    assert e.compile_conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool)

    e = make_execution_engine(
        NativeExecutionEngine({"ab": "c"}), {FUGUE_CONF_SQL_IGNORE_CASE: True}, de="f"
    )
    assert isinstance(e, NativeExecutionEngine)
    assert e.compile_conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool)
    assert "c" == e.compile_conf.get_or_throw("ab", str)
    assert "f" == e.compile_conf.get_or_throw("de", str)
    assert "c" == e.conf.get_or_throw("ab", str)
    assert "de" not in e.conf

    e = make_execution_engine("pandas", {FUGUE_CONF_SQL_IGNORE_CASE: True})
    assert isinstance(e, NativeExecutionEngine)
    assert e.compile_conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool)

    e = make_execution_engine(
        (NativeExecutionEngine, "sqlite"), {FUGUE_CONF_SQL_IGNORE_CASE: True}
    )
    assert isinstance(e, NativeExecutionEngine)
    assert e.compile_conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool)

    e = make_execution_engine(NativeExecutionEngine({FUGUE_CONF_SQL_IGNORE_CASE: True}))
    assert isinstance(e, NativeExecutionEngine)
    assert e.compile_conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool)

    e = make_execution_engine(
        (NativeExecutionEngine({FUGUE_CONF_SQL_IGNORE_CASE: True}), "sqlite")
    )
    e = make_execution_engine(e)
    assert isinstance(e, NativeExecutionEngine)
    assert e.compile_conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool)
    assert isinstance(e.sql_engine, SqliteEngine)

    assert isinstance(make_sql_engine(None, e), SqliteEngine)

    # MUST HAVE THIS STEP, or other tests will fail
    _reset()


def _reset():
    register_default_execution_engine(
        lambda conf, **kwargs: NativeExecutionEngine(conf)
    )
    register_default_sql_engine(lambda ee, **kwargs: ee.sql_engine)
