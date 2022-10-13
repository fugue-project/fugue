from triad.collections.dict import ParamDict
from fugue import NativeExecutionEngine, register_global_conf
from fugue.constants import FUGUE_CONF_SQL_IGNORE_CASE
from fugue.rpc.base import NativeRPCServer
from pytest import raises
from triad.utils.convert import get_full_type_path


class _MockExecutionEngine(NativeExecutionEngine):
    def __init__(self, conf=None):
        super().__init__(conf=conf)
        self._stop = 0

    def stop_engine(self):
        self._stop += 1


class _MockRPC(NativeRPCServer):
    _start = 0
    _stop = 0

    def __init__(self, conf):
        super().__init__(conf)
        _MockRPC._start = 0
        _MockRPC._stop = 0

    def start_handler(self):
        _MockRPC._start += 1

    def stop_handler(self):
        _MockRPC._stop += 1


def test_start_stop():
    conf = {"fugue.rpc.server": get_full_type_path(_MockRPC)}
    engine = _MockExecutionEngine(conf=conf)
    engine.stop()
    assert 1 == engine._stop


def test_global_conf():
    register_global_conf({"ftest.a": 1})
    engine = _MockExecutionEngine()
    assert 1 == engine.conf.get_or_throw("ftest.a", int)
    engine = _MockExecutionEngine({"ftest.a": 2})
    assert 2 == engine.conf.get_or_throw("ftest.a", int)
    assert not engine.conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool)

    # with duplicated value but it's the same as existing ones
    register_global_conf({"ftest.a": 1, "ftest.b": 2}, on_dup=ParamDict.THROW)
    engine = _MockExecutionEngine()
    assert 1 == engine.conf.get_or_throw("ftest.a", int)
    assert 2 == engine.conf.get_or_throw("ftest.b", int)

    # transactional, of one value has problem, the whole conf will not be added
    with raises(ValueError):
        register_global_conf({"ftest.a": 2, "ftest.c": 3}, on_dup=ParamDict.THROW)
    assert 1 == engine.conf.get_or_throw("ftest.a", int)
    assert 2 == engine.conf.get_or_throw("ftest.b", int)
    assert "ftest.c" not in engine.conf
