from fugue import NativeExecutionEngine
from fugue.rpc.base import NativeRPCServer
from triad.utils.convert import get_full_type_path


class _MockExecutionEngine(NativeExecutionEngine):
    def __init__(self, conf=None):
        super().__init__(conf=conf)
        self._start = 0
        self._stop = 0

    def start_engine(self):
        self._start += 1

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
    engine.start()
    engine.start()
    engine.stop()
    engine.stop()
    # second round
    engine.start()
    engine.stop()
    assert 2 == engine._start
    assert 2 == engine._stop
    assert 2 == _MockRPC._start
    assert 2 == _MockRPC._stop
