from pytest import raises

import fugue.api as fa
from fugue import NativeExecutionEngine, register_global_conf
from fugue.exceptions import FugueInvalidOperation


class MyEngine(NativeExecutionEngine):
    def __init__(self, conf=None):
        super().__init__(conf)
        self.pre_enter_state = []
        self.post_exit_state = []
        self.stop_calls = 0

    def on_enter_context(self) -> None:
        self.pre_enter_state += [self.in_context]

    def on_exit_context(self) -> None:
        self.post_exit_state += [self.in_context]

    def stop_engine(self) -> None:
        self.stop_calls += 1


def test_engine_operations():
    o = MyEngine()
    assert fa.get_current_conf().get("fugue.x", 0) == 0
    register_global_conf({"fugue.x": 1})
    assert fa.get_current_conf().get("fugue.x", 0) == 1
    e = fa.set_global_engine(o, {"fugue.x": 2})
    assert e.pre_enter_state == [False]
    assert e.post_exit_state == []
    assert fa.get_current_conf().get("fugue.x", 0) == 2
    assert isinstance(e, NativeExecutionEngine)
    assert e.in_context and e.is_global
    assert fa.get_context_engine() is e
    with fa.engine_context("duckdb", {"fugue.x": 3}) as e2:
        assert fa.get_current_conf().get("fugue.x", 0) == 3
        assert fa.get_context_engine() is e2
        assert not e2.is_global and e2.in_context
        with e.as_context():
            assert e.pre_enter_state == [False, True]
            assert e.post_exit_state == []
            assert fa.get_current_conf().get("fugue.x", 0) == 2
            assert not e2.is_global and e2.in_context
            assert e.in_context and e.is_global
            assert fa.get_context_engine() is e
            assert e.stop_calls == 0
        assert e.pre_enter_state == [False, True]
        assert e.post_exit_state == [True]
        assert fa.get_current_conf().get("fugue.x", 0) == 3
        assert e.in_context and e.is_global
        assert fa.get_context_engine() is e2
    assert e.stop_calls == 0
    assert e.pre_enter_state == [False, True]
    assert e.post_exit_state == [True]
    assert fa.get_current_conf().get("fugue.x", 0) == 2
    assert not e2.is_global and not e2.in_context
    assert e.in_context and e.is_global
    e3 = fa.set_global_engine("duckdb", {"fugue.x": 4})
    assert e.stop_calls == 1
    assert e.pre_enter_state == [False, True]
    assert e.post_exit_state == [True, False]
    assert fa.get_current_conf().get("fugue.x", 0) == 4
    assert not e.in_context and not e.is_global
    assert e3.in_context and e3.is_global
    fa.clear_global_engine()
    assert not e3.in_context and not e3.is_global
    assert fa.get_current_conf().get("fugue.x", 0) == 1
    raises(FugueInvalidOperation, lambda: fa.get_context_engine())
