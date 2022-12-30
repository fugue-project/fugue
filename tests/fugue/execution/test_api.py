import fugue.api as fa
from fugue import NativeExecutionEngine


def test_engine_operations():
    e = fa.set_global_engine("native")
    assert isinstance(e, NativeExecutionEngine)
    assert e.in_context and e.is_global
    assert fa.get_current_engine() is e
    with fa.engine_context("duckdb") as e2:
        assert fa.get_current_engine() is e2
        assert not e2.is_global and e2.in_context
        with e.as_context():
            assert not e2.is_global and e2.in_context
            assert e.in_context and e.is_global
            assert fa.get_current_engine() is e
        assert e.in_context and e.is_global
        assert fa.get_current_engine() is e2
    assert not e2.is_global and not e2.in_context
    assert e.in_context and e.is_global
    e3 = fa.set_global_engine("duckdb")
    assert not e.in_context and not e.is_global
    assert e3.in_context and e3.is_global
    fa.clear_global_engine()
    assert not e3.in_context and not e3.is_global
