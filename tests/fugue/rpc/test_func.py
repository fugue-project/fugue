from fugue.rpc import RPCFunc, to_rpc_handler
from pytest import raises
from triad import to_uuid
from copy import copy, deepcopy


def test_rpc_func():
    def f1(a: str) -> str:
        return "1"

    d1 = RPCFunc(f1)
    d2 = to_rpc_handler(f1)
    assert to_uuid(d1) == to_uuid(d2)
    assert to_uuid(d1) == to_uuid(to_rpc_handler(d1))
    assert "1" == d1("x")
    with raises(ValueError):
        RPCFunc(1)


def test_determinism():
    def _f1(a: str) -> str:
        return "1"

    assert to_uuid(RPCFunc(_f1)) == to_uuid(to_rpc_handler(_f1))
    assert to_uuid(RPCFunc(lambda x: x)) == to_uuid(RPCFunc(lambda x: x + 1))


def test_no_copy():
    class T(object):
        def __init__(self):
            self.n = 0

        def call(self, n: int) -> int:
            self.n += n
            return self.n

    t = T()
    d1 = RPCFunc(t.call)
    assert 10 == d1(10)
    assert 10 == t.n

    d2 = to_rpc_handler(t.call)
    d2(10)

    d3 = to_rpc_handler(d1)
    d3(10)
    assert 30 == t.n

    d4 = copy(d3)
    d4(10)

    d5 = deepcopy(d4)
    d5(10)
    assert 50 == t.n
