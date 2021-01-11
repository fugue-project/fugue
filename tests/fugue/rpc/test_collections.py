from fugue.rpc import RPCFuncDict, to_rpc_func_dict
from pytest import raises
from triad import to_uuid
from copy import copy, deepcopy


def test_rpc_func_dict():
    def f1(a: str) -> str:
        return "1"

    def f2(a: str) -> str:
        return "2"

    def f3(a: str) -> str:
        return "3"

    d1 = RPCFuncDict({"f1": f1, "f2": f2, "f3": f3})
    d2 = to_rpc_func_dict({"f1": f1, "f3": f3, "f2": f2})
    assert to_uuid(d1) != to_uuid(d2)
    assert to_uuid(d1) == to_uuid(RPCFuncDict(d1))
    assert "1" == d1["f1"]("x")
    assert "2" == d2["f2"]("y")
    with raises(ValueError):
        RPCFuncDict({"f2": 123})


def test_determinism():
    def f1(a: str) -> str:
        return "1"

    def f2(a: str) -> str:
        return "2"

    def f3(a: str) -> str:
        return "3"

    class DeterministicRPCFuncDict(RPCFuncDict):
        def __init__(self, obj):
            super().__init__(obj)

        def __uuid__(self):
            return "x"

    d1 = DeterministicRPCFuncDict({"f1": f1, "f2": f2, "f3": f3})
    d2 = to_rpc_func_dict(DeterministicRPCFuncDict({"f3": f3}))
    assert to_uuid(d1) == to_uuid(d2)


def test_no_copy():
    class T(object):
        def __init__(self):
            self.n = 0

        def call(self, n: str) -> str:
            self.n += int(n)
            return str(self.n)

    t = T()
    d1 = RPCFuncDict({"t": t.call})
    assert "10" == d1["t"]("10")
    assert 10 == t.n

    d2 = to_rpc_func_dict({"t": t.call})
    d2["t"]("10")

    d3 = RPCFuncDict(d1)
    d3["t"]("10")
    assert 30 == t.n

    d4 = copy(d3)
    d4["t"]("10")

    d5 = deepcopy(d4)
    d5["t"]("10")
    assert 50 == t.n
