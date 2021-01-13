import pickle

from fugue.rpc import make_rpc_server, to_rpc_handler, RPCFuncDict, RPCEmptyHandler
from pytest import raises
from triad import ParamDict


def test_default_server():
    def k(value: str) -> str:
        return value + "x"

    def kk(value: str) -> str:
        return value + "xx"

    conf = {"x": "y"}

    with make_rpc_server(conf).start() as server:
        assert "y" == server.conf["x"]
        with server.start():  # recursive start will take no effect
            client = server.make_client({"k": k})
        assert "dddx" == client("k", "ddd")
        client = server.make_client({"kk": kk})
        assert "dddxx" == client("kk", "ddd")
        server.stop()  # extra stop in the end will take no effect

    with raises(pickle.PicklingError):
        pickle.dumps(client)

    with raises(pickle.PicklingError):
        pickle.dumps(server)


def test_server_handlers():
    funcs1 = {
        "a": lambda x: x + "aa",
        "b": lambda x: x + "bb",
    }

    class _Dict(RPCFuncDict):
        def __init__(self, obj):
            super().__init__(obj)
            self.start_called = 0
            self.stop_called = 0

        def start_handler(self):
            self.start_called += 1

        def stop_handler(self):
            self.stop_called += 1

    server = make_rpc_server({})
    server.start()
    d1 = _Dict(funcs1)
    c1 = server.make_client(d1)
    assert "xaa" == c1("a", "x")
    assert 1 == d1.start_called
    assert 0 == d1.stop_called
    server.stop()
    assert 1 == d1.start_called
    assert 1 == d1.stop_called

    with server.start():
        d2 = _Dict(funcs1)
        c1 = server.make_client(d2)
        server.start()
        assert "xaa" == c1("a", "x")
        assert 1 == d2.start_called
        assert 0 == d2.stop_called
        assert 1 == d1.start_called
        assert 1 == d1.stop_called
        server.stop()
    assert 1 == d2.start_called
    assert 1 == d2.stop_called
    assert 1 == d1.start_called
    assert 1 == d1.stop_called


def test_to_rpc_handler():
    assert isinstance(to_rpc_handler(None), RPCEmptyHandler)
    assert isinstance(to_rpc_handler({}), RPCEmptyHandler)
    assert isinstance(to_rpc_handler({"a": lambda x: x}), RPCFuncDict)
    handler = make_rpc_server({})
    assert handler is to_rpc_handler(handler)
    raises(ValueError, lambda: to_rpc_handler({"x": 1}))
    raises(ValueError, lambda: to_rpc_handler(1))
