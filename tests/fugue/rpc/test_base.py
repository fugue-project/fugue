import pickle

from fugue.rpc import make_rpc_server
from pytest import raises
from triad import ParamDict


def test_default_service():
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
