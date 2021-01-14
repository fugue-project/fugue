from fugue.rpc import make_rpc_server
from triad import ParamDict
import pickle
import fugue


def test_flask_service():
    # fugue.rpc.flask.FlaskRPCServer
    conf = ParamDict(
        {
            "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
            "fugue.rpc.flask_server.host": "127.0.0.1",
            "fugue.rpc.flask_server.port": "1234",
            "fugue.rpc.flask_server.timeout": "2 sec",
        }
    )

    def k(value: str) -> str:
        return value + "x"

    def kk(a: int, b: int) -> int:
        return a + b

    with make_rpc_server(conf).start() as server:
        assert "1234" == server.conf["fugue.rpc.flask_server.port"]
        with server.start():  # recursive start will take no effect
            client1 = pickle.loads(pickle.dumps(server.make_client(k)))
        assert "dddx" == client1("ddd")
        client2 = pickle.loads(pickle.dumps(server.make_client(kk)))
        assert 3 == client2(1, 2)
        assert "dddx" == client1("ddd")
        server.stop()  # extra stop in the end will take no effect