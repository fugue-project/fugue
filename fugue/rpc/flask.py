import base64
import logging
import pickle
from threading import Thread
from typing import Any, Optional, Tuple, Dict, List

import requests
from fugue.rpc.base import RPCClient, RPCServer
from triad.utils.convert import to_timedelta
from werkzeug.serving import make_server

from flask import Flask, request

log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)


class FlaskRPCServer(RPCServer):
    """Flask RPC server that can be used in a distributed environment.
    It's required to set ``fugue.rpc.flask_server.host`` and
    ``fugue.rpc.flask_server.port``. If ``fugue.rpc.flask_server.timeout``
    is not set, then the client could hang until the connection is closed.

    :param conf: |FugueConfig|

    :Examples:

        .. code-block:: python

            conf = {
                "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
                "fugue.rpc.flask_server.host": "127.0.0.1",
                "fugue.rpc.flask_server.port": "1234",
                "fugue.rpc.flask_server.timeout": "2 sec",
            }

            with make_rpc_server(conf).start() as server:
                server...
    """

    class _Thread(Thread):
        def __init__(self, app: Flask, host: str, port: int):
            super().__init__()
            self._srv = make_server(host, port, app)
            self._ctx = app.app_context()
            self._ctx.push()

        def run(self) -> None:
            self._srv.serve_forever()

        def shutdown(self) -> None:
            self._srv.shutdown()

    def __init__(self, conf: Any):
        super().__init__(conf)
        self._host = conf.get_or_throw("fugue.rpc.flask_server.host", str)
        self._port = conf.get_or_throw("fugue.rpc.flask_server.port", int)
        timeout = conf.get_or_none("fugue.rpc.flask_server.timeout", object)
        self._timeout_sec = (
            -1.0 if timeout is None else to_timedelta(timeout).total_seconds()
        )
        self._server: Optional[FlaskRPCServer._Thread] = None

    def make_client(self, handler: Any) -> RPCClient:
        """Add ``handler`` and correspondent :class:`~.FlaskRPCClient`

        :param handler: |RPCHandlerLikeObject|
        :return: the flask RPC client that can be distributed
        """
        key = self.register(handler)
        return FlaskRPCClient(
            key,
            self._host,
            self._port,
            self._timeout_sec,
        )

    def start_server(self) -> None:
        """Start Flask RPC server"""
        app = Flask("FlaskRPCServer")
        app.route("/invoke", methods=["POST"])(self._invoke)
        self._server = FlaskRPCServer._Thread(app, self._host, self._port)
        self._server.start()

    def stop_server(self) -> None:
        """Stop Flask RPC server"""
        if self._server is not None:
            self._server.shutdown()
            self._server.join()

    def _invoke(self) -> str:
        key = str(request.form.get("key"))
        args, kwargs = _decode(str(request.form.get("value")))
        return _encode(self.invoke(key, *args, **kwargs))  # type: ignore


class FlaskRPCClient(RPCClient):
    """Flask RPC Client that can be used distributedly.
    Use :meth:`~.FlaskRPCServer.make_client` to create this instance.

    :param key: the unique key for the handler and this client
    :param host: the host address of the flask server
    :param port: the port of the flask server
    :param timeout_sec: timeout seconds for flask clients
    """

    def __init__(self, key: str, host: str, port: int, timeout_sec: float):
        self._url = f"http://{host}:{port}/invoke"
        self._timeout_sec = timeout_sec
        self._key = key

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the underlying function on the server side"""
        timeout: Any = None if self._timeout_sec <= 0 else self._timeout_sec
        res = requests.post(
            self._url,
            data=dict(key=self._key, value=_encode(*args, **kwargs)),
            timeout=timeout,
        )
        res.raise_for_status()
        return _decode(res.text)[0][0]


def _encode(*args: Any, **kwargs: Any) -> str:
    data = base64.b64encode(pickle.dumps(dict(args=args, kwargs=kwargs)))
    return data.decode("ascii")


def _decode(data: str) -> Tuple[List[Any], Dict[str, Any]]:
    data = pickle.loads(base64.b64decode(data.encode("ascii")))
    return data["args"], data["kwargs"]  # type: ignore
