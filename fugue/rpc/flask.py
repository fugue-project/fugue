from threading import Thread
from typing import Any, Callable, Dict, Optional

import requests
from fugue.rpc.base import RPCClient, RPCServer
from triad.utils.convert import to_timedelta
from werkzeug.serving import make_server

from flask import Flask, request


class FlaskRPCServer(RPCServer):
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

    def _invoke(self) -> str:
        key = request.form.get("key")
        method = request.form.get("method")
        value = request.form.get("value")
        return self.invoke(key, method, value)  # type: ignore

    def make_client(self, methods: Dict[str, Callable[[str], str]]) -> RPCClient:
        key = self.add_methods(methods)
        return FlaskRPCClient(
            key,
            self._host,
            self._port,
            self._timeout_sec,
        )

    def start_server(self) -> None:
        app = Flask("FlaskRPCServer")
        app.route("/invoke", methods=["POST"])(self._invoke)
        self._server = FlaskRPCServer._Thread(app, self._host, self._port)
        self._server.start()

    def stop_server(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.join()


class FlaskRPCClient(RPCClient):
    def __init__(self, key: str, host: str, port: int, timeout_sec: float):
        self._url = f"http://{host}:{port}/invoke"
        self._timeout_sec = timeout_sec
        self._key = key

    def __call__(self, method: str, value: str) -> str:
        timeout: Any = None if self._timeout_sec <= 0 else self._timeout_sec
        res = requests.post(
            self._url,
            data=dict(key=self._key, method=method, value=value),
            timeout=timeout,
        )
        res.raise_for_status()
        return res.text
