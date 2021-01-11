from abc import ABC, abstractmethod
from threading import RLock
from typing import Any, Callable, Dict
from uuid import uuid4

from fugue.rpc.collections import RPCFuncDict, to_rpc_func_dict
from triad import ParamDict, assert_or_throw
from triad.utils.convert import to_type
import pickle


class RPCClient(object):
    def __call__(self, method: str, value: str) -> str:  # pragma: no cover
        raise NotImplementedError


class RPCServer(ABC):
    def __init__(self, conf: Any):
        self._lock = RLock()
        self._conf = ParamDict(conf)
        self._services: Dict[str, RPCFuncDict] = {}
        self._running = 0

    @property
    def conf(self) -> ParamDict:
        return self._conf

    @abstractmethod
    def make_client(self, methods: Dict[str, Callable[[str], str]]) -> RPCClient:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def start_server(self) -> None:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def stop_server(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def invoke(self, key: str, method: str, value: str) -> str:
        with self._lock:
            handler = self._services[key][method]
        return handler(value)

    def add_methods(self, methods: Dict[str, Callable[[str], str]]) -> str:
        with self._lock:
            key = "_" + str(uuid4()).split("-")[-1]
            self._services[key] = to_rpc_func_dict(methods)
            return key

    def start(self) -> "RPCServer":
        with self._lock:
            if self._running == 0:
                self.start_server()
            self._running += 1
        return self

    def stop(self) -> None:
        with self._lock:
            if self._running == 1:
                self.stop_server()
            self._running -= 1
            if self._running < 0:
                self._running = 0

    def __enter__(self) -> "RPCServer":
        with self._lock:
            assert_or_throw(self._running, "use `with <instance>.start():` instead")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()

    def __getstate__(self):
        raise pickle.PicklingError(f"{self} is not serializable")


class NativeRPCClient(RPCClient):
    def __init__(self, server: "NativeRPCServer", key: str):
        self._key = key
        self._server = server

    def __call__(self, method: str, value: str) -> str:
        return self._server.invoke(self._key, method, value)

    def __getstate__(self):
        raise pickle.PicklingError(f"{self} is not serializable")


class NativeRPCServer(RPCServer):
    def __init__(self, conf: Any):
        super().__init__(conf)

    def make_client(self, methods: Dict[str, Callable[[str], str]]) -> RPCClient:
        key = self.add_methods(methods)
        return NativeRPCClient(self, key)

    def start_server(self) -> None:
        return

    def stop_server(self) -> None:
        return


def make_rpc_server(conf: Any) -> RPCServer:
    conf = ParamDict(conf)
    tp = conf.get_or_none("fugue.rpc.server", str)
    t_server = NativeRPCServer if tp is None else to_type(tp, RPCServer)
    return t_server(conf)  # type: ignore
