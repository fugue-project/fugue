from abc import ABC, abstractmethod
from threading import RLock
from typing import Any, Callable, Dict
from uuid import uuid4

from triad import ParamDict, assert_or_throw, to_uuid
from triad.utils.convert import to_type, get_full_type_path
import pickle
from types import LambdaType


class RPCClient(object):
    def __call__(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


class RPCHandler(RPCClient):
    def __init__(self):
        self._lock = RLock()
        self._running = 0

    @property
    def running(self) -> bool:
        return self._running > 0

    def __uuid__(self) -> str:
        return ""  # pragma: no cover

    def start_handler(self) -> None:
        return

    def stop_handler(self) -> None:
        return

    def start(self) -> "RPCHandler":
        with self._lock:
            if self._running == 0:
                self.start_handler()
            self._running += 1
        return self

    def stop(self) -> None:
        with self._lock:
            if self._running == 1:
                self.stop_handler()
            self._running -= 1
            if self._running < 0:
                self._running = 0

    def __enter__(self) -> "RPCHandler":
        with self._lock:
            assert_or_throw(self._running, "use `with <instance>.start():` instead")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()

    def __getstate__(self):
        raise pickle.PicklingError(f"{self} is not serializable")

    def __copy__(self) -> "RPCHandler":
        return self

    def __deepcopy__(self, memo: Any) -> "RPCHandler":
        return self


class RPCEmptyHandler(RPCHandler):
    def __init__(self):
        super().__init__()


class RPCServer(RPCHandler, ABC):
    def __init__(self, conf: Any):
        super().__init__()
        self._conf = ParamDict(conf)
        self._handlers: Dict[str, RPCHandler] = {}

    @property
    def conf(self) -> ParamDict:
        return self._conf

    @abstractmethod
    def make_client(self, handler: Any) -> RPCClient:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def start_server(self) -> None:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def stop_server(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def start_handler(self) -> None:
        with self._lock:
            self.start_server()

    def stop_handler(self) -> None:
        with self._lock:
            self.stop_server()
            for v in self._handlers.values():
                if v.running:
                    v.stop()
            self._handlers.clear()

    def invoke(self, key: str, *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            handler = self._handlers[key]
        return handler(*args, **kwargs)

    def register(self, handler: Any) -> str:
        with self._lock:
            key = "_" + str(uuid4()).split("-")[-1]
            assert_or_throw(key not in self._handlers, f"{key} already exists")
            self._handlers[key] = to_rpc_handler(handler).start()
            return key


class NativeRPCClient(RPCClient):
    def __init__(self, server: "NativeRPCServer", key: str):
        self._key = key
        self._server = server

    def __call__(self, *args: Any, **kwargs: Any) -> str:
        return self._server.invoke(self._key, *args, **kwargs)

    def __getstate__(self):
        raise pickle.PicklingError(f"{self} is not serializable")


class NativeRPCServer(RPCServer):
    def __init__(self, conf: Any):
        super().__init__(conf)

    def make_client(self, handler: Any) -> RPCClient:
        key = self.register(handler)
        return NativeRPCClient(self, key)

    def start_server(self) -> None:
        return

    def stop_server(self) -> None:
        return


class RPCFunc(RPCHandler):
    def __init__(self, func: Callable):
        super().__init__()
        assert_or_throw(callable(func), ValueError(func))
        self._func = func
        if isinstance(func, LambdaType):
            self._uuid = to_uuid("lambda")
        else:
            self._uuid = to_uuid(get_full_type_path(func))

    def __uuid__(self) -> str:
        return self._uuid

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._func(*args, **kwargs)


def to_rpc_handler(obj: Any) -> RPCHandler:
    if obj is None:
        return RPCEmptyHandler()
    if isinstance(obj, RPCHandler):
        return obj
    if callable(obj):
        return RPCFunc(obj)
    raise ValueError(obj)


def make_rpc_server(conf: Any) -> RPCServer:
    conf = ParamDict(conf)
    tp = conf.get_or_none("fugue.rpc.server", str)
    t_server = NativeRPCServer if tp is None else to_type(tp, RPCServer)
    return t_server(conf)  # type: ignore
