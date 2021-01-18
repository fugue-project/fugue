from abc import ABC, abstractmethod
from threading import RLock
from typing import Any, Callable, Dict
from uuid import uuid4

from triad import ParamDict, assert_or_throw, to_uuid
from triad.utils.convert import to_type, get_full_type_path
import pickle
from types import LambdaType


class RPCClient(object):
    """RPC client interface"""

    def __call__(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


class RPCHandler(RPCClient):
    """RPC handler hosting the real logic on driver side"""

    def __init__(self):
        self._rpchandler_lock = RLock()
        self._running = 0

    @property
    def running(self) -> bool:
        """Whether the handler is in running state"""
        return self._running > 0

    def __uuid__(self) -> str:
        """UUID that can affect the determinism of the workflow"""
        return ""  # pragma: no cover

    def start_handler(self) -> None:
        """User implementation of starting the handler"""
        return

    def stop_handler(self) -> None:
        """User implementation of stopping the handler"""
        return

    def start(self) -> "RPCHandler":
        """Start the handler, wrapping :meth:`~.start_handler`

        :return: the instance itself
        """
        with self._rpchandler_lock:
            if self._running == 0:
                self.start_handler()
            self._running += 1
        return self

    def stop(self) -> None:
        """Stop the handler, wrapping :meth:`~.stop_handler`"""
        with self._rpchandler_lock:
            if self._running == 1:
                self.stop_handler()
            self._running -= 1
            if self._running < 0:
                self._running = 0

    def __enter__(self) -> "RPCHandler":
        """``with`` statement. :meth:`~.start` must be called

        :Examples:

        .. code-block:: python

            with handler.start():
                handler...
        """
        with self._rpchandler_lock:
            assert_or_throw(self._running, "use `with <instance>.start():` instead")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()

    def __getstate__(self):
        """
        :raises pickle.PicklingError: serialization
          of ``RPCHandler`` is not allowed
        """
        raise pickle.PicklingError(f"{self} is not serializable")

    def __copy__(self) -> "RPCHandler":
        """Copy takes no effect

        :return: the instance itself
        """
        return self

    def __deepcopy__(self, memo: Any) -> "RPCHandler":
        """Deep copy takes no effect

        :return: the instance itself
        """
        return self


class EmptyRPCHandler(RPCHandler):
    """The class representing empty :class:`~.RPCHandler`"""

    def __init__(self):
        super().__init__()


class RPCServer(RPCHandler, ABC):
    """Server abstract class hosting multiple :class:`~.RPCHandler`.

    :param conf: |FugueConfig|
    """

    def __init__(self, conf: Any):
        super().__init__()
        self._conf = ParamDict(conf)
        self._handlers: Dict[str, RPCHandler] = {}

    @property
    def conf(self) -> ParamDict:
        """Config initialized this instance"""
        return self._conf

    @abstractmethod
    def make_client(self, handler: Any) -> RPCClient:
        """Make a :class:`~.RPCHandler` and return the correspondent
        :class:`~.RPCClient`

        :param handler: |RPCHandlerLikeObject|
        :return: the client connecting to the handler
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def start_server(self) -> None:
        """User implementation of starting the server"""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def stop_server(self) -> None:
        """User implementation of stopping the server"""
        raise NotImplementedError  # pragma: no cover

    def start_handler(self) -> None:
        """Wrapper to start the server, do not override or call directly"""
        with self._rpchandler_lock:
            self.start_server()

    def stop_handler(self) -> None:
        """Wrapper to stop the server, do not override or call directly"""
        with self._rpchandler_lock:
            self.stop_server()
            for v in self._handlers.values():
                if v.running:
                    v.stop()
            self._handlers.clear()

    def invoke(self, key: str, *args: Any, **kwargs: Any) -> Any:
        """Invoke the correspondent handler

        :param key: key of the handler
        :return: the return value of the handler
        """
        with self._rpchandler_lock:
            handler = self._handlers[key]
        return handler(*args, **kwargs)

    def register(self, handler: Any) -> str:
        """Register the hander into the server

        :param handler: |RPCHandlerLikeObject|
        :return: the unique key of the handler
        """
        with self._rpchandler_lock:
            key = "_" + str(uuid4()).split("-")[-1]
            assert_or_throw(key not in self._handlers, f"{key} already exists")
            self._handlers[key] = to_rpc_handler(handler).start()
            return key


class NativeRPCClient(RPCClient):
    """Native RPC Client that can only be used locally.
    Use :meth:`~.NativeRPCServer.make_client` to create this instance.

    :param server: the parent :class:`~.NativeRPCServer`
    :param key: the unique key for the handler and this client
    """

    def __init__(self, server: "NativeRPCServer", key: str):
        self._key = key
        self._server = server

    def __call__(self, *args: Any, **kwargs: Any) -> str:
        return self._server.invoke(self._key, *args, **kwargs)

    def __getstate__(self):
        raise pickle.PicklingError(f"{self} is not serializable")


class NativeRPCServer(RPCServer):
    """Native RPC Server that can only be used locally.

    :param conf: |FugueConfig|
    """

    def __init__(self, conf: Any):
        super().__init__(conf)

    def make_client(self, handler: Any) -> RPCClient:
        """Add ``handler`` and correspondent :class:`~.NativeRPCClient`

        :param handler: |RPCHandlerLikeObject|
        :return: the native RPC client
        """
        key = self.register(handler)
        return NativeRPCClient(self, key)

    def start_server(self) -> None:
        """Do nothing"""
        return

    def stop_server(self) -> None:
        """Do nothing"""
        return


class RPCFunc(RPCHandler):
    """RPCHandler wrapping a python function.

    :param func: a python function
    """

    def __init__(self, func: Callable):
        super().__init__()
        assert_or_throw(callable(func), ValueError(func))
        self._func = func
        if isinstance(func, LambdaType):
            self._uuid = to_uuid("lambda")
        else:
            self._uuid = to_uuid(get_full_type_path(func))

    def __uuid__(self) -> str:
        """If the underlying function is a static function, then the full
        type path of the function determines the uuid, but for a lambda
        function, the uuid is a constant, so it could be overly
        deterministic

        :return: the unique id
        """
        return self._uuid

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._func(*args, **kwargs)


def to_rpc_handler(obj: Any) -> RPCHandler:
    """Convert object to :class:`~.RPCHandler`. If the object is already
    ``RPCHandler``, then the original instance will be returned.
    If the object is ``None`` then :class:`~.EmptyRPCHandler` will be returned.
    If the object is a python function then :class:`~.RPCFunc` will be returned.

    :param obj: |RPCHandlerLikeObject|
    :return: the RPC handler
    """
    if obj is None:
        return EmptyRPCHandler()
    if isinstance(obj, RPCHandler):
        return obj
    if callable(obj):
        return RPCFunc(obj)
    raise ValueError(obj)


def make_rpc_server(conf: Any) -> RPCServer:
    """Make :class:`~.RPCServer` based on configuration.
    If '`fugue.rpc.server`` is set, then the value will be used as
    the server type for the initialization. Otherwise, a
    :class:`~.NativeRPCServer` instance will be returned

    :param conf: |FugueConfig|
    :return: the RPC server
    """
    conf = ParamDict(conf)
    tp = conf.get_or_none("fugue.rpc.server", str)
    t_server = NativeRPCServer if tp is None else to_type(tp, RPCServer)
    return t_server(conf)  # type: ignore
