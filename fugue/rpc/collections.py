from typing import Any, Callable, Iterable, Tuple, Dict
from uuid import uuid4

from triad import IndexedOrderedDict, assert_or_throw


class RPCFuncDict(IndexedOrderedDict[str, Callable[[str], str]]):
    def __init__(self, data: Dict[str, Callable[[str], str]]):
        if isinstance(data, RPCFuncDict):
            super().__init__(data)
            self._uuid = data.__uuid__()
        else:
            super().__init__(self.get_tuples(data))
            self._uuid = str(uuid4())
        self.set_readonly()

    def __uuid__(self) -> str:
        return self._uuid

    def get_tuples(
        self, data: Dict[str, Callable[[str], str]]
    ) -> Iterable[Tuple[str, Callable[[str], str]]]:
        for k, v in sorted([(k, v) for k, v in data.items()], key=lambda p: p[0]):
            assert_or_throw(callable(v), ValueError(k, v))
            yield k, v

    def __copy__(self) -> "RPCFuncDict":
        return self

    def __deepcopy__(self, memo: Any) -> "RPCFuncDict":
        return self


def to_rpc_func_dict(obj: Any) -> RPCFuncDict:
    if isinstance(obj, RPCFuncDict):
        return obj
    return RPCFuncDict(obj)
