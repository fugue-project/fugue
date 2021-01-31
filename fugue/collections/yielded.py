from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid
from typing import Any


class Yielded(object):
    def __init__(self, yid: str):
        self._yid = to_uuid(yid)

    def __uuid__(self) -> str:
        return self._yid

    @property
    def is_set(self) -> bool:  # pragma: no cover
        raise NotImplementedError

    def __copy__(self) -> Any:  # pragma: no cover
        return self

    def __deepcopy__(self, memo: Any) -> Any:
        return self


class YieldedFile(Yielded):
    def __init__(self, yid: str):
        super().__init__(yid)
        self._path = ""

    @property
    def is_set(self) -> bool:
        return self._path != ""

    def set_value(self, path: str) -> None:
        self._path = path

    @property
    def path(self) -> str:
        assert_or_throw(self.is_set, "value is not set")
        return self._path
