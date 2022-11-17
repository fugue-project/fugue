from types import GeneratorType
from typing import Any, Iterable, List

from ..exceptions import FugueDatasetEmptyError
from .bag import LocalBoundedBag


class ArrayBag(LocalBoundedBag):
    def __init__(self, data: Any, copy: bool = True):
        if isinstance(data, list):
            self._native = list(data) if copy else data
        elif isinstance(data, (GeneratorType, Iterable)):
            self._native = list(data)
        else:
            raise ValueError(f"{type(data)} can't be converted to ArrayBag")
        super().__init__()

    @property
    def native(self) -> List[Any]:
        """The underlying Python list object"""
        return self._native

    @property
    def empty(self) -> bool:
        return len(self._native) == 0

    def count(self) -> int:
        return len(self._native)

    def peek(self) -> Any:
        if self.count() == 0:
            raise FugueDatasetEmptyError()
        return self._native[0]

    def as_array(self) -> List[Any]:
        return list(self._native)

    def head(self, n: int) -> LocalBoundedBag:
        return ArrayBag(self._native[:n])
