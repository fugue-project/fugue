from typing import Any, List, Iterable

from ..exceptions import FugueDatasetEmptyError
from .bag import LocalBag


class ArrayBag(LocalBag):
    def __init__(self, data: Any, metadata: Any = None, copy: bool = True):
        if isinstance(data, list):
            self._native = list(data) if copy else data
        elif isinstance(data, Iterable):
            self._native = list(data)
        else:
            raise ValueError(f"{type(data)} can't be converted to ArrayBag")
        super().__init__(metadata)

    @property
    def native(self) -> List[Any]:
        """The underlying Python list object"""
        return self._native

    @property
    def is_local(self) -> bool:
        return True

    @property
    def is_bounded(self) -> bool:
        return True

    @property
    def num_partitions(self) -> int:
        return 1

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

    def head(self, n: int) -> List[Any]:
        return self._native[:n]
