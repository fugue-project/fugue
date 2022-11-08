from abc import abstractmethod
from typing import Any, List

from ..dataset import Dataset


class Bag(Dataset):
    """The base class of Fugue Bags.
    Bag <https://fugue-tutorials.readthedocs.io/>
     Bag contains a collection of
    objects,
    """

    @abstractmethod
    def as_local(self) -> "LocalBag":  # pragma: no cover
        """Convert this bag to a :class:`.LocalBag`"""
        raise NotImplementedError

    @abstractmethod
    def peek(self) -> Any:  # pragma: no cover
        """Peek the first row of the dataframe as array

        :raises FugueDatasetEmptyError: if it is empty
        """
        raise NotImplementedError

    @abstractmethod
    def as_array(self) -> List[Any]:  # pragma: no cover
        """Convert to a native python array

        :return: the native python array
        """
        raise NotImplementedError

    @abstractmethod
    def head(self, n: int) -> List[Any]:  # pragma: no cover
        """Take the first n elements

        :return: the python array of the first n elements
        """
        raise NotImplementedError

    def __copy__(self) -> "Bag":
        return self

    def __deepcopy__(self, memo: Any) -> "Bag":
        return self


class LocalBag(Bag):
    @property
    def is_local(self) -> bool:
        return True

    def as_local(self) -> "LocalBag":
        return self
