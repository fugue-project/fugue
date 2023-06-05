from abc import abstractmethod
from typing import Any, List, Optional

from ..dataset import Dataset, DatasetDisplay, get_dataset_display


class Bag(Dataset):
    """The base class of Fugue Bags. Bag contains a collection of
    unordered objects.
    """

    def as_local(self) -> "LocalBag":  # pragma: no cover
        """Convert this bag to a :class:`.LocalBag`"""
        return self.as_local_bounded()

    @abstractmethod
    def as_local_bounded(self) -> "LocalBoundedBag":  # pragma: no cover
        """Convert this bag to a :class:`.LocalBoundedBag`"""
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
    def head(self, n: int) -> "LocalBoundedBag":  # pragma: no cover
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

    @property
    def num_partitions(self) -> int:
        return 1


class LocalBoundedBag(LocalBag):
    @property
    def is_bounded(self) -> bool:
        return True

    def as_local_bounded(self) -> "LocalBoundedBag":
        return self


class BagDisplay(DatasetDisplay):
    """:class:`~.Bag` plain display class"""

    @property
    def bg(self) -> Bag:
        """The target :class:`~.Bag`"""
        return self._ds  # type: ignore

    def show(
        self, n: int = 10, with_count: bool = False, title: Optional[str] = None
    ) -> None:
        head_rows = self.bg.head(n).as_array()
        if len(head_rows) < n:
            count = len(head_rows)
        else:
            count = self.bg.count() if with_count else -1
        with DatasetDisplay._SHOW_LOCK:
            if title is not None and title != "":
                print(title)
            print(type(self.bg).__name__)
            print(head_rows)
            if count >= 0:
                print(f"Total count: {count}")
                print("")
            if self.bg.has_metadata:
                print("Metadata:")
                try:
                    # try pretty print, but if not convertible to json, print original
                    print(self.bg.metadata.to_json(indent=True))
                except Exception:  # pragma: no cover
                    print(self.bg.metadata)
                print("")


@get_dataset_display.candidate(lambda ds: isinstance(ds, Bag), priority=0.1)
def _get_bag_display(ds: Bag):
    return BagDisplay(ds)
