from abc import ABC, abstractmethod
from typing import Any, Optional
from triad import ParamDict, assert_or_throw
from .exceptions import FugueDatasetEmptyError


class Dataset(ABC):
    """The base class of Fugue :class:`~.fugue.dataframe.dataframe.DataFrame`
    and :class:`~.fugue.bag.bag.Bag`.

    .. note::

        This is for internal use only.
    """

    def __init__(self):
        self._metadata: Optional[ParamDict] = None

    @property
    def metadata(self) -> ParamDict:
        """Metadata of the dataset"""
        if self._metadata is None:
            self._metadata = ParamDict()
        return self._metadata

    @property
    def has_metadata(self) -> bool:
        """Whether this dataframe contains any metadata"""
        return self._metadata is not None and len(self._metadata) > 0

    def reset_metadata(self, metadata: Any) -> None:
        """Reset metadata"""
        self._metadata = ParamDict(metadata) if metadata is not None else None

    @property
    @abstractmethod
    def is_local(self) -> bool:  # pragma: no cover
        """Whether this dataframe is a :class:`.LocalDataFrame`"""
        raise NotImplementedError

    @property
    @abstractmethod
    def is_bounded(self) -> bool:  # pragma: no cover
        """Whether this dataframe is bounded"""
        raise NotImplementedError

    @property
    @abstractmethod
    def num_partitions(self) -> int:  # pragma: no cover
        """Number of physical partitions of this dataframe.
        Please read |PartitionTutorial|
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def empty(self) -> bool:  # pragma: no cover
        """Whether this dataframe is empty"""
        raise NotImplementedError

    @abstractmethod
    def count(self) -> int:  # pragma: no cover
        """Get number of rows of this dataframe"""
        raise NotImplementedError

    def assert_not_empty(self) -> None:
        """Assert this dataframe is not empty

        :raises FugueDatasetEmptyError: if it is empty
        """
        assert_or_throw(not self.empty, FugueDatasetEmptyError("dataframe is empty"))
