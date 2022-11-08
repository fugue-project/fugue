from abc import ABC, abstractmethod
from typing import Any
from triad import ParamDict, assert_or_throw
from .exceptions import FugueDatasetEmptyError


class Dataset(ABC):
    """The base class of Fugue :class:`~.fugue.dataframe.dataframe.DataFrame`
    and :class:`~.fugue.bag.bag.Bag`.

    :param metadata: dict-like object with string keys, default ``None``

    .. note::

        This is for internal use only.
    """

    def __init__(self, metadata: Any = None):
        self._metadata = (
            metadata
            if isinstance(metadata, ParamDict)
            else ParamDict(metadata, deep=True)
        )
        self._metadata.set_readonly()

    @property
    def metadata(self) -> ParamDict:
        """Metadata of the dataset"""
        return self._metadata

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
