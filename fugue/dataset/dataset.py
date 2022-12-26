import html
from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

from triad import ParamDict, SerializableRLock, assert_or_throw

from .._utils.registry import fugue_plugin
from ..exceptions import FugueDatasetEmptyError


AnyDataset = TypeVar("AnyDataset", "Dataset", object)


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
    def native(self) -> Any:  # pragma: no cover
        """The native object this Dataset class wraps"""
        raise NotImplementedError

    @property
    @abstractmethod
    def is_local(self) -> bool:  # pragma: no cover
        """Whether this dataframe is a local Dataset"""
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

    def show(
        self, n: int = 10, with_count: bool = False, title: Optional[str] = None
    ) -> None:
        """Display the Dataset

        :param n: number of rows to print, defaults to 10
        :param with_count: whether to show dataset count, defaults to False
        :param title: title of the dataset, defaults to None

        .. note::

            When ``with_count`` is True, it can trigger expensive calculation for
            a distributed dataframe. So if you call this function directly, you may
            need to :func:`fugue.execution.execution_engine.ExecutionEngine.persist`
            the dataset.
        """
        return get_dataset_display(self).show(n=n, with_count=with_count, title=title)

    def __repr__(self):
        """String representation of the Dataset"""
        return get_dataset_display(self).repr()

    def _repr_html_(self):
        """HTML representation of the Dataset"""
        return get_dataset_display(self).repr_html()


class DatasetDisplay(ABC):
    """The base class for display handlers of :class:`~.Dataset`

    :param ds: the Dataset
    """

    _SHOW_LOCK = SerializableRLock()

    def __init__(self, ds: Dataset):
        self._ds = ds

    @abstractmethod
    def show(
        self, n: int = 10, with_count: bool = False, title: Optional[str] = None
    ) -> None:  # pragma: no cover
        """Show the :class:`~.Dataset`

        :param n: top n items to display, defaults to 10
        :param with_count: whether to display the total count, defaults to False
        :param title: title to display, defaults to None
        """
        raise NotImplementedError

    def repr(self) -> str:
        """The string representation of the :class:`~.Dataset`

        :return: the string representation
        """
        return str(type(self._ds).__name__)

    def repr_html(self) -> str:
        """The HTML representation of the :class:`~.Dataset`

        :return: the HTML representation
        """
        return html.escape(self.repr())


@fugue_plugin
def get_dataset_display(ds: "Dataset") -> DatasetDisplay:  # pragma: no cover
    """Get the display class to display a :class:`~.Dataset`

    :param ds: the Dataset to be displayed
    """

    raise NotImplementedError(f"no matching DatasetDisplay registered for {type(ds)}")
