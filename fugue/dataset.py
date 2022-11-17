from abc import ABC, abstractmethod
from typing import Any, Optional
from triad import ParamDict, assert_or_throw
from .exceptions import FugueDatasetEmptyError
from ._utils.registry import fugue_plugin


@fugue_plugin
def display_dataset(
    ds: "Dataset", n: int = 10, with_count: bool = False, title: Optional[str] = None
) -> None:  # pragma: no cover
    """General function to display a :class:`~.Dataset`

    .. admonition:: Example: how to register a custom display

        .. code-block:: python

            from fugue import display_dataset, DataFrame

            # higher priority will overwrite the existing display functions
            @display_dataset.candidate(
                lambda ds, *args, **kwargs: isinstance(ds, DataFrame), priority=1.0)
            def my_dataframe_display(ds, n=10, with_count=False, title=None):
                print(type(ds))

    :param ds: the Dataset to be displayed
    :param n: top n items to display, defaults to 10
    :param with_count: whether to display the total count, defaults to False
    :param title: title to display, defaults to None
    """

    raise NotImplementedError(f"No matching display function registered for {type(ds)}")


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

    def show(
        self, n: int = 10, with_count: bool = False, title: Optional[str] = None
    ) -> None:
        """Display the Dataset

        :param rows: number of rows to print, defaults to 10
        :param with_count: whether to show dataset count, defaults to False
        :param title: title of the dataset, defaults to None

        .. note::

            When ``with_count`` is True, it can trigger expensive calculation for
            a distributed dataframe. So if you call this function directly, you may
            need to :func:`fugue.execution.execution_engine.ExecutionEngine.persist`
            the dataset.
        """
        return display_dataset(self, n=n, with_count=with_count, title=title)

    def assert_not_empty(self) -> None:
        """Assert this dataframe is not empty

        :raises FugueDatasetEmptyError: if it is empty
        """
        assert_or_throw(not self.empty, FugueDatasetEmptyError("dataframe is empty"))
