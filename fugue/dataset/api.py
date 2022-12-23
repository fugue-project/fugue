from typing import Any, Optional

from .._utils.registry import fugue_plugin
from .dataset import Dataset


@fugue_plugin
def as_fugue_dataset(data: Any) -> Dataset:
    """Wrap the input as a :class:`~.Dataset`

    :param data: the data to be wrapped
    """
    if isinstance(data, Dataset):
        return data
    raise NotImplementedError(f"no registered dataset conversion for {type(data)}")


def show(
    data: Any, n: int = 10, with_count: bool = False, title: Optional[str] = None
) -> None:
    """Display the Dataset

    :param data: the data that can be recognized by Fugue
    :param n: number of rows to print, defaults to 10
    :param with_count: whether to show dataset count, defaults to False
    :param title: title of the dataset, defaults to None

    .. note::

        When ``with_count`` is True, it can trigger expensive calculation for
        a distributed dataframe. So if you call this function directly, you may
        need to :func:`fugue.execution.execution_engine.ExecutionEngine.persist`
        the dataset.
    """
    return as_fugue_dataset(data).show(n=n, with_count=with_count, title=title)


@fugue_plugin
def is_local(data: Any) -> bool:
    """Whether the dataset is local

    :param data: the data that can be recognized by Fugue
    """
    return as_fugue_dataset(data).is_local


@fugue_plugin
def is_bounded(data: Any) -> bool:
    """Whether the dataset is local

    :param data: the data that can be recognized by Fugue
    """
    return as_fugue_dataset(data).is_bounded


@fugue_plugin
def is_empty(data: Any) -> bool:
    """Whether the dataset is empty

    :param data: the data that can be recognized by Fugue
    """
    return as_fugue_dataset(data).empty


@fugue_plugin
def count(data: Any) -> int:
    """The number of elements in the dataset

    :param data: the data that can be recognized by Fugue
    """
    return as_fugue_dataset(data).count()
