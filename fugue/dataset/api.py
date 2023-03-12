from typing import Any, Optional

from .._utils.registry import fugue_plugin
from .dataset import AnyDataset, Dataset


@fugue_plugin
def as_fugue_dataset(data: AnyDataset, **kwargs: Any) -> Dataset:
    """Wrap the input as a :class:`~.Dataset`

    :param data: the dataset to be wrapped
    """
    if isinstance(data, Dataset) and len(kwargs) == 0:
        return data
    raise NotImplementedError(f"no registered dataset conversion for {type(data)}")


def show(
    data: AnyDataset, n: int = 10, with_count: bool = False, title: Optional[str] = None
) -> None:
    """Display the Dataset

    :param data: the dataset that can be recognized by Fugue
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
def as_local(data: AnyDataset) -> AnyDataset:
    """Convert the dataset to a local dataset

    :param data: the dataset that can be recognized by Fugue
    """
    return as_local_bounded(data)


@fugue_plugin
def as_local_bounded(data: AnyDataset) -> AnyDataset:
    """Convert the dataset to a local bounded dataset

    :param data: the dataset that can be recognized by Fugue
    """
    raise NotImplementedError(
        f"no registered function to convert {type(data)} to a local bounded dataset"
    )


@fugue_plugin
def is_local(data: AnyDataset) -> bool:
    """Whether the dataset is local

    :param data: the dataset that can be recognized by Fugue
    """
    return as_fugue_dataset(data).is_local


@fugue_plugin
def is_bounded(data: AnyDataset) -> bool:
    """Whether the dataset is local

    :param data: the dataset that can be recognized by Fugue
    """
    return as_fugue_dataset(data).is_bounded


@fugue_plugin
def is_empty(data: AnyDataset) -> bool:
    """Whether the dataset is empty

    :param data: the dataset that can be recognized by Fugue
    """
    return as_fugue_dataset(data).empty


@fugue_plugin
def count(data: AnyDataset) -> int:
    """The number of elements in the dataset

    :param data: the dataset that can be recognized by Fugue
    """
    return as_fugue_dataset(data).count()


@fugue_plugin
def get_num_partitions(data: AnyDataset) -> bool:
    """Get the number of partitions of the dataset

    :param data: the dataset that can be recognized by Fugue
    """
    return as_fugue_dataset(data).num_partitions
