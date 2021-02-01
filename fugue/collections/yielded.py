from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid
from typing import Any


class Yielded(object):
    """Yields from :class:`~fugue.workflow.workflow.FugueWorkflow`.
    Users shouldn't create this object directly.

    :param yid: unique id for determinism
    """

    def __init__(self, yid: str):
        self._yid = to_uuid(yid)

    def __uuid__(self) -> str:
        """uuid of the instance"""
        return self._yid

    @property
    def is_set(self) -> bool:  # pragma: no cover
        """Whether the value is set. It can be false if the parent workflow
        has not been executed.
        """
        raise NotImplementedError

    def __copy__(self) -> Any:  # pragma: no cover
        """``copy`` should have no effect"""
        return self

    def __deepcopy__(self, memo: Any) -> Any:
        """``deepcopy`` should have no effect"""
        return self


class YieldedFile(Yielded):
    """Yielded file from :class:`~fugue.workflow.workflow.FugueWorkflow`.
    Users shouldn't create this object directly.

    :param yid: unique id for determinism
    """

    def __init__(self, yid: str):
        super().__init__(yid)
        self._path = ""

    @property
    def is_set(self) -> bool:
        return self._path != ""

    def set_value(self, path: str) -> None:
        """Set the yielded path after compute

        :param path: file path
        """
        self._path = path

    @property
    def path(self) -> str:
        """File path of the yield"""
        assert_or_throw(self.is_set, "value is not set")
        return self._path
