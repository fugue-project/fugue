from typing import Any

from triad import assert_or_throw
from triad.utils.hash import to_uuid


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

    def __deepcopy__(self, memo: Any) -> Any:  # pragma: no cover
        """``deepcopy`` should have no effect"""
        return self


class PhysicalYielded(Yielded):
    """Physical yielded object from :class:`~fugue.workflow.workflow.FugueWorkflow`.
    Users shouldn't create this object directly.

    :param yid: unique id for determinism
    :param storage_type: ``file`` or ``table``
    """

    def __init__(self, yid: str, storage_type: str):
        super().__init__(yid)
        self._name = ""
        assert_or_throw(
            storage_type in ["file", "table"],
            ValueError(f"{storage_type} not in (file, table) "),
        )
        self._storage_type = storage_type

    @property
    def is_set(self) -> bool:
        return self._name != ""

    def set_value(self, name: str) -> None:
        """Set the storage name after compute

        :param name: name reference of the storage
        """
        self._name = name

    @property
    def name(self) -> str:
        """The name reference of the yield"""
        assert_or_throw(self.is_set, "value is not set")
        return self._name

    @property
    def storage_type(self) -> str:
        """The storage type of this yield"""
        return self._storage_type
