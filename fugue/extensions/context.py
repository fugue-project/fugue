from fugue.collections.partition import PartitionCursor, PartitionSpec
from fugue.execution.execution_engine import ExecutionEngine
from triad.collections import ParamDict, Schema
from triad.utils.convert import get_full_type_path
from triad.utils.hash import to_uuid


class ExtensionContext(object):
    @property
    def params(self) -> ParamDict:
        return self._params  # type: ignore

    @property
    def workflow_conf(self) -> ParamDict:
        if "_workflow_conf" in self.__dict__:
            return self._workflow_conf  # type: ignore
        return self.execution_engine.conf

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._execution_engine  # type: ignore

    @property
    def output_schema(self) -> Schema:
        return self._output_schema  # type: ignore

    @property
    def key_schema(self) -> Schema:
        return self._key_schema  # type: ignore

    @property
    def partition_spec(self) -> PartitionSpec:
        return self._partition_spec  # type: ignore

    @property
    def cursor(self) -> PartitionCursor:
        return self._cursor  # type: ignore

    def __uuid__(self) -> str:
        return to_uuid(get_full_type_path(self))
