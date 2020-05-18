from fugue.collections.partition import PartitionCursor, PartitionSpec
from fugue.execution.execution_engine import ExecutionEngine
from triad.collections import ParamDict, Schema


class ExtensionContext(object):
    @property
    def params(self) -> ParamDict:
        return self._params  # type: ignore

    @property
    def workflow_params(self) -> ParamDict:
        return self._workflow_params  # type: ignore

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
