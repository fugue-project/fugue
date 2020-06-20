from fugue.collections.partition import PartitionCursor, PartitionSpec
from fugue.execution.execution_engine import ExecutionEngine
from triad.collections import ParamDict, Schema
from triad.utils.convert import get_full_type_path
from triad.utils.hash import to_uuid


class ExtensionContext(object):
    """Context variables that extensions can access. It's also the base
    class of all extensions.
    """

    @property
    def params(self) -> ParamDict:
        """Parameters set for using this extension.

        :Examples:
        >>> FugueWorkflow().df(...).transform(using=dummy, params={"a": 1})

        You will get ``{"a": 1}`` as `params` in the ``dummy`` transformer
        """
        return self._params  # type: ignore

    @property
    def workflow_conf(self) -> ParamDict:
        """Workflow level configs, this is accessible even in
        :class:`~fugue.extensions.transformer.transformer.Transformer` and
        :class:`~fugue.extensions.transformer.transformer.CoTransformer`

        :Examples:
        >>> dag = FugueWorkflow().df(...).transform(using=dummy)
        >>> dag.run(NativeExecutionEngine(conf={"b": 10}))

        You will get ``{"b": 10}`` as `workflow_conf` in the ``dummy`` transformer
        on both driver and workers.
        """
        if "_workflow_conf" in self.__dict__:
            return self._workflow_conf  # type: ignore
        return self.execution_engine.conf

    @property
    def execution_engine(self) -> ExecutionEngine:
        """Execution engine for the current execution, this is only available on
        driver side
        """
        return self._execution_engine  # type: ignore

    @property
    def output_schema(self) -> Schema:
        """Output schema of the operation. This is accessible for all extensions (
        if defined), and on both driver and workers
        """
        return self._output_schema  # type: ignore

    @property
    def key_schema(self) -> Schema:
        """Partition keys schema, this is for transformers only, and available on both
        driver and workers
        """
        return self._key_schema  # type: ignore

    @property
    def partition_spec(self) -> PartitionSpec:
        """Partition specification, this is for all extensions except for creators,
        and available on both driver and workers
        """
        return self._partition_spec  # type: ignore

    @property
    def cursor(self) -> PartitionCursor:
        """Cursor of the current logical partition, this is for transformers only,
        and only available on worker side
        """
        return self._cursor  # type: ignore

    def __uuid__(self) -> str:
        return to_uuid(get_full_type_path(self))
