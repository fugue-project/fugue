from typing import Any

from adagio.instances import (
    NoOpCache,
    SequentialExecutionEngine,
    WorkflowContext,
    WorkflowHooks,
)
from fugue.execution.execution_engine import ExecutionEngine


class FugueWorkflowContext(WorkflowContext):
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        cache: Any = NoOpCache,
        workflow_engine: Any = SequentialExecutionEngine,
        hooks: Any = WorkflowHooks,
    ):
        super().__init__(
            cache=cache,
            engine=workflow_engine,
            hooks=hooks,
            logger=execution_engine.log,
            config=execution_engine.conf,
        )
        self._fugue_engine = execution_engine

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._fugue_engine
