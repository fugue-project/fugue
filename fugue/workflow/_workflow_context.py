from threading import RLock
from typing import Any, Dict
from uuid import uuid4

from adagio.instances import (
    NoOpCache,
    ParallelExecutionEngine,
    WorkflowContext,
    WorkflowHooks,
)
from adagio.specs import WorkflowSpec
from fugue.constants import FUGUE_CONF_WORKFLOW_CONCURRENCY, FUGUE_DEFAULT_CONF
from fugue.dataframe import DataFrame
from fugue.execution.execution_engine import ExecutionEngine
from fugue.execution.factory import make_execution_engine
from fugue.workflow._checkpoint import CheckpointPath


class FugueWorkflowContext(WorkflowContext):
    def __init__(
        self,
        execution_engine: Any = None,
        cache: Any = NoOpCache,
        workflow_engine: Any = None,
        hooks: Any = WorkflowHooks,
    ):
        ee = make_execution_engine(execution_engine)
        self._fugue_engine = ee
        self._lock = RLock()
        self._results: Dict[Any, DataFrame] = {}
        self._execution_id = ""
        self._checkpoint_path = CheckpointPath(self.execution_engine)
        if workflow_engine is None:
            workflow_engine = ParallelExecutionEngine(
                self.execution_engine.conf.get(
                    FUGUE_CONF_WORKFLOW_CONCURRENCY,
                    FUGUE_DEFAULT_CONF[FUGUE_CONF_WORKFLOW_CONCURRENCY],
                ),
                self,
            )
        super().__init__(
            cache=cache,
            engine=workflow_engine,
            hooks=hooks,
            logger=ee.log,
            config=ee.conf,
        )

    def run(self, spec: WorkflowSpec, conf: Dict[str, Any]) -> None:
        try:
            self._execution_id = str(uuid4())
            self._checkpoint_path = CheckpointPath(self.execution_engine)
            self._checkpoint_path.init_temp_path(self._execution_id)
            self.execution_engine.start()
            super().run(spec, conf)
        finally:
            self._checkpoint_path.remove_temp_path()
            self._execution_id = ""
            self.execution_engine.stop()

    @property
    def checkpoint_path(self) -> CheckpointPath:
        return self._checkpoint_path

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._fugue_engine

    def set_result(self, key: Any, df: DataFrame) -> None:
        with self._lock:
            self._results[key] = df

    def get_result(self, key: Any) -> DataFrame:
        with self._lock:
            return self._results[key]
