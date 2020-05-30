from threading import RLock
from typing import Any, Dict

from adagio.instances import (
    NoOpCache,
    SequentialExecutionEngine,
    WorkflowContext,
    WorkflowHooks,
)
from fugue.dataframe import DataFrame
from fugue.execution.execution_engine import ExecutionEngine
from fugue.execution.native_execution_engine import NativeExecutionEngine
from triad.utils.convert import to_instance


class FugueWorkflowContext(WorkflowContext):
    def __init__(
        self,
        execution_engine: Any = None,
        cache: Any = NoOpCache,
        workflow_engine: Any = SequentialExecutionEngine,
        hooks: Any = WorkflowHooks,
    ):
        if execution_engine is None:
            ee: ExecutionEngine = NativeExecutionEngine()
        else:
            ee = to_instance(execution_engine, ExecutionEngine)
        super().__init__(
            cache=cache,
            engine=workflow_engine,
            hooks=hooks,
            logger=ee.log,
            config=ee.conf,
        )
        self._fugue_engine = ee
        self._lock = RLock()
        self._results: Dict[Any, DataFrame] = {}

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._fugue_engine

    def set_result(self, key: Any, df: DataFrame) -> None:
        with self._lock:
            self._results[key] = df

    def get_result(self, key: Any) -> DataFrame:
        with self._lock:
            return self._results[key]
