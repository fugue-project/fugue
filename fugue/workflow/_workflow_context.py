from threading import RLock
from typing import Any, Dict, Tuple
from uuid import uuid4

from adagio.instances import (
    NoOpCache,
    ParallelExecutionEngine,
    WorkflowContext,
    WorkflowHooks,
    WorkflowResultCache,
)
from adagio.specs import WorkflowSpec
from fugue.constants import FUGUE_CONF_WORKFLOW_CONCURRENCY, FUGUE_DEFAULT_CONF
from fugue.dataframe import DataFrame
from fugue.execution.execution_engine import ExecutionEngine
from fugue.execution.native_execution_engine import NativeExecutionEngine
from fugue.workflow._checkpoint import CheckpointPath
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_instance


class FugueWorkflowContext(WorkflowContext):
    def __init__(
        self,
        execution_engine: Any = None,
        cache: Any = NoOpCache,
        workflow_engine: Any = None,
        hooks: Any = WorkflowHooks,
    ):
        if execution_engine is None:
            ee: ExecutionEngine = NativeExecutionEngine()
        else:
            ee = to_instance(execution_engine, ExecutionEngine)
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
            super().run(spec, conf)
        finally:
            self._checkpoint_path.remove_temp_path()
            self._execution_id = ""

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


class _FugueInteractiveWorkflowContext(FugueWorkflowContext):
    def __init__(
        self,
        execution_engine: Any = None,
        cache: Any = NoOpCache,
        workflow_engine: Any = None,
        hooks: Any = WorkflowHooks,
    ):
        super().__init__(
            cache=cache,
            workflow_engine=workflow_engine,
            hooks=hooks,
            execution_engine=execution_engine,
        )
        self._cache = _FugueInteractiveCache(self, self._cache)  # type: ignore


class _FugueInteractiveCache(WorkflowResultCache):
    """Fugue cache for interactive operations."""

    def __init__(self, wf_ctx: "WorkflowContext", cache: FugueWorkflowContext):
        super().__init__(wf_ctx)
        self._lock = RLock()
        self._data: Dict[str, Any] = {}
        self._cache = cache

    def set(self, key: str, value: Any) -> None:
        """Set `key` with `value`

        :param key: uuid string
        :param value: any value
        """
        with self._lock:
            self._data[key] = value
        self._cache.set(key, value)

    def skip(self, key: str) -> None:  # pragma: no cover
        """Skip `key`

        :param key: uuid string
        """
        raise InvalidOperationError("skip is not valid in FugueInteractiveCache")

    def get(self, key: str) -> Tuple[bool, bool, Any]:
        """Try to get value for `key`

        :param key: uuid string
        :return: <hasvalue>, <skipped>, <value>
        """
        with self._lock:
            if key in self._data:
                return True, False, self._data[key]
            has_value, skipped, value = self._cache.get(key)
            assert_or_throw(
                not skipped,
                InvalidOperationError("skip is not valid in FugueInteractiveCache"),
            )
            if has_value:
                self._data[key] = value
            return has_value, skipped, value
