from typing import Any, Dict
from uuid import uuid4

from adagio.instances import (
    NoOpCache,
    ParallelExecutionEngine,
    WorkflowContext,
    WorkflowHooks,
)
from adagio.specs import WorkflowSpec
from fugue.constants import FUGUE_CONF_WORKFLOW_CONCURRENCY
from fugue.dataframe import DataFrame
from fugue.execution.execution_engine import ExecutionEngine
from fugue.rpc.base import make_rpc_server, RPCServer
from fugue.workflow._checkpoint import CheckpointPath
from triad import SerializableRLock, ParamDict


class FugueWorkflowContext(WorkflowContext):
    def __init__(
        self,
        engine: ExecutionEngine,
        compile_conf: Any = None,
        cache: Any = NoOpCache,
        workflow_engine: Any = None,
        hooks: Any = WorkflowHooks,
    ):
        conf = ParamDict(compile_conf)
        self._fugue_engine = engine
        self._lock = SerializableRLock()
        self._results: Dict[Any, DataFrame] = {}
        self._execution_id = ""
        self._checkpoint_path = CheckpointPath(self.execution_engine)
        self._rpc_server = make_rpc_server(engine.conf)
        if workflow_engine is None:
            workflow_engine = ParallelExecutionEngine(
                conf.get_or_throw(FUGUE_CONF_WORKFLOW_CONCURRENCY, int),
                self,
            )
        super().__init__(
            cache=cache,
            engine=workflow_engine,
            hooks=hooks,
            logger=self.execution_engine.log,
            config=conf,
        )

    def run(self, spec: WorkflowSpec, conf: Dict[str, Any]) -> None:
        try:
            self._execution_id = str(uuid4())
            self._checkpoint_path = CheckpointPath(self.execution_engine)
            self._checkpoint_path.init_temp_path(self._execution_id)
            self._rpc_server.start()
            super().run(spec, conf)
        finally:
            self._checkpoint_path.remove_temp_path()
            self._rpc_server.stop()
            self._execution_id = ""

    @property
    def checkpoint_path(self) -> CheckpointPath:
        return self._checkpoint_path

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._fugue_engine

    @property
    def rpc_server(self) -> RPCServer:
        return self._rpc_server

    def set_result(self, key: Any, df: DataFrame) -> None:
        with self._lock:
            self._results[key] = df

    def get_result(self, key: Any) -> DataFrame:
        with self._lock:
            return self._results[key]
