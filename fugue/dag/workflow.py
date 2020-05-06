from typing import Any, Dict, List, TypeVar

from adagio.specs import WorkflowSpec
from fugue.builtins import CreateData, Show
from fugue.builtins import AssertEqual
from fugue.builtins.processors import RunTransformer
from fugue.collections.partition import PartitionSpec
from fugue.dag.tasks import Create, FugueTask, Output, Process
from fugue.execution.execution_engine import ExecutionEngine
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_or_throw

_DEFAULT_IGNORE_ERRORS: List[Any] = []

TDF = TypeVar("TDF", bound="WorkflowDataFrame")


class WorkflowDataFrame(object):
    def __init__(
        self, workflow: "FugueWorkflow", task: FugueTask, metadata: Any = None
    ):
        self._workflow = workflow
        self._task = task
        self._metadata = ParamDict(metadata)

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self.workflow.execution_engine

    @property
    def workflow(self) -> "FugueWorkflow":
        return self._workflow

    def show(self, rows: int = 10, count: bool = False, title: str = "") -> None:
        task = Output(
            1,
            self.execution_engine,
            outputter=Show,
            pre_partition=None,
            params=dict(rows=rows, count=count, title=title),
        )
        self.workflow.add(task, self)

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        self.workflow.assert_eq(self, *dfs, **params)

    def transform(
        self: TDF,
        using: Any,
        schema: Any = None,
        params: Any = None,
        partition: Any = None,
        ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
        lazy: bool = True,
    ) -> TDF:
        if partition is None:
            partition = self._metadata.get("pre_partition", PartitionSpec())
        task = Process(
            1,
            self.execution_engine,
            RunTransformer,
            schema=None,
            params=dict(
                transformer=using,
                schema=schema,
                ignore_errors=ignore_errors,
                params=params,
            ),
            pre_partition=partition,
            lazy=lazy,
        )
        return self.to_self_type(self.workflow.add(task, self))

    def persist(self: TDF, level: Any = None) -> TDF:
        self._task.persist("" if level is None else level)
        return self

    def broadcast(self: TDF) -> TDF:
        self._task.broadcast()
        return self

    def partition(self: TDF, *args, **kwargs) -> TDF:
        return self.to_self_type(
            WorkflowDataFrame(
                self.workflow,
                self._task,
                {"pre_partition": PartitionSpec(*args, **kwargs)},
            )
        )

    def to_self_type(self: TDF, df: "WorkflowDataFrame") -> TDF:
        return df  # type: ignore


class FugueWorkflow(object):
    def __init__(self, execution_engine: ExecutionEngine):
        self._spec = WorkflowSpec()
        self._execution_engine = execution_engine

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._execution_engine

    def create_data(
        self, data: Any, schema: Any = None, metadata: Any = None, partition: Any = None
    ) -> WorkflowDataFrame:
        task = Create(
            self.execution_engine,
            creator=CreateData,
            params=dict(data=data, schema=schema, metadata=metadata),
        )
        return self.add(task)

    def df(
        self, data: Any, schema: Any = None, metadata: Any = None, partition: Any = None
    ) -> WorkflowDataFrame:
        return self.create_data(data, schema, metadata, partition)

    def show(
        self, *dfs: Any, rows: int = 10, count: bool = False, title: str = ""
    ) -> None:
        task = Output(
            len(dfs),
            self.execution_engine,
            outputter=Show,
            pre_partition=None,
            params=dict(rows=rows, count=count, title=title),
        )
        self.add(task, *dfs)

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        task = Output(
            len(dfs), self.execution_engine, outputter=AssertEqual, params=params
        )
        self.add(task, *dfs)

    def add(self, task: FugueTask, *args: Any, **kwargs: Any) -> WorkflowDataFrame:
        task = task.copy()
        dep = _Dependencies(self, task, {}, *args, **kwargs)
        name = "_" + str(len(self._spec.tasks))
        self._spec.add_task(name, task, dep.dependency)
        return WorkflowDataFrame(self, task)


class _Dependencies(object):
    def __init__(
        self,
        workflow: "FugueWorkflow",
        task: FugueTask,
        local_vars: Dict[str, Any],
        *args: Any,
        **kwargs: Any,
    ):
        self.workflow = workflow
        self._local_vars = local_vars
        self.dependency: Dict[str, str] = {}
        for i in range(len(args)):
            key = task.inputs.get_key_by_index(i)
            self.dependency[key] = self._parse_single_dependency(args[i])
        for k, v in kwargs.items():
            self.dependency[k] = self._parse_single_dependency(v)

    def _parse_single_dependency(self, dep: Any) -> str:
        if isinstance(dep, tuple):  # (cursor_like_obj, output_name)
            cursor = self._parse_cursor(dep[0])
            return cursor._task.name + "." + dep[1]
        return self._parse_cursor(dep)._task.single_output_expression

    def _parse_cursor(self, dep: Any) -> WorkflowDataFrame:
        if isinstance(dep, WorkflowDataFrame):
            return dep
        if isinstance(dep, str):
            assert_or_throw(
                dep in self._local_vars, KeyError(f"{dep} is not a local variable")
            )
            if isinstance(self._local_vars[dep], WorkflowDataFrame):
                return self._local_vars[dep]
            # TODO: should also accept dataframe?
            raise TypeError(f"{self._local_vars[dep]} is not a valid dependency type")
        raise TypeError(f"{dep} is not a valid dependency type")
