from typing import Any, Dict, Iterable, List, Optional, TypeVar

from adagio.specs import WorkflowSpec
from fugue.builtins import AssertEqual, CreateData, RunJoin, RunTransformer, Show
from fugue.builtins.processors import RunSQLSelect
from fugue.collections.partition import PartitionSpec
from fugue.dag.tasks import Create, FugueTask, Output, Process
from fugue.dataframe import DataFrame
from fugue.dataframe.dataframes import DataFrames
from fugue.execution.execution_engine import ExecutionEngine
from triad.collections import Schema
from triad.utils.assertion import assert_or_throw

_DEFAULT_IGNORE_ERRORS: List[Any] = []

TDF = TypeVar("TDF", bound="WorkflowDataFrame")


class WorkflowDataFrame(DataFrame):
    def __init__(
        self, workflow: "FugueWorkflow", task: FugueTask, metadata: Any = None
    ):
        super().__init__("_0:int", metadata)
        self._workflow = workflow
        self._task = task

    @property
    def name(self) -> str:
        return self._task.name

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self.workflow.execution_engine

    @property
    def workflow(self) -> "FugueWorkflow":
        return self._workflow

    def show(
        self,
        rows: int = 10,
        show_count: bool = False,
        title: Optional[str] = None,
        best_width: int = 100,
    ) -> None:
        task = Output(
            1,
            self.execution_engine,
            outputter=Show,
            pre_partition=None,
            params=dict(
                rows=rows, count=show_count, title=title, best_width=best_width
            ),
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
    ) -> TDF:
        if partition is None:
            partition = self._metadata.get("pre_partition", PartitionSpec())
        df = self.workflow.process(
            self,
            using=RunTransformer,
            schema=None,
            params=dict(
                transformer=using,
                schema=schema,
                ignore_errors=ignore_errors,
                params=params,
            ),
            pre_partition=partition,
        )
        return self.to_self_type(df)

    def join(
        self: TDF, *dfs: Any, how: str, keys: Optional[Iterable[str]] = None
    ) -> TDF:  # pragma: no cover
        df = self.workflow.join(self, *dfs, how=how, keys=keys)
        return self.to_self_type(df)

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

    @property
    def schema(self) -> Schema:
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    @property
    def is_local(self) -> bool:  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    def as_local(self) -> DataFrame:  # type: ignore  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    @property
    def is_bounded(self) -> bool:  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    @property
    def empty(self) -> bool:  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    def peek_array(self) -> Any:  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    def count(self, persist: bool = False) -> int:  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")

    def drop(self, cols: List[str]) -> "DataFrame":  # pragma: no cover
        raise NotImplementedError(f"WorkflowDataFrame does not support this method")


class FugueWorkflow(object):
    def __init__(self, execution_engine: ExecutionEngine):
        self._spec = WorkflowSpec()
        self._execution_engine = execution_engine

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._execution_engine

    def create(
        self, using: Any, schema: Any = None, params: Any = None
    ) -> WorkflowDataFrame:
        task = Create(
            self.execution_engine, creator=using, schema=schema, params=params
        )
        return self.add(task)

    def process(
        self,
        *dfs: Any,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
    ) -> WorkflowDataFrame:
        dfs = self._to_dfs(*dfs)
        task = Process(
            len(dfs),
            self.execution_engine,
            processor=using,
            schema=schema,
            params=params,
            pre_partition=pre_partition,
        )
        if dfs.has_key:
            return self.add(task, **dfs)
        else:
            return self.add(task, *dfs.values())

    def output(
        self, *dfs: Any, using: Any, params: Any = None, pre_partition: Any = None
    ) -> None:
        dfs = self._to_dfs(*dfs)
        task = Output(
            len(dfs),
            self.execution_engine,
            outputter=using,
            params=params,
            pre_partition=pre_partition,
        )
        if dfs.has_key:
            self.add(task, **dfs)
        else:
            self.add(task, *dfs.values())

    def create_data(
        self, data: Any, schema: Any = None, metadata: Any = None, partition: Any = None
    ) -> WorkflowDataFrame:
        if isinstance(data, WorkflowDataFrame):
            assert_or_throw(
                data.workflow is self, f"{data} does not belong to this workflow"
            )
            return data
        return self.create(
            using=CreateData, params=dict(data=data, schema=schema, metadata=metadata)
        )

    def df(
        self, data: Any, schema: Any = None, metadata: Any = None, partition: Any = None
    ) -> WorkflowDataFrame:
        return self.create_data(data, schema, metadata, partition)

    def show(
        self,
        *dfs: Any,
        rows: int = 10,
        count: bool = False,
        title: Optional[str] = None,
    ) -> None:
        self.output(*dfs, using=Show, params=dict(rows=rows, count=count, title=title))

    def join(
        self, *dfs: Any, how: str, keys: Optional[Iterable[str]] = None
    ) -> WorkflowDataFrame:  # pragma: no cover
        _keys: List[str] = list(keys) if keys is not None else []
        return self.process(*dfs, using=RunJoin, params=dict(how=how, keys=_keys))

    def select(self, *statements: Any, sql_engine: Any = None) -> WorkflowDataFrame:
        s_str: List[str] = []
        dfs: Dict[str, DataFrame] = {}
        for s in statements:
            if isinstance(s, str):
                s_str.append(s)
            if isinstance(s, DataFrame):
                ws = self.df(s)
                dfs[ws.name] = ws
                s_str.append(ws.name)
        sql = " ".join(s_str).strip()
        if not sql.upper().startswith("SELECT"):
            sql = "SELECT " + sql
        return self.process(
            self._to_dfs(dfs),
            using=RunSQLSelect,
            params=dict(statement=sql, sql_engine=sql_engine),
        )

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        self.output(*dfs, using=AssertEqual, params=params)

    def add(self, task: FugueTask, *args: Any, **kwargs: Any) -> WorkflowDataFrame:
        task = task.copy()
        dep = _Dependencies(self, task, {}, *args, **kwargs)
        name = "_" + str(len(self._spec.tasks))
        self._spec.add_task(name, task, dep.dependency)
        return WorkflowDataFrame(self, task)

    def _to_dfs(self, *args: Any, **kwargs: Any) -> DataFrames:
        return DataFrames(*args, **kwargs).convert(self.create_data)


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
        if isinstance(dep, DataFrame):
            return self.workflow.create_data(dep)
        if isinstance(dep, str):
            assert_or_throw(
                dep in self._local_vars, KeyError(f"{dep} is not a local variable")
            )
            if isinstance(self._local_vars[dep], WorkflowDataFrame):
                return self._local_vars[dep]
            # TODO: should also accept dataframe?
            raise TypeError(f"{self._local_vars[dep]} is not a valid dependency type")
        raise TypeError(f"{dep} is not a valid dependency type")
