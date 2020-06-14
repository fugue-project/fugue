from threading import RLock
from typing import Any, Dict, Iterable, List, Optional, TypeVar

from adagio.specs import WorkflowSpec
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame
from fugue.dataframe.dataframes import DataFrames
from fugue.exceptions import FugueWorkflowError
from fugue.extensions.builtins import (
    AssertEqual,
    CreateData,
    DropColumns,
    Load,
    Rename,
    RunJoin,
    RunSQLSelect,
    RunTransformer,
    Save,
    SelectColumns,
    Show,
    Zip,
)
from fugue.extensions.transformer.convert import to_transformer
from fugue.workflow.tasks import Create, FugueTask, Output, Process
from fugue.workflow.workflow_context import (
    FugueInteractiveWorkflowContext,
    FugueWorkflowContext,
)
from triad.collections import Schema
from triad.collections.dict import ParamDict
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

    def spec_uuid(self) -> str:
        return self._task.__uuid__()

    @property
    def name(self) -> str:
        return self._task.name

    @property
    def workflow(self) -> "FugueWorkflow":
        return self._workflow

    @property
    def result(self) -> DataFrame:
        return self.workflow.get_result(self)

    def compute(self, *args, **kwargs) -> DataFrame:
        # TODO: it computes entire graph
        self.workflow.run(*args, **kwargs)
        return self.result

    def process(
        self: TDF,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
    ) -> TDF:
        if pre_partition is None:
            pre_partition = self._metadata.get("pre_partition", PartitionSpec())
        df = self.workflow.process(
            self, using=using, schema=schema, params=params, pre_partition=pre_partition
        )
        return self.to_self_type(df)

    def output(self, using: Any, params: Any = None, pre_partition: Any = None) -> None:
        if pre_partition is None:
            pre_partition = self._metadata.get("pre_partition", PartitionSpec())
        self.workflow.output(
            self, using=using, params=params, pre_partition=pre_partition
        )

    def show(
        self,
        rows: int = 10,
        show_count: bool = False,
        title: Optional[str] = None,
        best_width: int = 100,
    ) -> None:
        # TODO: best_width is not used
        self.workflow.show(self, rows=rows, show_count=show_count, title=title)

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        self.workflow.assert_eq(self, *dfs, **params)

    def transform(
        self: TDF,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
        ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
    ) -> TDF:
        if pre_partition is None:
            pre_partition = self._metadata.get("pre_partition", PartitionSpec())
        df = self.workflow.transform(
            self,
            using=using,
            schema=schema,
            params=params,
            pre_partition=pre_partition,
            ignore_errors=ignore_errors,
        )
        return self.to_self_type(df)

    def join(self: TDF, *dfs: Any, how: str, on: Optional[Iterable[str]] = None) -> TDF:
        df = self.workflow.join(self, *dfs, how=how, on=on)
        return self.to_self_type(df)

    def inner_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        return self.join(*dfs, how="inner", on=on)

    def semi_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        return self.join(*dfs, how="semi", on=on)

    def left_semi_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        return self.join(*dfs, how="left_semi", on=on)

    def anti_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        return self.join(*dfs, how="anti", on=on)

    def left_anti_join(self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None) -> TDF:
        return self.join(*dfs, how="left_anti", on=on)

    def left_outer_join(
        self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None
    ) -> TDF:
        return self.join(*dfs, how="left_outer", on=on)

    def right_outer_join(
        self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None
    ) -> TDF:
        return self.join(*dfs, how="right_outer", on=on)

    def full_outer_join(
        self: TDF, *dfs: Any, on: Optional[Iterable[str]] = None
    ) -> TDF:
        return self.join(*dfs, how="full_outer", on=on)

    def cross_join(self: TDF, *dfs: Any) -> TDF:
        return self.join(*dfs, how="cross")

    def checkpoint(self: TDF, namespace: Any = None) -> TDF:
        self._task.checkpoint(namespace)
        return self

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

    def drop(  # type: ignore
        self: TDF, columns: List[str], if_exists: bool = False
    ) -> TDF:
        df = self.workflow.process(
            self, using=DropColumns, params=dict(columns=columns, if_exists=if_exists)
        )
        return self.to_self_type(df)

    def rename(self: TDF, *args: Any, **kwargs: Any) -> TDF:
        m: Dict[str, str] = {}
        for a in args:
            m.update(a)
        m.update(kwargs)
        df = self.workflow.process(self, using=Rename, params=dict(columns=m))
        return self.to_self_type(df)

    def zip(
        self: TDF,
        *dfs: Any,
        how: str = "inner",
        partition: Any = None,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
    ) -> TDF:
        if partition is None:
            partition = self._metadata.get("pre_partition", PartitionSpec())
        df = self.workflow.zip(
            self,
            *dfs,
            how=how,
            partition=partition,
            temp_path=temp_path,
            to_file_threshold=to_file_threshold,
        )
        return self.to_self_type(df)

    def __getitem__(self: TDF, columns: List[Any]) -> TDF:
        df = self.workflow.process(
            self, using=SelectColumns, params=dict(columns=columns)
        )
        return self.to_self_type(df)

    def save(
        self,
        path: str,
        fmt: str = "",
        mode: str = "overwrite",
        partition: Any = None,
        single: bool = False,
        **kwargs: Any,
    ) -> None:
        if partition is None:
            partition = self._metadata.get("pre_partition", PartitionSpec())
        self.workflow.output(
            self,
            using=Save,
            pre_partition=partition,
            params=dict(path=path, fmt=fmt, mode=mode, single=single, params=kwargs),
        )

    @property
    def schema(self) -> Schema:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    @property
    def is_local(self) -> bool:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def as_local(self) -> DataFrame:  # type: ignore  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    @property
    def is_bounded(self) -> bool:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    @property
    def empty(self) -> bool:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    @property
    def num_partitions(self) -> int:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def peek_array(self) -> Any:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def count(self) -> int:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def _drop_cols(self: TDF, cols: List[str]) -> DataFrame:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")

    def _select_cols(self, keys: List[Any]) -> DataFrame:  # pragma: no cover
        raise NotImplementedError("WorkflowDataFrame does not support this method")


class FugueWorkflow(object):
    def __init__(self, *args: Any, **kwargs: Any):
        self._lock = RLock()
        self._spec = WorkflowSpec()
        self._workflow_ctx = self._to_ctx(*args, **kwargs)
        self._computed = False

    @property
    def conf(self) -> ParamDict:
        return self._workflow_ctx.conf

    def spec_uuid(self) -> str:
        return self._spec.__uuid__()

    def run(self, *args: Any, **kwargs: Any) -> None:
        with self._lock:
            self._computed = False
            if len(args) > 0 or len(kwargs) > 0:
                self._workflow_ctx = self._to_ctx(*args, **kwargs)
            self._workflow_ctx.run(self._spec, {})
            self._computed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.run()

    def get_result(self, df: WorkflowDataFrame) -> DataFrame:
        assert_or_throw(self._computed, FugueWorkflowError("not computed"))
        return self._workflow_ctx.get_result(id(df._task))

    def create(
        self, using: Any, schema: Any = None, params: Any = None
    ) -> WorkflowDataFrame:
        task = Create(creator=using, schema=schema, params=params)
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
            processor=using,
            schema=schema,
            params=params,
            pre_partition=pre_partition,
            input_names=None if not dfs.has_key else list(dfs.keys()),
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
            outputter=using,
            params=params,
            pre_partition=pre_partition,
            input_names=None if not dfs.has_key else list(dfs.keys()),
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
        schema = None if schema is None else Schema(schema)
        return self.create(
            using=CreateData, params=dict(data=data, schema=schema, metadata=metadata)
        )

    def df(
        self, data: Any, schema: Any = None, metadata: Any = None, partition: Any = None
    ) -> WorkflowDataFrame:
        return self.create_data(data, schema, metadata, partition)

    def load(
        self, path: str, fmt: str = "", columns: Any = None, **kwargs: Any
    ) -> WorkflowDataFrame:
        return self.create(
            using=Load, params=dict(path=path, fmt=fmt, columns=columns, params=kwargs)
        )

    def show(
        self,
        *dfs: Any,
        rows: int = 10,
        show_count: bool = False,
        title: Optional[str] = None,
    ) -> None:
        self.output(
            *dfs, using=Show, params=dict(rows=rows, show_count=show_count, title=title)
        )

    def join(
        self, *dfs: Any, how: str, on: Optional[Iterable[str]] = None
    ) -> WorkflowDataFrame:  # pragma: no cover
        _on: List[str] = list(on) if on is not None else []
        return self.process(*dfs, using=RunJoin, params=dict(how=how, on=_on))

    def zip(
        self,
        *dfs: Any,
        how: str = "inner",
        partition: Any = None,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
    ) -> WorkflowDataFrame:
        return self.process(
            *dfs,
            using=Zip,
            params=dict(
                how=how, temp_path=temp_path, to_file_threshold=to_file_threshold
            ),
            pre_partition=partition,
        )

    def transform(
        self,
        *dfs: Any,
        using: Any,
        schema: Any = None,
        params: Any = None,
        pre_partition: Any = None,
        ignore_errors: List[Any] = _DEFAULT_IGNORE_ERRORS,
    ) -> WorkflowDataFrame:
        assert_or_throw(
            len(dfs) == 1,
            NotImplementedError("transform supports only single dataframe"),
        )
        tf = to_transformer(using, schema)
        return self.process(
            *dfs,
            using=RunTransformer,
            schema=None,
            params=dict(transformer=tf, ignore_errors=ignore_errors, params=params),
            pre_partition=pre_partition,
        )

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
            dfs, using=RunSQLSelect, params=dict(statement=sql, sql_engine=sql_engine)
        )

    def assert_eq(self, *dfs: Any, **params: Any) -> None:
        self.output(*dfs, using=AssertEqual, params=params)

    def add(self, task: FugueTask, *args: Any, **kwargs: Any) -> WorkflowDataFrame:
        assert_or_throw(task._node_spec is None, f"can't reuse {task}")
        dep = _Dependencies(self, task, {}, *args, **kwargs)
        name = "_" + str(len(self._spec.tasks))
        wt = self._spec.add_task(name, task, dep.dependency)
        return WorkflowDataFrame(self, wt)

    def _to_dfs(self, *args: Any, **kwargs: Any) -> DataFrames:
        return DataFrames(*args, **kwargs).convert(self.create_data)

    def _to_ctx(self, *args: Any, **kwargs) -> FugueWorkflowContext:
        if len(args) == 1 and isinstance(args[0], FugueWorkflowContext):
            return args[0]
        return FugueWorkflowContext(*args, **kwargs)


class FugueInteractiveWorkflow(FugueWorkflow):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__()
        self._workflow_ctx = self._to_ctx(*args, **kwargs)

    def run(self, *args: Any, **kwargs: Any) -> None:
        assert_or_throw(
            len(args) == 0 and len(kwargs) == 0,
            FugueWorkflowError(
                "can't reset workflow context in FugueInteractiveWorkflow"
            ),
        )
        with self._lock:
            self._computed = False
            self._workflow_ctx.run(self._spec, {})
            self._computed = True

    def get_result(self, df: WorkflowDataFrame) -> DataFrame:
        return self._workflow_ctx.get_result(id(df._task))

    def __enter__(self):
        raise FugueWorkflowError(
            "with statement is invalid for FugueInteractiveWorkflow"
        )

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        raise FugueWorkflowError(  # pragma: no cover
            "with statement is invalid for FugueInteractiveWorkflow"
        )

    def add(self, task: FugueTask, *args: Any, **kwargs: Any) -> WorkflowDataFrame:
        df = super().add(task, *args, **kwargs)
        self.run()
        return df

    def _to_ctx(self, *args: Any, **kwargs) -> FugueInteractiveWorkflowContext:
        if len(args) == 1 and isinstance(args[0], FugueInteractiveWorkflowContext):
            return args[0]
        return FugueInteractiveWorkflowContext(*args, **kwargs)


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
        # if isinstance(dep, tuple):  # (cursor_like_obj, output_name)
        #     cursor = self._parse_cursor(dep[0])
        #     return cursor._task.name + "." + dep[1]
        return self._parse_cursor(dep)._task.single_output_expression

    def _parse_cursor(self, dep: Any) -> WorkflowDataFrame:
        if isinstance(dep, WorkflowDataFrame):
            return dep
        # if isinstance(dep, DataFrame):
        #     return self.workflow.create_data(dep)
        # if isinstance(dep, str):
        #     assert_or_throw(
        #         dep in self._local_vars, KeyError(f"{dep} is not a local variable")
        #     )
        #     if isinstance(self._local_vars[dep], WorkflowDataFrame):
        #         return self._local_vars[dep]
        #     # TODO: should also accept dataframe?
        #     raise TypeError(f"{self._local_vars[dep]} is not a valid dependency type")
        raise TypeError(f"{dep} is not a valid dependency type")  # pragma: no cover
