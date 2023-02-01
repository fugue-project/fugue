import sys
from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Callable, List, Optional, no_type_check

from adagio.instances import TaskContext
from adagio.specs import InputSpec, OutputSpec, TaskSpec
from triad import ParamDict
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid

from fugue._utils.exception import (
    _MODIFIED_EXCEPTION_VAR_NAME,
    frames_to_traceback,
    modify_traceback,
)
from fugue.collections.partition import PartitionSpec
from fugue.collections.yielded import PhysicalYielded
from fugue.dataframe import DataFrame, DataFrames
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.exceptions import FugueWorkflowError
from fugue.execution import ExecutionEngine
from fugue.extensions.creator.convert import _to_creator
from fugue.extensions.outputter.convert import _to_outputter
from fugue.extensions.processor.convert import _to_processor
from fugue.rpc.base import RPCServer
from fugue.workflow._checkpoint import Checkpoint, StrongCheckpoint
from fugue.workflow._workflow_context import FugueWorkflowContext


class FugueTask(TaskSpec, ABC):
    def __init__(
        self,
        input_n: int = 0,
        output_n: int = 0,
        configs: Any = None,
        params: Any = None,
        deterministic: bool = True,
        lazy: bool = False,
        input_names: Optional[List[str]] = None,
    ):
        assert_or_throw(
            output_n <= 1,  # TODO: for now we don't support multi output
            NotImplementedError("Fugue doesn't support multiple output tasks"),
        )
        if input_names is None:
            inputs = [
                InputSpec("_" + str(i), DataFrame, nullable=False)
                for i in range(input_n)
            ]
        else:
            inputs = [
                InputSpec(input_names[i], DataFrame, nullable=False)
                for i in range(input_n)
            ]
        outputs = [
            OutputSpec("_" + str(i), DataFrame, nullable=False) for i in range(output_n)
        ]
        self._input_has_key = input_names is not None
        super().__init__(
            configs=configs,
            inputs=inputs,
            outputs=outputs,
            func=self.execute,
            metadata=params,
            deterministic=deterministic,
            lazy=lazy,
        )
        self._checkpoint = Checkpoint()
        self._broadcast = False
        self._dependency_uuid: Any = None
        self._yield_dataframe_handler: Any = None
        self._yield_local_dataframe = False
        self._traceback: Optional[TracebackType] = None

    def reset_traceback(
        self, limit: int, should_prune: Optional[Callable[[str], bool]] = None
    ) -> None:
        cf = sys._getframe(1)
        self._traceback = frames_to_traceback(
            cf, limit=limit, should_prune=should_prune
        )

    def __uuid__(self) -> str:
        # _checkpoint is not part of determinism
        # _yield_name is not part of determinism
        return to_uuid(
            self.configs,
            self.inputs,
            self.outputs,
            # get_full_type_path(self.func),
            self.metadata,
            self.deterministic,
            self.lazy,
            self._get_dependency_uuid(),
            self._broadcast,
        )

    @abstractmethod
    def execute(self, ctx: TaskContext) -> None:  # pragma: no cover
        raise NotImplementedError

    @property
    def single_output_expression(self) -> str:
        assert_or_throw(
            len(self.outputs) == 1,
            lambda: FugueWorkflowError(f"{self.name} does not have single output"),
        )
        return self.name + "." + self.outputs.get_key_by_index(0)

    def copy(self) -> "FugueTask":
        raise InvalidOperationError("can't copy")

    def __copy__(self) -> "FugueTask":
        raise InvalidOperationError("can't copy")

    def __deepcopy__(self, memo: Any) -> "FugueTask":
        raise InvalidOperationError("can't copy")

    def set_checkpoint(self, checkpoint: Checkpoint) -> "FugueTask":
        self._checkpoint = checkpoint
        return self

    @property
    def has_checkpoint(self) -> bool:
        return not self._checkpoint.is_null

    @property
    def yielded(self) -> PhysicalYielded:
        if isinstance(self._checkpoint, StrongCheckpoint):
            return self._checkpoint.yielded
        raise ValueError("can't populate value from non-physical yield")

    def broadcast(self) -> "FugueTask":
        self._broadcast = True
        return self

    def set_yield_dataframe_handler(self, handler: Callable, as_local: bool) -> None:
        self._yield_dataframe_handler = handler
        self._yield_local_dataframe = as_local

    def set_result(self, ctx: TaskContext, df: DataFrame) -> DataFrame:
        df = self._handle_checkpoint(df, ctx)
        df = self._handle_broadcast(df, ctx)
        if self._yield_dataframe_handler is not None:
            out_df = self._get_execution_engine(ctx).convert_yield_dataframe(
                df, as_local=self._yield_local_dataframe
            )
            self._yield_dataframe_handler(out_df)
        self._get_workflow_context(ctx).set_result(id(self), df)
        return df

    def _get_workflow_context(self, ctx: TaskContext) -> FugueWorkflowContext:
        wfctx = ctx.workflow_context
        assert isinstance(wfctx, FugueWorkflowContext)
        return wfctx

    def _get_execution_engine(self, ctx: TaskContext) -> ExecutionEngine:
        return self._get_workflow_context(ctx).execution_engine

    def _get_rpc_server(self, ctx: TaskContext) -> RPCServer:
        return self._get_workflow_context(ctx).rpc_server

    def _handle_checkpoint(self, df: DataFrame, ctx: TaskContext) -> DataFrame:
        wfctx = self._get_workflow_context(ctx)
        return self._execute_with_modified_traceback(
            lambda: self._checkpoint.run(df, wfctx.checkpoint_path)
        )

    def _handle_broadcast(self, df: DataFrame, ctx: TaskContext) -> DataFrame:
        if not self._broadcast:
            return df
        return self._execute_with_modified_traceback(
            lambda: self._get_execution_engine(ctx).broadcast(df)
        )

    def _get_dependency_uuid(self) -> Any:
        # TODO: this should be a part of adagio!!
        if self._dependency_uuid is not None:
            return self._dependency_uuid
        values: List[Any] = []
        for k, v in self.node_spec.dependency.items():
            t = v.split(".", 1)
            assert_or_throw(len(t) == 2)
            values.append(k)
            values.append(t[1])
            task = self.parent_workflow.tasks[t[0]]
            values.append(task.__uuid__())
        self._dependency_uuid = to_uuid(values)
        return self._dependency_uuid

    def _execute_with_modified_traceback(self, func: Callable) -> Any:
        try:
            return func()
        except Exception as ex:
            if self._traceback is None:  # pragma: no cover
                raise

            if _MODIFIED_EXCEPTION_VAR_NAME in self._traceback.tb_frame.f_locals:
                raise ex from self._traceback.tb_frame.f_locals[
                    _MODIFIED_EXCEPTION_VAR_NAME
                ]

            # add caller traceback
            ctb = modify_traceback(
                sys.exc_info()[2].tb_next, None, self._traceback  # type: ignore
            )
            if ctb is None:  # pragma: no cover
                raise
            raise ex.with_traceback(ctb)


class Create(FugueTask):
    @no_type_check
    def __init__(
        self,
        creator: Any,
        schema: Any = None,
        params: Any = None,
        deterministic: bool = True,
        lazy: bool = True,
    ):
        self._creator = _to_creator(creator, schema)
        self._creator._params = ParamDict(params, deep=False)
        super().__init__(
            params=params, input_n=0, output_n=1, deterministic=deterministic, lazy=lazy
        )

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(super().__uuid__(), self._creator, self._creator._params)

    @no_type_check
    def execute(self, ctx: TaskContext) -> None:
        e = self._get_execution_engine(ctx)
        self._creator._execution_engine = e
        df = self._execute_with_modified_traceback(lambda: self._creator.create())
        df = self.set_result(ctx, df)
        ctx.outputs["_0"] = df


class Process(FugueTask):
    @no_type_check
    def __init__(
        self,
        input_n: int,
        processor: Any,
        schema: Any,
        params: Any,
        pre_partition: Any = None,
        deterministic: bool = True,
        lazy: bool = False,
        input_names: Optional[List[str]] = None,
    ):
        self._processor = _to_processor(processor, schema)
        self._processor._params = ParamDict(params)
        self._processor._partition_spec = PartitionSpec(pre_partition)
        self._processor.validate_on_compile()
        super().__init__(
            params=params,
            input_n=input_n,
            output_n=1,
            deterministic=deterministic,
            lazy=lazy,
            input_names=input_names,
        )

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(
            super().__uuid__(),
            self._processor,
            self._processor._params,
            self._processor._partition_spec,
        )

    @no_type_check
    def execute(self, ctx: TaskContext) -> None:
        e = self._get_execution_engine(ctx)
        self._processor._execution_engine = e
        self._processor._rpc_server = self._get_rpc_server(ctx)
        if self._input_has_key:
            inputs = DataFrames(ctx.inputs)
        else:
            inputs = DataFrames(ctx.inputs.values())

        def exe() -> Any:
            self._processor.validate_on_runtime(inputs)
            return self._processor.process(inputs)

        df = self._execute_with_modified_traceback(exe)
        df = self.set_result(ctx, df)
        ctx.outputs["_0"] = df


class Output(FugueTask):
    @no_type_check
    def __init__(
        self,
        input_n: int,
        outputter: Any,
        params: Any,
        pre_partition: Any = None,
        deterministic: bool = True,
        lazy: bool = False,
        input_names: Optional[List[str]] = None,
    ):
        assert_or_throw(input_n > 0, FugueWorkflowError("must have at least one input"))
        self._outputter = _to_outputter(outputter)
        self._outputter._params = ParamDict(params)
        self._outputter._partition_spec = PartitionSpec(pre_partition)
        self._outputter.validate_on_compile()
        super().__init__(
            params=params,
            input_n=input_n,
            output_n=1,
            deterministic=deterministic,
            lazy=lazy,
            input_names=input_names,
        )

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(
            super().__uuid__(),
            self._outputter,
            self._outputter._params,
            self._outputter._partition_spec,
        )

    @no_type_check
    def execute(self, ctx: TaskContext) -> None:
        self._outputter._execution_engine = self._get_execution_engine(ctx)
        self._outputter._rpc_server = self._get_rpc_server(ctx)
        if self._input_has_key:
            inputs = DataFrames(ctx.inputs)
        else:
            inputs = DataFrames(ctx.inputs.values())

        def exe():
            self._outputter.validate_on_runtime(inputs)
            self._outputter.process(inputs)

        self._execute_with_modified_traceback(exe)
        # TODO: output dummy to force cache to work, should we fix adagio?
        ctx.outputs["_0"] = ArrayDataFrame([], "_0:int")
