from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, List, Optional, no_type_check

from adagio.instances import TaskContext
from adagio.specs import InputSpec, OutputSpec, TaskSpec
from fugue.collections.partition import PartitionSpec
from fugue.collections.yielded import YieldedFile
from fugue.dataframe import DataFrame, DataFrames
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.exceptions import FugueWorkflowCompileError, FugueWorkflowError
from fugue.execution import ExecutionEngine
from fugue.extensions.creator.convert import _to_creator
from fugue.extensions.outputter.convert import _to_outputter
from fugue.extensions.processor.convert import _to_processor
from fugue.workflow._checkpoint import Checkpoint
from fugue.workflow._workflow_context import FugueWorkflowContext
from fugue.workflow.utils import is_acceptable_raw_df
from triad import ParamDict, Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid


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
            FugueWorkflowError(f"{self.name} does not have single output"),
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
    def yielded_file(self) -> YieldedFile:
        return self._checkpoint.yielded_file

    def broadcast(self) -> "FugueTask":
        self._broadcast = True
        return self

    def set_yield_dataframe_handler(self, handler: Callable) -> None:
        self._yield_dataframe_handler = handler

    def set_result(self, ctx: TaskContext, df: DataFrame) -> DataFrame:
        df = self._handle_checkpoint(df, ctx)
        df = self._handle_broadcast(df, ctx)
        if self._yield_dataframe_handler is not None:
            out_df = self._get_execution_engine(ctx).convert_yield_dataframe(df)
            self._yield_dataframe_handler(out_df)
        self._get_workflow_context(ctx).set_result(id(self), df)
        return df

    def _get_workflow_context(self, ctx: TaskContext) -> FugueWorkflowContext:
        wfctx = ctx.workflow_context
        assert isinstance(wfctx, FugueWorkflowContext)
        return wfctx

    def _get_execution_engine(self, ctx: TaskContext) -> ExecutionEngine:
        return self._get_workflow_context(ctx).execution_engine

    def _handle_checkpoint(self, df: DataFrame, ctx: TaskContext) -> DataFrame:
        wfctx = self._get_workflow_context(ctx)
        return self._checkpoint.run(df, wfctx.checkpoint_path)

    def _handle_broadcast(self, df: DataFrame, ctx: TaskContext) -> DataFrame:
        if not self._broadcast:
            return df
        return self._get_execution_engine(ctx).broadcast(df)

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


class CreateData(FugueTask):
    """CreateData task.

    :Notice:

    This task's determinism is not dependent on the input dataframe, but if
    you want the input dataframe to affect the determinism, then you should
    provide a function ``data_determiner``, which can compute a unique id from ``df``
    """

    @no_type_check
    def __init__(
        self,
        data: Any,
        schema: Any = None,
        metadata: Any = None,
        deterministic: bool = True,
        data_determiner: Optional[Callable[[Any], str]] = None,
        lazy: bool = True,
    ):
        self._validate_data(data, schema, metadata)
        self._data = data
        self._schema = None if schema is None else Schema(schema)
        self._metadata = None if metadata is None else ParamDict(metadata)
        did = "" if data_determiner is None else data_determiner(data)
        super().__init__(
            params=dict(
                schema=self._schema,
                metadata=self._metadata,
                determinism_id=did,
            ),
            input_n=0,
            output_n=1,
            deterministic=deterministic,
            lazy=lazy,
        )

    @no_type_check
    def execute(self, ctx: TaskContext) -> None:
        e = self._get_execution_engine(ctx)
        df = e.to_df(self._data, self._schema, self._metadata)
        df = self.set_result(ctx, df)
        ctx.outputs["_0"] = df

    def _validate_data(
        self, data: Any, schema: Any = None, metadata: Any = None
    ) -> None:
        if not is_acceptable_raw_df(data):
            if isinstance(data, (List, Iterable)):
                assert_or_throw(
                    schema is not None, FugueWorkflowCompileError("schema is required")
                )
            else:
                raise FugueWorkflowCompileError(
                    f"{data} can't be converted to WorkflowDataFrame"
                )


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
        df = self._creator.create()
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
        if self._input_has_key:
            inputs = DataFrames(ctx.inputs)
        else:
            inputs = DataFrames(ctx.inputs.values())
        self._processor.validate_on_runtime(inputs)
        df = self._processor.process(inputs)
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
        if self._input_has_key:
            inputs = DataFrames(ctx.inputs)
        else:
            inputs = DataFrames(ctx.inputs.values())
        self._outputter.validate_on_runtime(inputs)
        self._outputter.process(inputs)
        # TODO: output dummy to force cache to work, should we fix adagio?
        ctx.outputs["_0"] = ArrayDataFrame([], "_0:int")
