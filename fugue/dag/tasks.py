import copy
from abc import ABC, abstractmethod
from typing import Any, no_type_check, Optional, List

from adagio.instances import TaskContext
from adagio.specs import InputSpec, OutputSpec, TaskSpec
from fugue.collections.partition import PartitionSpec
from fugue.creator.convert import to_creator
from fugue.dataframe import DataFrame, DataFrames
from fugue.exceptions import FugueWorkflowError
from fugue.execution import ExecutionEngine
from fugue.outputter.convert import to_outputter
from fugue.processor.convert import to_processor
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_or_throw


class FugueTask(TaskSpec, ABC):
    def __init__(
        self,
        execution_engine: ExecutionEngine,
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
        self._execution_engine = execution_engine
        super().__init__(
            configs=configs,
            inputs=inputs,
            outputs=outputs,
            func=self.execute,
            metadata=params,
            deterministic=deterministic,
            lazy=lazy,
        )
        self._persist: Any = self.params.get_or_none("persist", object)
        self._broadcast = self.params.get("broadcast", bool)
        self._partition = self.params.get_or_none("partition", PartitionSpec)

    @abstractmethod
    def execute(self, ctx: TaskContext) -> None:  # pragma: no cover
        raise NotImplementedError

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._execution_engine

    @property
    def params(self) -> ParamDict:
        return self.metadata

    @property
    def single_output_expression(self) -> str:
        assert_or_throw(
            len(self.outputs) == 1,
            FugueWorkflowError(f"{self.name} does not have single output"),
        )
        return self.name + "." + self.outputs.get_key_by_index(0)

    def copy(self) -> "FugueTask":
        t = copy.copy(self)
        t._node_spec = None
        return t

    def persist(self, level: Any) -> "FugueTask":
        self._persist = "" if level is None else level
        return self

    def handle_persist(self, df: DataFrame) -> DataFrame:
        if self._persist is None:
            return df
        return self.execution_engine.persist(
            df, None if self._persist == "" else self._persist
        )

    def broadcast(self) -> "FugueTask":
        self._broadcast = True
        return self

    def handle_broadcast(self, df: DataFrame) -> DataFrame:
        if not self._broadcast:
            return df
        return self.execution_engine.broadcast(df)

    def pre_partition(self, *args: Any, **kwargs: Any) -> "FugueTask":
        self._pre_partition = PartitionSpec(*args, **kwargs)
        return self


class Create(FugueTask):
    @no_type_check
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        creator: Any,
        schema: Any = None,
        params: Any = None,
        deterministic: bool = True,
        lazy: bool = True,
    ):
        self._creator = to_creator(creator, schema)
        self._creator._params = ParamDict(params)
        self._creator._execution_engine = execution_engine
        super().__init__(
            execution_engine,
            params=params,
            input_n=0,
            output_n=1,
            deterministic=deterministic,
            lazy=lazy,
        )

    @no_type_check
    def execute(self, ctx: TaskContext) -> None:
        df = self._creator.create()
        df = self.handle_persist(df)
        df = self.handle_broadcast(df)
        ctx.outputs["_0"] = df


class Process(FugueTask):
    @no_type_check
    def __init__(
        self,
        input_n: int,
        execution_engine: ExecutionEngine,
        processor: Any,
        schema: Any,
        params: Any,
        pre_partition: Any = None,
        deterministic: bool = True,
        lazy: bool = False,
        input_names: Optional[List[str]] = None,
    ):
        self._processor = to_processor(processor, schema)
        self._processor._params = ParamDict(params)
        self._processor._pre_partition = PartitionSpec(pre_partition)
        self._processor._execution_engine = execution_engine
        super().__init__(
            execution_engine,
            params=params,
            input_n=input_n,
            output_n=1,
            deterministic=deterministic,
            lazy=lazy,
            input_names=input_names,
        )

    @no_type_check
    def execute(self, ctx: TaskContext) -> None:
        df = self._processor.process(DataFrames(ctx.inputs))
        df = self.handle_persist(df)
        df = self.handle_broadcast(df)
        ctx.outputs["_0"] = df


class Output(FugueTask):
    @no_type_check
    def __init__(
        self,
        input_n: int,
        execution_engine: ExecutionEngine,
        outputter: Any,
        params: Any,
        pre_partition: Any = None,
        deterministic: bool = True,
        lazy: bool = False,
        input_names: Optional[List[str]] = None,
    ):
        assert_or_throw(input_n > 0, FugueWorkflowError("must have at least one input"))
        self._outputter = to_outputter(outputter)
        self._outputter._params = ParamDict(params)
        self._outputter._pre_partition = PartitionSpec(pre_partition)
        self._outputter._execution_engine = execution_engine
        super().__init__(
            execution_engine,
            params=params,
            input_n=input_n,
            deterministic=deterministic,
            lazy=lazy,
            input_names=input_names,
        )

    @no_type_check
    def execute(self, ctx: TaskContext) -> None:
        self._outputter.process(DataFrames(ctx.inputs))
