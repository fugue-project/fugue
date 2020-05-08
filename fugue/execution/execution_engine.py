import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, List

from fs.base import FS as FileSystem
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame, DataFrames
from triad.collections.dict import ParamDict

_DEFAULT_JOIN_KEYS: List[str] = []


class SQLEngine(ABC):
    def __init__(self, execution_engine: "ExecutionEngine") -> None:
        self._execution_engine = execution_engine

    @property
    def execution_engine(self) -> "ExecutionEngine":
        return self._execution_engine

    @property
    def conf(self) -> ParamDict:
        return self.execution_engine.conf

    @abstractmethod
    def select(self, dfs: DataFrames, statement: str) -> DataFrame:  # pragma: no cover
        raise NotImplementedError


class ExecutionEngine(ABC):
    def __init__(self, conf: Any):
        self._conf = ParamDict(conf)

    @property
    def conf(self) -> ParamDict:
        return self._conf

    @property
    @abstractmethod
    def log(self) -> logging.Logger:  # pragma: no cover
        raise NotImplementedError

    @property
    @abstractmethod
    def fs(self) -> FileSystem:  # pragma: no cover
        raise NotImplementedError

    @property
    @abstractmethod
    def default_sql_engine(self) -> SQLEngine:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def to_df(
        self, data: Any, schema: Any = None, metadata: Any = None
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def map_partitions(
        self,
        df: DataFrame,
        mapFunc: Callable[[int, Iterable[Any]], Any],
        output_schema: Any,
        partition_spec: PartitionSpec,
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def broadcast(self, df: DataFrame) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def persist(
        self, df: DataFrame, level: Any = None
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        keys: List[str] = _DEFAULT_JOIN_KEYS,
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    # @abstractmethod
    # def load_df(
    #     self, path: str, format_hint: Any = None, **kwargs: Any
    # ) -> DataFrame:  # pragma: no cover
    #     raise NotImplementedError

    # @abstractmethod
    # def save_df(
    #     self,
    #     df: DataFrame,
    #     path: str,
    #     overwrite: bool,
    #     format_hint: Any = None,
    #     **kwargs: Any,
    # ) -> None:  # pragma: no cover
    #     raise NotImplementedError

    def __copy__(self) -> "ExecutionEngine":
        return self

    def __deepcopy__(self, memo: Any) -> "ExecutionEngine":
        return self
