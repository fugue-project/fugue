import logging
from abc import ABC, abstractmethod
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List

from fs.base import FS as FileSystem
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame
from triad.collections.dict import ParamDict
from triad.utils.convert import to_instance

_DEFAULT_JOIN_KEYS: List[str] = []


class DatabaseEngine(ABC):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._execution_engine: "ExecutionEngine" = to_instance(
            kwargs["execution_engine"], ExecutionEngine
        )
        self._conf: ParamDict = to_instance(kwargs["conf"], ParamDict)

    @property
    def execution_engine(self) -> "ExecutionEngine":
        return self._execution_engine

    @property
    def conf(self) -> ParamDict:
        return self._conf

    @abstractmethod
    def select(
        self, dfs: Dict[str, DataFrame], statement: str
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError


class ExecutionEngine(ABC):
    OPLOCK = RLock()

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
