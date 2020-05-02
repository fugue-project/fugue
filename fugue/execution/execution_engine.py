import logging
from abc import ABC, abstractmethod
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List

from fs.base import FS as FileSystem
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame
from fugue.dataframe.iterable_dataframe import IterableDataFrame
from fugue.transformer import Transformer
from fugue.transformer.convert import to_transformer
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_instance


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

    def transform(
        self,
        df: DataFrame,
        transformer: Any,
        params: Any,
        partition_spec: PartitionSpec,
        ignore_errors: List[type],
    ) -> DataFrame:
        # for interfaceless functions, you must do to_transformer
        # before calling transform.
        tf = to_transformer(transformer, None)
        assert_or_throw(isinstance(tf, Transformer), f"{tf} is not Transformer")
        tf._params = ParamDict(params)  # type: ignore
        tf._partition_spec = partition_spec  # type: ignore
        tf._key_schema = partition_spec.get_key_schema(df.schema)  # type: ignore
        tf._output_schema = tf.get_output_schema(df)  # type: ignore
        tr = _TransformerRunner(df, tf, ignore_errors)  # type: ignore
        return self.map_partitions(
            df=df,
            mapFunc=tr.run,
            output_schema=tf.output_schema,  # type: ignore
            partition_spec=partition_spec,
        )


class _TransformerRunner(object):
    def __init__(
        self, df: DataFrame, transformer: Transformer, ignore_errors: List[type]
    ):
        self.schema = df.schema
        self.metadata = df.metadata
        self.transformer = transformer
        self.ignore_errors = ignore_errors

    def run(self, no: int, data: Iterable[Any]) -> Iterable[Any]:
        df = IterableDataFrame(data, self.schema, self.metadata)
        if df.empty:
            return
        spec = self.transformer.partition_spec
        self.transformer._cursor = spec.get_cursor(  # type: ignore
            self.schema, no
        )
        partitioner = spec.get_partitioner(self.schema)
        self.transformer.init_physical_partition(df)
        for pn, sn, sub in partitioner.partition(df.native):
            self.transformer.cursor.set(sub.peek(), pn, sn)
            sub_df = IterableDataFrame(sub, self.schema)
            sub_df._metadata = self.metadata
            self.transformer.init_logical_partition(sub_df)
            good = True
            try:
                res = self.transformer.transform(sub_df)
            except Exception as e:
                if isinstance(e, tuple(self.ignore_errors)):
                    good = False
                else:
                    raise e
            if not good:
                continue
            for r in res.as_array_iterable(type_safe=True):
                yield r
