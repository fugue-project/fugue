import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Optional

from fs.base import FS as FileSystem
from fugue.collections.partition import (
    PartitionCursor,
    PartitionSpec,
    EMPTY_PARTITION_SPEC,
)
from fugue.dataframe import DataFrame, DataFrames
from fugue.dataframe.iterable_dataframe import IterableDataFrame
from fugue.dataframe.utils import deserialize_df, serialize_df
from triad.collections import ParamDict, Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_size
from triad.exceptions import InvalidOperationError
from fugue.dataframe.array_dataframe import ArrayDataFrame

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
        metadata: Any = None,
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
        on: List[str] = _DEFAULT_JOIN_KEYS,
        metadata: Any = None,
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    def serialize_by_partition(
        self,
        df: DataFrame,
        partition_spec: PartitionSpec,
        df_name: str,
        temp_path: Optional[str] = None,
        to_file_threshold: int = -1,
    ) -> DataFrame:
        on = list(filter(lambda k: k in df.schema, partition_spec.partition_by))
        presort = list(
            filter(lambda p: p[0] in df.schema, partition_spec.presort.items())
        )
        col_name = _df_name_to_serialize_col(df_name)
        if len(on) == 0:
            partition_spec = PartitionSpec(
                partition_spec, num=1, by=[], presort=presort
            )
            output_schema = Schema(f"{col_name}:str")
        else:
            partition_spec = PartitionSpec(partition_spec, by=on, presort=presort)
            output_schema = partition_spec.get_key_schema(df.schema) + f"{col_name}:str"
        s = _PartitionSerializer(
            df.schema, partition_spec.partition_by, temp_path, to_file_threshold
        )
        metadata = dict(
            serialized=True,
            serialized_cols={df_name: col_name},
            schemas={df_name: df.schema},
        )
        return self.map_partitions(df, s.run, output_schema, partition_spec, metadata)

    def zip_dataframes(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str = "inner",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
    ):
        on = list(partition_spec.partition_by)
        how = how.lower()
        assert_or_throw(
            "semi" not in how and "anti" not in how,
            InvalidOperationError("zip_dataframes does not support semi or anti joins"),
        )
        to_file_threshold = (
            -1 if to_file_threshold == -1 else to_size(to_file_threshold)
        )
        serialized_cols: Dict[str, Any] = {}
        schemas: Dict[str, Any] = {}
        if len(on) == 0:
            if how != "cross":
                on = df1.schema.extract(
                    df2.schema.names, ignore_key_mismatch=True
                ).names
        else:
            assert_or_throw(
                how != "cross",
                InvalidOperationError("can't specify keys for cross join"),
            )
        partition_spec = PartitionSpec(partition_spec, by=on)

        def update_df(df: DataFrame) -> DataFrame:
            if not df.metadata.get("serialized", False):
                df = self.serialize_by_partition(
                    df,
                    partition_spec,
                    f"_{len(serialized_cols)}",
                    temp_path,
                    to_file_threshold,
                )
            for k in df.metadata["serialized_cols"].keys():
                assert_or_throw(
                    k not in serialized_cols, ValueError(f"{k} is duplicated")
                )
                serialized_cols[k] = df.metadata["serialized_cols"][k]
                schemas[k] = df.metadata["schemas"][k]
            return df

        df1 = update_df(df1)
        df2 = update_df(df2)
        metadata = dict(
            serialized=True, serialized_cols=serialized_cols, schemas=schemas
        )
        return self.join(df1, df2, how=how, on=on, metadata=metadata)

    def comap_serialized(
        self,
        df: DataFrame,
        mapFunc: Callable[[PartitionCursor, DataFrames], Iterable[Any]],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
    ):
        cs = _ComapSerialized(df, mapFunc)
        return self.map_partitions(df, cs.run, output_schema, partition_spec, metadata)

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


def _df_name_to_serialize_col(name: str):
    assert_or_throw(name is not None, "Dataframe name can't be None")
    return "__blob__" + name + "__"


class _PartitionSerializer(object):
    def __init__(
        self,
        schema: Schema,
        keys: List[str],
        temp_path: Optional[str],
        to_file_threshold: int,
    ):
        self.schema = schema
        self.index = [schema.index_of_key(key) for key in keys]
        self.temp_path = temp_path
        self.to_file_threshold = to_file_threshold

    def run(self, pn: int, data: Iterable[Any]) -> Iterable[Any]:
        df = IterableDataFrame(data, self.schema)
        arr = df.peek_array()
        row = [arr[i] for i in self.index]
        row.append(serialize_df(df, self.to_file_threshold, self.temp_path))
        yield row


class _ComapSerialized(object):
    def __init__(self, df: DataFrame, func: Callable):
        assert_or_throw(df.metadata["serialized"], ValueError("df is not serilaized"))
        self.schema = df.schema
        self.key_schema = df.schema - list(df.metadata["serialized_cols"].values())
        self.partition_spec = PartitionSpec(by=list(self.key_schema.keys()))
        self.df_idx = [
            (df.schema.index_of_key(v), df.metadata["schemas"][k])
            for k, v in df.metadata["serialized_cols"].items()
        ]
        self.func = func

    def run(self, no: int, data: Iterable[Any]) -> Iterable[Any]:
        cursor = self.partition_spec.get_cursor(self.schema, no)
        n = 0
        for row in data:
            dfs = DataFrames(list(self._get_dfs(row)))
            cursor.set(row, n, 0)
            for r in self.func(cursor, dfs):
                yield r
            n += 1

    def _get_dfs(self, row: Any) -> Iterable[DataFrame]:
        for k, v in self.df_idx:
            if row[k] is None:
                yield ArrayDataFrame([], v)
            else:
                df = deserialize_df(row[k])
                assert df is not None
                yield df
