import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from fugue.collections.partition import (
    EMPTY_PARTITION_SPEC,
    PartitionCursor,
    PartitionSpec,
)
from fugue.dataframe import DataFrame, DataFrames
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.dataframe import LocalDataFrame
from fugue.dataframe.utils import deserialize_df, serialize_df
from fugue.exceptions import FugueBug
from triad.collections import ParamDict, Schema
from triad.collections.fs import FileSystem
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_size
from triad.utils.string import validate_triad_var_name

_DEFAULT_JOIN_KEYS: List[str] = []


class SQLEngine(ABC):
    def __init__(self, execution_engine: "ExecutionEngine") -> None:
        self._execution_engine = execution_engine

    @property
    def execution_engine(self) -> "ExecutionEngine":
        return self._execution_engine

    # @property
    # def conf(self) -> ParamDict:
    #    return self.execution_engine.conf

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
    def map(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
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
        to_file_threshold: Any = -1,
        has_name: bool = False,
    ) -> DataFrame:
        to_file_threshold = _get_file_threshold(to_file_threshold)
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
        s = _PartitionSerializer(output_schema, temp_path, to_file_threshold)
        metadata = dict(
            serialized=True,
            serialized_cols={df_name: col_name},
            schemas={df_name: str(df.schema)},
            serialized_has_name=has_name,
        )
        return self.map(df, s.run, output_schema, partition_spec, metadata)

    def zip(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str = "inner",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
        df1_name: Optional[str] = None,
        df2_name: Optional[str] = None,
    ):
        on = list(partition_spec.partition_by)
        how = how.lower()
        assert_or_throw(
            "semi" not in how and "anti" not in how,
            InvalidOperationError("zip does not support semi or anti joins"),
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

        def update_df(df: DataFrame, name: Optional[str]) -> DataFrame:
            if name is None:
                name = f"_{len(serialized_cols)}"
            if not df.metadata.get("serialized", False):
                df = self.serialize_by_partition(
                    df,
                    partition_spec,
                    name,
                    temp_path,
                    to_file_threshold,
                    has_name=name is not None,
                )
            for k in df.metadata["serialized_cols"].keys():
                assert_or_throw(
                    k not in serialized_cols, ValueError(f"{k} is duplicated")
                )
                serialized_cols[k] = df.metadata["serialized_cols"][k]
                schemas[k] = df.metadata["schemas"][k]
            return df

        df1 = update_df(df1, df1_name)
        df2 = update_df(df2, df2_name)
        metadata = dict(
            serialized=True,
            serialized_cols=serialized_cols,
            schemas=schemas,
            serialized_has_name=df1_name is not None or df2_name is not None,
        )
        return self.join(df1, df2, how=how, on=on, metadata=metadata)

    def zip_all(
        self,
        dfs: DataFrames,
        how: str = "inner",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        temp_path: Optional[str] = None,
        to_file_threshold: Any = -1,
    ) -> DataFrame:
        assert_or_throw(len(dfs) > 0, "can't zip 0 dataframes")
        pairs = list(dfs.items())
        has_name = dfs.has_key
        if len(dfs) == 1:
            return self.serialize_by_partition(
                pairs[0][1],
                partition_spec,
                pairs[0][0],
                temp_path,
                to_file_threshold,
                has_name=has_name,
            )
        df = self.zip(
            pairs[0][1],
            pairs[1][1],
            how=how,
            partition_spec=partition_spec,
            temp_path=temp_path,
            to_file_threshold=to_file_threshold,
            df1_name=pairs[0][0] if has_name else None,
            df2_name=pairs[1][0] if has_name else None,
        )
        for i in range(2, len(dfs)):
            df = self.zip(
                df,
                pairs[i][1],
                how=how,
                partition_spec=partition_spec,
                temp_path=temp_path,
                to_file_threshold=to_file_threshold,
                df2_name=pairs[i][0] if has_name else None,
            )
        return df

    def comap(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, DataFrames], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        metadata: Any = None,
        on_init: Optional[Callable[[int, DataFrames], Any]] = None,
    ):
        assert_or_throw(df.metadata["serialized"], ValueError("df is not serilaized"))
        cs = _Comap(df, map_func, on_init)
        if partition_spec.empty:
            key_schema = df.schema - list(df.metadata["serialized_cols"].values())
            partition_spec = PartitionSpec(by=list(key_schema.keys()))
        return self.map(
            df, cs.run, output_schema, partition_spec, metadata, on_init=cs.on_init
        )

    @abstractmethod
    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def save_df(
        self,
        df: DataFrame,
        path: str,
        format_hint: Any = None,
        mode: str = "overwrite",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    def __copy__(self) -> "ExecutionEngine":
        return self

    def __deepcopy__(self, memo: Any) -> "ExecutionEngine":
        return self


def _get_file_threshold(size: Any) -> int:
    if size is None:
        return -1
    if isinstance(size, int):
        return size
    return to_size(size)


def _df_name_to_serialize_col(name: str):
    assert_or_throw(name is not None, "Dataframe name can't be None")
    name = "__blob__" + name + "__"
    assert_or_throw(validate_triad_var_name(name), "Invalid name " + name)
    return name


class _PartitionSerializer(object):
    def __init__(
        self, output_schema: Schema, temp_path: Optional[str], to_file_threshold: int
    ):
        self.output_schema = output_schema
        self.temp_path = temp_path
        self.to_file_threshold = to_file_threshold

    def run(self, cursor: PartitionCursor, df: LocalDataFrame) -> LocalDataFrame:
        data = serialize_df(df, self.to_file_threshold, self.temp_path)
        row = cursor.key_value_array + [data]
        return ArrayDataFrame([row], self.output_schema)


class _Comap(object):
    def __init__(
        self,
        df: DataFrame,
        func: Callable,
        on_init: Optional[Callable[[int, DataFrames], Any]],
    ):
        self.schemas = df.metadata["schemas"]
        self.df_idx = [
            (df.schema.index_of_key(v), k, self.schemas[k])
            for k, v in df.metadata["serialized_cols"].items()
        ]
        self.named = df.metadata.get("serialized_has_name", False)
        self.func = func
        self._on_init = on_init

    def on_init(self, partition_no, df: DataFrame) -> None:
        if self._on_init is None:
            return
        # TODO: currently, get_output_schema only gets empty dataframes
        empty_dfs = _generate_comap_empty_dfs(self.schemas, self.named)
        self._on_init(partition_no, empty_dfs)

    def run(self, cursor: PartitionCursor, df: LocalDataFrame) -> LocalDataFrame:
        data = df.as_array(type_safe=True)
        assert_or_throw(
            len(data) == 1,
            FugueBug("each comap partition can have one and only one row"),
        )
        dfs = DataFrames(list(self._get_dfs(data[0])))
        return self.func(cursor, dfs)

    def _get_dfs(self, row: Any) -> Iterable[Any]:
        for k, name, v in self.df_idx:
            if row[k] is None:
                df: DataFrame = ArrayDataFrame([], v)
            else:
                df = deserialize_df(row[k])  # type: ignore
                assert df is not None
            if self.named:
                yield name, df
            else:
                yield df


def _generate_comap_empty_dfs(schemas: Any, named: bool) -> DataFrames:
    if named:
        return DataFrames({k: ArrayDataFrame([], v) for k, v in schemas.items()})
    else:
        return DataFrames([ArrayDataFrame([], v) for v in schemas.values()])
