import logging
from threading import RLock
from typing import Any, Callable, Iterable

from fs.base import FS as FileSystem
from fs.osfs import OSFS
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    IterableDataFrame,
    LocalBoundedDataFrame,
    PandasDataFrame,
    to_local_bounded_df,
)
from fugue.dataframe.utils import to_local_df
from fugue.execution.execution_engine import ExecutionEngine
from triad.collections.dict import ParamDict


class NaiveExecutionEngine(ExecutionEngine):
    OPLOCK = RLock()

    def __init__(self, conf: Any = None):
        self._conf = ParamDict(conf)
        self._fs = OSFS("/")
        self._log = logging.getLogger()

    def __repr__(self) -> str:
        return "NaiveExecutionEngine"

    @property
    def log(self) -> logging.Logger:
        return self._log

    @property
    def fs(self) -> FileSystem:
        return self._fs

    def stop(self) -> None:  # pragma: no cover
        return

    def to_df(
        self, df: Any, schema: Any = None, metadata: Any = None
    ) -> LocalBoundedDataFrame:
        return to_local_bounded_df(df, schema, metadata)

    def repartition(self, df: DataFrame, partition_spec: PartitionSpec) -> DataFrame:
        self.log.warning(f"{self} doesn't respect repartition")
        return df

    def map_partitions(
        self,
        df: DataFrame,
        mapFunc: Callable[[int, Iterable[Any]], Iterable[Any]],
        output_schema: Any,
        partition_spec: PartitionSpec,
    ) -> DataFrame:  # pragma: no cover
        df = to_local_df(df)
        if partition_spec.num_partitions != "0":
            self.log.warning(
                f"{self} doesn't respect num_partitions {partition_spec.num_partitions}"
            )
        partitioner = partition_spec.get_partitioner(df.schema)
        sorts = partition_spec.get_sorts(df.schema)
        if len(partition_spec.partition_by) == 0:  # no partition
            return IterableDataFrame(
                mapFunc(0, df.as_array_iterable(type_safe=True)), output_schema
            )
        pdf = df.as_pandas()
        sorts = partition_spec.get_sorts(df.schema)
        if len(sorts) > 0:
            pdf = pdf.sort_values(
                list(sorts.keys()), ascending=list(sorts.values()), na_position="first"
            ).reset_index(drop=True)
        df = PandasDataFrame(pdf, df.schema)

        def get_rows() -> Iterable[Any]:
            for _, _, sub in partitioner.partition(
                df.as_array_iterable(type_safe=True)
            ):
                for r in mapFunc(0, sub):
                    yield r

        return ArrayDataFrame(get_rows(), output_schema)

    def broadcast(self, df: DataFrame) -> DataFrame:
        return self.to_df(df)

    def persist(self, df: DataFrame, level: Any = None) -> DataFrame:
        return self.to_df(df)
