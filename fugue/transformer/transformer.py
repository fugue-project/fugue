from abc import ABC, abstractmethod
from typing import Any

from fugue.collections.partition import PartitionCursor, PartitionSpec
from fugue.dataframe import DataFrame, LocalDataFrame
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema


class Transformer(ABC):
    """The interface to process one physical partition of dataframe on one machine.
    A dataframe such as SparkDataFrame can be distributed. But this one is to tell
    the system how to process each partition locally.

    There are 3 levels of partitioning: physical level, logical level and slice level.
    For example we have a dataframe `[[0,1],[1,1],[1,1],[1,9],[3,1]]` with schema
    `a:int,b:int`. When you want to transform this df partition by column `a`, you
    could get `[[0,1],[1,1],[1,1],[1,9]]` as the first physical partition on machine `A`
    and `[[3,1]]` as the second on `B`. Notice that each logical partition can be in
    only one physical partition while one physical partition can contain several
    logical partitions. So on machine `A`, we will get the logical partitions
    `[[0,1]]` and `[[1,1],[1,1],[1,9]]`. And if we set `row_limit` to 2, then the system
    will further slice the 2nd logical partition to ``[[1,1],[1,1]]` and `[[1,9]]`. Then
    in this case, transform, which is on slice level, will be invoked 3 times on
    `A` and 1 time on `B`.

    To implement this class, you should not have __init__, please directly implement
    the interface functions.

    :Notice:
    Before implementing this class, do you really need to implement this
    interface? Do you know the interfaceless feature of Fugue? Commonly, if you don't
    need to implement `init_physical_partition` and `init_logical_partition` and you
    don't need to slice logcial partition (row_limit=0), you can choose the
    interfaceless approach which may decouple your code from Fugue.
    """

    @abstractmethod
    def get_output_schema(self, df: DataFrame) -> Any:  # pragma: no cover
        """Generate the output schema on the driver side.

        :Notice:
        * This is the only in this interface running on driver
        * This is the only function in this interface that is facing the entire
        DataFrame that is not necessarily local, for example a SparkDataFrame
        * Normally, you should not consumer this dataframe, and you should only use its
        schema and metadata
        * You can access all properties except for `cursor`

        :param df: the entire dataframe you are going to transform.
        :return: Schema like object, should not be None or empty
        """
        return None

    def init_physical_partition(self, df: LocalDataFrame) -> None:  # pragma: no cover
        """Initialize physical partition that contains one or multiple logical partitions.
        You may put expensive initialization logic that is specific for this physical
        partition here so you will not have to repeat that in `init_logical_partition`
        or `transform` functions

        :Notice:
        * This call can be on a random machine (depending on the ExecutionEngine you
        use), you should get the context from the properties of this class
        * The input dataframe may be unbounded, but must be empty aware. That means you
        must not consume the df by any means, and you can not count. However you can
        safely peek the first row of the dataframe for multiple times.
        * The input dataframe is never empty. Empty dataframes are skipped

        :param df: entire dataframe of this physical partition
        """
        pass

    def init_logical_partition(self, df: LocalDataFrame) -> None:  # pragma: no cover
        """Initialize logic partition defined by partition keys. You may put expensive
        initialization logic that is specific for this partition here so you will not
        have to repeat that in `transform` function

        :Notice:
        * This call can be on a random machine (depending on the ExecutionEngine you
        use), you should get the context from the properties of this class
        * The input dataframe may be unbounded, but must be empty aware. That means you
        must not consume the df by any means, and you can not count. However you can
        safely peek the first row of the dataframe for multiple times.
        * The input dataframe is never empty. Empty dataframes are skipped

        :param df: first slice LocalDataFrame of this logical partition
        """
        pass

    @abstractmethod
    def transform(self, df: LocalDataFrame) -> LocalDataFrame:  # pragma: no cover
        """Custom logic to transform from one local dataframe to another local dataframe.

        :Notice:
        * This call can be on a random machine (depending on the ExecutionEngine you
        use), you should get the context from the properties of this class
        * The input dataframe may be unbounded, but must be empty aware. It's safe to
        consume it for only once
        * The input dataframe is never empty. Empty dataframes are skipped

        :param df: one slice of logical partition as LocalDataFrame to transform on
        :return: new LocalDataFrame
        """
        raise NotImplementedError

    @property
    def params(self) -> ParamDict:
        return self._params  # type: ignore

    @property
    def output_schema(self) -> Schema:
        return self._output_schema  # type: ignore

    @property
    def key_schema(self) -> Schema:
        return self._key_schema  # type: ignore

    @property
    def partition_spec(self) -> PartitionSpec:
        return self._partition_spec  # type: ignore

    @property
    def cursor(self) -> PartitionCursor:
        return self._cursor  # type: ignore


class MultiInputTransformer(object):
    pass
