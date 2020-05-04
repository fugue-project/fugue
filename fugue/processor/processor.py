from abc import ABC, abstractmethod

from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame, DataFrames
from fugue.execution import ExecutionEngine
from fugue.utils.misc import get_attribute
from triad.collections import ParamDict, Schema


class Processor(ABC):
    """The interface to process one or multiple incoming dataframes and return one
    DataFrame. For example dropping a column of df should be a type of Processor.
    Processor is task level extension, running on driver, and execution engine aware.

    :Notice:
    Before implementing this class, do you really need to implement this
    interface? Do you know the interfaceless feature of Fugue? Implementing Processor
    is commonly unnecessary. You can choose the interfaceless approach which may
    decouple your code from Fugue.
    """

    @abstractmethod
    def process(self, dfs: DataFrames) -> DataFrame:  # pragma: no cover
        """Process the collection of dataframes on driver side

        :Notice:
        * It runs on driver side
        * The dataframes are not necessarily local, for example a SparkDataFrame
        * It is engine aware, you can put platform dependent code in it (for example
        native pyspark code) but by doing so your code may not be portable. If you
        only use the functions of the general ExecutionEngine, it's still portable.

        :param dfs: dataframe collection to operate on
        :return: result dataframe
        """
        raise NotImplementedError

    @property
    def output_schema(self) -> Schema:
        return self._output_schema  # type: ignore

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._execution_engine  # type: ignore

    @property
    def params(self) -> ParamDict:
        return get_attribute(self, "_params", ParamDict)

    @property
    def pre_partition(self) -> PartitionSpec:
        return get_attribute(self, "_pre_partition", PartitionSpec)
