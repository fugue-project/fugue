from abc import ABC, abstractmethod

from fugue.dataframe import DataFrames
from fugue.extensions.context import ExtensionContext


class Outputter(ExtensionContext, ABC):
    """The interface to process one or multiple incoming dataframes without returning
    anything. For example printing or saving dataframes should be a type of Outputter.
    Outputter is task level extension, running on driver, and execution engine aware.

    To implement this class, you should not have ``__init__``, please directly implement
    the interface functions.

    :Notice:

    Before implementing this class, do you really need to implement this
    interface? Do you know the interfaceless feature of Fugue? Implementing Outputter
    is commonly unnecessary. You can choose the interfaceless approach which may
    decouple your code from Fugue.

    Please read :ref:`Outputter Tutorial <tutorial:/tutorials/outputter.ipynb>`
    """

    @abstractmethod
    def process(self, dfs: DataFrames) -> None:  # pragma: no cover
        """Process the collection of dataframes on driver side

        :Notice:

        * It runs on driver side
        * The dataframes are not necessarily local, for example a SparkDataFrame
        * It is engine aware, you can put platform dependent code in it (for example
          native pyspark code) but by doing so your code may not be portable. If you
          only use the functions of the general ExecutionEngine, it's still portable.

        :param dfs: dataframe collection to process
        """
        raise NotImplementedError
