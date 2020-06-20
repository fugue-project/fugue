from abc import ABC, abstractmethod

from fugue.dataframe import DataFrame
from fugue.extensions.context import ExtensionContext


class Creator(ExtensionContext, ABC):
    """The interface is to generate single DataFrame from `params`.
    For example reading data from file should be a type of Creator.
    Creator is task level extension, running on driver, and execution engine aware.

    To implement this class, you should not have ``__init__``, please directly implement
    the interface functions.

    :Notice:

    Before implementing this class, do you really need to implement this
    interface? Do you know the interfaceless feature of Fugue? Implementing Creator
    is commonly unnecessary. You can choose the interfaceless approach which may
    decouple your code from Fugue.

    Please read :ref:`Creator Tutorial <tutorial:/tutorials/creator.ipynb>`
    """

    @abstractmethod
    def create(self) -> DataFrame:  # pragma: no cover
        """Create DataFrame on driver side

        :Notice:

        * It runs on driver side
        * The output dataframe is not necessarily local, for example a SparkDataFrame
        * It is engine aware, you can put platform dependent code in it (for example
          native pyspark code) but by doing so your code may not be portable. If you
          only use the functions of the general ExecutionEngine interface, it's still
          portable.

        :return: result dataframe
        """
        raise NotImplementedError
