from typing import Any, Optional

from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame, ArrayDataFrame
from fugue.extensions.context import ExtensionContext
from fugue.extensions.transformer.constants import OUTPUT_TRANSFORMER_DUMMY_SCHEMA


class Transformer(ExtensionContext):
    """The interface to process logical partitions of a dataframe.
    A dataframe such as SparkDataFrame can be distributed. But this interface is about
    local process, scalability and throughput is not a concern of Transformer.

    To implement this class, you should not have ``__init__``, please directly implement
    the interface functions.

    .. note::

      Before implementing this class, do you really need to implement this
      interface? Do you know the interfaceless feature of Fugue? Commonly, if you don't
      need to implement :meth:`~.on_init`, you can choose the
      interfaceless approach which may decouple your code from Fugue.

      It's important to understand |PartitionTutorial|, and please
      also read |TransformerTutorial|


      Due to similar issue on spark
      `pickling ABC objects <https://github.com/cloudpipe/cloudpickle/issues/305>`_.
      This class is not ABC. If you encounter the similar issue, possible solution
      `at <https://github.com/cloudpipe/cloudpickle/issues/305#issuecomment-529246171>`_
    """

    def get_output_schema(self, df: DataFrame) -> Any:  # pragma: no cover
        """Generate the output schema on the driver side.

        .. note::

          * This is running on driver
          * This is the only function in this interface that is facing the entire
            DataFrame that is not necessarily local, for example a SparkDataFrame
          * Normally, you should not consume this dataframe in this step, and you s
            hould only use its schema
          * You can access all properties except for :meth:`~.cursor`

        :param df: the entire dataframe you are going to transform.
        :return: |SchemaLikeObject|, should not be None or empty
        """
        raise NotImplementedError

    def get_format_hint(self) -> Optional[str]:
        """Get the transformer's preferred data format, for example it can be
        ``pandas``, ``pyarrow`` and None. This is to help the execution engine
        use the most efficient way to execute the logic.
        """
        return None

    def on_init(self, df: DataFrame) -> None:  # pragma: no cover
        """Callback for initializing
        :ref:`physical partition that contains one or multiple logical partitions
        <tutorial:tutorials/advanced/partition:physical vs logical partitions>`.
        You may put expensive initialization logic here so you will not have to repeat
        that in :meth:`~.transform`

        .. note::

          * This call can be on a random machine (depending on the ExecutionEngine you
            use), you should get the context from the properties of this class
          * You can get physical partition no (if available from the execution egnine)
            from :meth:`~.cursor`
          * The input dataframe may be unbounded, but must be empty aware. That means
            you must not consume the df by any means, and you can not count.
            However you can safely peek the first row of the dataframe for multiple
            times.
          * The input dataframe is never empty. Empty physical partitions are skipped

        :param df: the entire dataframe of this physical partition
        """
        pass

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:  # pragma: no cover
        """The transformation logic from one local dataframe to another local dataframe.

        .. note::

          * This function operates on :ref:`logical partition level
            <tutorial:tutorials/advanced/partition:physical vs logical partitions>`
          * This call can be on a random machine (depending on the ExecutionEngine you
            use), you should get the :class:`context
            <fugue.extensions.context.ExtensionContext>` from the properties of this
            class
          * The input dataframe may be unbounded, but must be empty aware. It's safe to
            consume it for ONLY ONCE
          * The input dataframe is never empty. Empty dataframes are skipped

        :param df: one logical partition to transform on
        :return: transformed dataframe
        """
        raise NotImplementedError


class OutputTransformer(Transformer):
    def process(self, df: LocalDataFrame) -> None:  # pragma: no cover
        raise NotImplementedError

    def get_output_schema(self, df: DataFrame) -> Any:
        return OUTPUT_TRANSFORMER_DUMMY_SCHEMA

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        self.process(df)
        return ArrayDataFrame([], OUTPUT_TRANSFORMER_DUMMY_SCHEMA)


class CoTransformer(ExtensionContext):
    """The interface to process logical partitions of a :ref:`zipped dataframe
    <tutorial:tutorials/advanced/execution_engine:zip & comap>`.
    A dataframe such as SparkDataFrame can be distributed. But this interface is about
    local process, scalability and throughput is not a concern of CoTransformer.

    To implement this class, you should not have ``__init__``, please directly implement
    the interface functions.

    .. note::

      Before implementing this class, do you really need to implement this
      interface? Do you know the interfaceless feature of Fugue? Commonly, if you don't
      need to implement :meth:`~.on_init`, you can choose the
      interfaceless approach which may decouple your code from Fugue.

      It's important to understand |ZipComap|, and
      please also read |CoTransformerTutorial|


      Due to similar issue on spark
      `pickling ABC objects <https://github.com/cloudpipe/cloudpickle/issues/305>`_.
      This class is not ABC. If you encounter the similar issue, possible solution
      `at <https://github.com/cloudpipe/cloudpickle/issues/305#issuecomment-529246171>`_
    """

    def get_output_schema(self, dfs: DataFrames) -> Any:  # pragma: no cover
        """Generate the output schema on the driver side.

        .. note::

          * This is running on driver
          * Currently, ``dfs`` is a collection of empty dataframes with the same
            structure and schemas
          * Normally, you should not consume this dataframe in this step, and you s
            hould only use its schema
          * You can access all properties except for :meth:`~.cursor`

        :param dfs: the collection of dataframes you are going to transform. They
          are empty dataframes with the same structure and schemas
        :return: |SchemaLikeObject|, should not be None or empty
        """
        raise NotImplementedError

    def get_format_hint(self) -> Optional[str]:  # pragma: no cover
        """Get the transformer's preferred data format, for example it can be
        ``pandas``, ``pyarrow`` and None. This is to help the execution engine
        use the most efficient way to execute the logic.
        """
        return None

    def on_init(self, dfs: DataFrames) -> None:  # pragma: no cover
        """Callback for initializing
        :ref:`physical partition that contains one or multiple logical partitions
        <tutorial:tutorials/advanced/partition:physical vs logical partitions>`.
        You may put expensive initialization logic here so you will not have to repeat
        that in :meth:`~.transform`

        .. note::

          * This call can be on a random machine (depending on the ExecutionEngine you
            use), you should get the context from the properties of this class
          * You can get physical partition no (if available from the execution egnine)
            from :meth:`~.cursor`
          * Currently, ``dfs`` is a collection of empty dataframes with the same
            structure and schemas

        :param dfs: a collection of empty dataframes with the same structure and schemas
        """
        pass

    def transform(self, dfs: DataFrames) -> LocalDataFrame:  # pragma: no cover
        """The transformation logic from a collection of dataframes (with the same
        partition keys) to a local dataframe.

        .. note::

          * This call can be on a random machine (depending on the ExecutionEngine you
            use), you should get the :class:`context
            <fugue.extensions.context.ExtensionContext>`
            from the properties of this class

        :param dfs:  a collection of dataframes with the same partition keys
        :return: transformed dataframe
        """
        raise NotImplementedError


class OutputCoTransformer(CoTransformer):
    def process(self, dfs: DataFrames) -> None:  # pragma: no cover
        raise NotImplementedError

    def get_output_schema(self, dfs: DataFrames) -> Any:
        return OUTPUT_TRANSFORMER_DUMMY_SCHEMA

    def transform(self, dfs: DataFrames) -> LocalDataFrame:
        self.process(dfs)
        return ArrayDataFrame([], OUTPUT_TRANSFORMER_DUMMY_SCHEMA)
