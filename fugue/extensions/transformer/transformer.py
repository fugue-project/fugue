from typing import Any

from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.extensions.context import ExtensionContext


class Transformer(ExtensionContext):
    """The interface to process one physical partition of dataframe on one machine.
    A dataframe such as SparkDataFrame can be distributed. But this one is to tell
    the system how to process each partition locally.

    There are 2 levels of partitioning: physical level, logical level.
    For example we have a dataframe `[[0,1],[1,1],[1,1],[1,9],[3,1]]` with schema
    `a:int,b:int`. When you want to transform this df partition by column `a`, you
    could get `[[0,1],[1,1],[1,1],[1,9]]` as the first physical partition on machine `A`
    and `[[3,1]]` as the second on `B`. Notice that each logical partition can be in
    only one physical partition while one physical partition can contain several
    logical partitions. So on machine `A`, we will get the logical partitions
    `[[0,1]]` and `[[1,1],[1,1],[1,9]]`. Then
    in this case, `transform`, which is on logical level, will be invoked 2 times on
    `A` and 1 time on `B`.

    To implement this class, you should not have __init__, please directly implement
    the interface functions.

    :Notice:
    Before implementing this class, do you really need to implement this
    interface? Do you know the interfaceless feature of Fugue? Commonly, if you don't
    need to implement `on_init`, you can choose the
    interfaceless approach which may decouple your code from Fugue.

    Due to similar issue on
    `spark pickling ABC objects <https://github.com/cloudpipe/cloudpickle/issues/305>`_.
    This class is not ABC. If you encounter the similar issue, possible solution
    `here <https://github.com/cloudpipe/cloudpickle/issues/305#issuecomment-529246171>`_
    """

    def get_output_schema(self, df: DataFrame) -> Any:  # pragma: no cover
        """Generate the output schema on the driver side.

        :Notice:
        * This is running on driver
        * This is the only function in this interface that is facing the entire
          DataFrame that is not necessarily local, for example a SparkDataFrame
        * Normally, you should not consumer this dataframe, and you should only use its
        schema and metadata
        * You can access all properties except for `cursor`

        :param df: the entire dataframe you are going to transform.
        :return: Schema like object, should not be None or empty
        """
        raise NotImplementedError

    def on_init(self, df: DataFrame) -> None:  # pragma: no cover
        """Initialize physical partition that contains one or multiple logical partitions.
        You may put expensive initialization logic that is specific for this physical
        partition here so you will not have to repeat that in `transform`

        :Notice:
        * This call can be on a random machine (depending on the ExecutionEngine you
          use), you should get the context from the properties of this class
        * You can get physical partition no (if available from the execution egnine)
          from `self.cursor`
        * The input dataframe may be unbounded, but must be empty aware. That means you
          must not consume the df by any means, and you can not count. However you can
          safely peek the first row of the dataframe for multiple times.
        * The input dataframe is never empty. Empty dataframes are skipped

        :param df: entire dataframe of this physical partition
        """
        pass

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:  # pragma: no cover
        """Custom logic to transform from one local dataframe to another local dataframe.

        :Notice:
        * This function operates on logical partition level
        * This call can be on a random machine (depending on the ExecutionEngine you
          use), you should get the context from the properties of this class
        * The input dataframe may be unbounded, but must be empty aware. It's safe to
          consume it for only once
        * The input dataframe is never empty. Empty dataframes are skipped

        :param df: one logical partition as LocalDataFrame to transform on
        :return: new LocalDataFrame
        """
        raise NotImplementedError


class CoTransformer(ExtensionContext):
    """The interface to process one physical partition of cogrouped dataframes on one
    machine. A dataframe such as SparkDataFrame can be distributed. But this one is to
    tell the system how to process each partition locally.

    For partitioning levels, read
    :func:`~fugue.extensions.transformer.transformer.Transformer`

    To implement this class, you should not have __init__, please directly implement
    the interface functions.

    :Notice:
    Before implementing this class, do you really need to implement this
    interface? Do you know the interfaceless feature of Fugue? Commonly, if you don't
    need to implement `on_init`, you can choose the
    interfaceless approach which may decouple your code from Fugue.

    Due to similar issue on
    `spark pickling ABC objects <https://github.com/cloudpipe/cloudpickle/issues/305>`_.
    This class is not ABC. If you encounter the similar issue, possible solution
    `here <https://github.com/cloudpipe/cloudpickle/issues/305#issuecomment-529246171>`_
    """

    def get_output_schema(self, dfs: DataFrames) -> Any:  # pragma: no cover
        """Generate the output schema on the driver side.

        :Notice:
        * This is running on driver
        * Currently, `dfs` is a collection of empty dataframes with the correspondent
          schemas
        * Normally, you should not consume this dataframe, and you should only use its
          schema and metadata
        * You can access all properties except for `cursor`

        :param dfs: the collection of dataframes you are going to transform.
        :return: Schema like object, should not be None or empty
        """
        raise NotImplementedError

    def on_init(self, dfs: DataFrames) -> None:  # pragma: no cover
        """Initialize physical partition that contains one or multiple logical partitions.
        You may put expensive initialization logic that is specific for this physical
        partition here so you will not have to repeat that in `transform` function

        :Notice:
        * This call can be on a random machine (depending on the ExecutionEngine you
          use), you should get the context from the properties of this class
        * You can get physical partition no (if available from the execution egnine)
          from `self.cursor`
        * Currently, `dfs` is a collection of empty dataframes with the correspondent
          schemas

        :param dfs: the first cogrouped dataframes on this physical partition
        """
        pass

    def transform(self, dfs: DataFrames) -> LocalDataFrame:  # pragma: no cover
        """Custom logic to transform from one local dataframe to another local dataframe.

        :Notice:
        * This function operates on logical partition level, it is different from
          :func:`~fugue.extensions.transformer.transformer.Transformer.transform`
        * This call can be on a random machine (depending on the ExecutionEngine you
          use), you should get the context from the properties of this class
        * The input dataframe may be unbounded, but must be empty aware. It's safe to
          consume it for only once
        * `dfs` may not include all dataframes, because in a outter joined cogroup,
          there can be NULL dataframes, and they will not appear in dfs.

        :param dfs: one cogrouped dataframes to transform on
        :return: new LocalDataFrame
        """
        raise NotImplementedError
