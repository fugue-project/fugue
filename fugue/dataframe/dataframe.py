import json
from abc import abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

import pandas as pd
import pyarrow as pa
from triad import SerializableRLock
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.pandas_like import PD_UTILS

from .._utils.display import PrettyTable
from ..collections.yielded import Yielded
from ..dataset import (
    Dataset,
    DatasetDisplay,
    as_local,
    as_local_bounded,
    get_dataset_display,
    as_fugue_dataset,
)
from ..exceptions import FugueDataFrameOperationError

AnyDataFrame = TypeVar("AnyDataFrame", "DataFrame", object)


class DataFrame(Dataset):
    """Base class of Fugue DataFrame. Please read
    |DataFrameTutorial| to understand the concept

    :param schema: |SchemaLikeObject|

    .. note::

        This is an abstract class, and normally you don't construct it by yourself
        unless you are
        implementing a new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    def __init__(self, schema: Any = None):
        super().__init__()
        if not callable(schema):
            schema = _input_schema(schema).assert_not_empty()
            schema.set_readonly()
            self._schema: Union[Schema, Callable[[], Schema]] = schema
            self._schema_discovered = True
        else:
            self._schema: Union[Schema, Callable[[], Schema]] = schema  # type: ignore
            self._schema_discovered = False
        self._lazy_schema_lock = SerializableRLock()

    @property
    def schema(self) -> Schema:
        """The schema of the dataframe"""
        if self.schema_discovered:
            # we must keep it simple because it could be called on every row by a user
            assert isinstance(self._schema, Schema)
            return self._schema  # type: ignore
        with self._lazy_schema_lock:
            self._schema = _input_schema(
                self._schema()
            ).assert_not_empty()  # type: ignore
            self._schema.set_readonly()
            self._schema_discovered = True
            return self._schema

    @property
    def schema_discovered(self) -> Schema:
        """Whether the schema has been discovered or still a lambda"""
        return self._schema_discovered

    @property
    def columns(self) -> List[str]:
        """The column names of the dataframe"""
        return self.schema.names

    @abstractmethod
    def native_as_df(self) -> AnyDataFrame:  # pragma: no cover
        """The dataframe form of the native object this Dataset class wraps.
        Dataframe form means the object contains schema information. For example
        the native an ArrayDataFrame is a python array, it doesn't contain schema
        information, and its ``native_as_df`` should be either a pandas dataframe
        or an arrow dataframe.
        """
        raise NotImplementedError

    def as_local(self) -> "LocalDataFrame":  # pragma: no cover
        """Convert this dataframe to a :class:`.LocalDataFrame`"""
        return self.as_local_bounded()

    @abstractmethod
    def as_local_bounded(self) -> "LocalBoundedDataFrame":  # pragma: no cover
        """Convert this dataframe to a :class:`.LocalBoundedDataFrame`"""
        raise NotImplementedError

    @abstractmethod
    def peek_array(self) -> List[Any]:  # pragma: no cover
        """Peek the first row of the dataframe as array

        :raises FugueDatasetEmptyError: if it is empty
        """
        raise NotImplementedError

    def peek_dict(self) -> Dict[str, Any]:
        """Peek the first row of the dataframe as dict

        :raises FugueDatasetEmptyError: if it is empty
        """
        arr = self.peek_array()
        return {self.columns[i]: arr[i] for i in range(len(self.columns))}

    def as_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame"""
        pdf = pd.DataFrame(self.as_array(), columns=self.columns)
        if len(pdf) == 0:  # TODO: move to triad
            return pd.DataFrame(
                {
                    k: pd.Series(dtype=v.type.to_pandas_dtype())
                    for k, v in self.schema.items()
                }
            )
        return PD_UTILS.enforce_type(pdf, self.schema.pa_schema, null_safe=True)

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        """Convert to pyArrow DataFrame"""
        return pa.Table.from_pandas(
            self.as_pandas().reset_index(drop=True),
            preserve_index=False,
            schema=self.schema.pa_schema,
            safe=type_safe,
        )

    @abstractmethod
    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:  # pragma: no cover
        """Convert to 2-dimensional native python array

        :param columns: columns to extract, defaults to None
        :param type_safe: whether to ensure output conforms with its schema,
          defaults to False
        :return: 2-dimensional native python array

        .. note::

            If ``type_safe`` is False, then the returned values are 'raw' values.
        """
        raise NotImplementedError

    @abstractmethod
    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:  # pragma: no cover
        """Convert to iterable of native python arrays

        :param columns: columns to extract, defaults to None
        :param type_safe: whether to ensure output conforms with its schema,
          defaults to False
        :return: iterable of native python arrays

        .. note::

            If ``type_safe`` is False, then the returned values are 'raw' values.
        """

        raise NotImplementedError

    @abstractmethod
    def _drop_cols(self, cols: List[str]) -> "DataFrame":  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def rename(self, columns: Dict[str, str]) -> "DataFrame":  # pragma: no cover
        """Rename the dataframe using a mapping dict

        :param columns: key: the original column name, value: the new name
        :return: a new dataframe with the new names
        """
        raise NotImplementedError

    @abstractmethod
    def alter_columns(self, columns: Any) -> "DataFrame":  # pragma: no cover
        """Change column types

        :param columns: |SchemaLikeObject|,
          all columns should be contained by the dataframe schema
        :return: a new dataframe with altered columns, the order of the
          original schema will not change
        """
        raise NotImplementedError

    @abstractmethod
    def _select_cols(self, cols: List[Any]) -> "DataFrame":  # pragma: no cover
        raise NotImplementedError

    def drop(self, columns: List[str]) -> "DataFrame":
        """Drop certain columns and return a new dataframe

        :param columns: columns to drop
        :raises FugueDataFrameOperationError: if
          ``columns`` are not strictly contained by this dataframe, or it is the
          entire dataframe columns
        :return: a new dataframe removing the columns
        """
        try:
            schema = self.schema - columns
        except Exception as e:
            raise FugueDataFrameOperationError from e
        if len(schema) == 0:
            raise FugueDataFrameOperationError(
                "can't remove all columns of a dataframe"
            )
        return self._drop_cols(columns)

    def __getitem__(self, columns: List[Any]) -> "DataFrame":
        """Get certain columns of the dataframe as a new dataframe

        :raises FugueDataFrameOperationError: if ``columns`` are not strictly contained
          by this dataframe or it is empty
        :return: a new dataframe with these columns
        """
        try:
            schema = self.schema.extract(columns)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        if len(schema) == 0:
            raise FugueDataFrameOperationError("must select at least one column")
        return self._select_cols(columns)

    @abstractmethod
    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> "LocalBoundedDataFrame":  # pragma: no cover
        """Get first n rows of the dataframe as a new local bounded dataframe

        :param n: number of rows
        :param columns: selected columns, defaults to None (all columns)
        :return: a local bounded dataframe
        """
        raise NotImplementedError

    def as_dict_iterable(
        self, columns: Optional[List[str]] = None
    ) -> Iterable[Dict[str, Any]]:
        """Convert to iterable of native python dicts

        :param columns: columns to extract, defaults to None
        :return: iterable of native python dicts

        .. note::

            The default implementation enforces ``type_safe`` True
        """
        if columns is None:
            columns = self.columns
        idx = range(len(columns))
        for x in self.as_array_iterable(columns, type_safe=True):
            yield {columns[i]: x[i] for i in idx}

    def get_info_str(self) -> str:
        """Get dataframe information (schema, type, metadata) as json string

        :return: json string
        """
        return json.dumps(
            {
                "schema": str(self.schema),
                "type": "{}.{}".format(
                    self.__class__.__module__, self.__class__.__name__
                ),
                "metadata": self.metadata if self.has_metadata else {},
            }
        )

    def __copy__(self) -> "DataFrame":
        return self

    def __deepcopy__(self, memo: Any) -> "DataFrame":
        return self

    def _get_altered_schema(self, subschema: Any) -> Schema:
        sub = Schema(subschema)
        assert_or_throw(
            sub.names in self.schema,
            lambda: FugueDataFrameOperationError(
                f"{sub.names} are not all in {self.schema}"
            ),
        )
        for k, v in sub.items():
            old_type = self.schema[k].type
            new_type = v.type
            if not old_type.equals(new_type):
                assert_or_throw(
                    not pa.types.is_struct(old_type)
                    and not pa.types.is_list(old_type)
                    and not pa.types.is_binary(old_type),
                    lambda: NotImplementedError(f"can't convert from {old_type}"),
                )
                assert_or_throw(
                    not pa.types.is_struct(new_type)
                    and not pa.types.is_list(new_type)
                    and not pa.types.is_binary(new_type),
                    lambda: NotImplementedError(f"can't convert to {new_type}"),
                )
        return Schema([(k, sub.get(k, v)) for k, v in self.schema.items()])


class LocalDataFrame(DataFrame):
    """Base class of all local dataframes. Please read
    :ref:`this <tutorial:tutorials/advanced/schema_dataframes:dataframe>`
    to understand the concept

    :param schema: a `schema-like <triad.collections.schema.Schema>`_ object

    .. note::

        This is an abstract class, and normally you don't construct it by yourself
        unless you are
        implementing a new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    def native_as_df(self) -> AnyDataFrame:
        return self.as_pandas()

    @property
    def is_local(self) -> bool:
        """Always True because it's a LocalDataFrame"""
        return True

    @property
    def num_partitions(self) -> int:  # pragma: no cover
        """Always 1 because it's a LocalDataFrame"""
        return 1


class LocalBoundedDataFrame(LocalDataFrame):
    """Base class of all local bounded dataframes. Please read
    :ref:`this <tutorial:tutorials/advanced/schema_dataframes:dataframe>`
    to understand the concept

    :param schema: |SchemaLikeObject|

    .. note::

        This is an abstract class, and normally you don't construct it by yourself
        unless you are
        implementing a new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    @property
    def is_bounded(self) -> bool:
        """Always True because it's a bounded dataframe"""
        return True

    def as_local_bounded(self) -> "LocalBoundedDataFrame":
        """Always True because it's a bounded dataframe"""
        return self


class LocalUnboundedDataFrame(LocalDataFrame):
    """Base class of all local unbounded dataframes. Read
    this <https://fugue-tutorials.readthedocs.io/
    en/latest/tutorials/advanced/schema_dataframes.html#DataFrame>`_
    to understand the concept

    :param schema: |SchemaLikeObject|

    .. note::

        This is an abstract class, and normally you don't construct it by yourself
        unless you are
        implementing a new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    @property
    def is_bounded(self):
        """Always False because it's an unbounded dataframe"""
        return False

    def as_local(self) -> "LocalDataFrame":
        return self

    def count(self) -> int:
        """
        :raises InvalidOperationError: You can't count an unbounded dataframe
        """
        raise InvalidOperationError("Impossible to count an LocalUnboundedDataFrame")


class YieldedDataFrame(Yielded):
    """Yielded dataframe from :class:`~fugue.workflow.workflow.FugueWorkflow`.
    Users shouldn't create this object directly.

    :param yid: unique id for determinism
    """

    def __init__(self, yid: str):
        super().__init__(yid)
        self._df: Any = None

    @property
    def is_set(self) -> bool:
        return self._df is not None

    def set_value(self, df: DataFrame) -> None:
        """Set the yielded dataframe after compute. Users should not
        call it.

        :param path: file path
        """
        self._df = df

    @property
    def result(self) -> DataFrame:
        """The yielded dataframe, it will be set after the parent
        workflow is computed
        """
        assert_or_throw(self.is_set, "value is not set")
        return self._df


class DataFrameDisplay(DatasetDisplay):
    """:class:`~.DataFrame` plain display class"""

    @property
    def df(self) -> DataFrame:
        """The target :class:`~.DataFrame`"""
        return self._ds  # type: ignore

    def show(
        self, n: int = 10, with_count: bool = False, title: Optional[str] = None
    ) -> None:
        best_width = 100
        head_rows = self.df.head(n).as_array(type_safe=True)
        if len(head_rows) < n:
            count = len(head_rows)
        else:
            count = self.df.count() if with_count else -1
        with DatasetDisplay._SHOW_LOCK:
            if title is not None and title != "":
                print(title)
            print(type(self.df).__name__)
            tb = PrettyTable(self.df.schema, head_rows, best_width)
            print("\n".join(tb.to_string()))
            if count >= 0:
                print(f"Total count: {count}")
                print("")
            if self.df.has_metadata:
                print("Metadata:")
                try:
                    # try pretty print, but if not convertible to json, print original
                    print(self.df.metadata.to_json(indent=True))
                except Exception:  # pragma: no cover
                    print(self.df.metadata)
                print("")


def as_fugue_df(df: AnyDataFrame, **kwargs: Any) -> DataFrame:
    """Wrap the object as a Fugue DataFrame.

    :param df: the object to wrap
    """
    ds = as_fugue_dataset(df, **kwargs)
    if isinstance(ds, DataFrame):
        return ds
    raise TypeError(f"{type(df)} {kwargs} is not recognized as a Fugue DataFrame: {ds}")


@get_dataset_display.candidate(lambda ds: isinstance(ds, DataFrame), priority=0.1)
def _get_dataframe_display(ds: DataFrame):
    return DataFrameDisplay(ds)


@as_local.candidate(lambda df: isinstance(df, DataFrame))
def _df_to_local(df: DataFrame) -> LocalDataFrame:
    return df.as_local()


@as_local_bounded.candidate(lambda df: isinstance(df, DataFrame))
def _df_to_local_bounded(df: DataFrame) -> LocalBoundedDataFrame:
    return df.as_local_bounded()


def _get_schema_change(
    orig_schema: Optional[Schema], schema: Any
) -> Tuple[Schema, List[int]]:
    if orig_schema is None:
        schema = _input_schema(schema).assert_not_empty()
        return schema, []
    elif schema is None:
        return orig_schema.assert_not_empty(), []
    if isinstance(schema, (str, Schema)) and orig_schema == schema:
        return orig_schema.assert_not_empty(), []
    if schema in orig_schema:
        # keys list or schema like object that is a subset of orig
        schema = orig_schema.extract(schema).assert_not_empty()
        pos = [orig_schema.index_of_key(x) for x in schema.names]
        if pos == list(range(len(orig_schema))):
            pos = []
        return schema, pos
    # otherwise it has to be a schema like object that must be a subset
    # of orig, and that has mismatched types
    schema = _input_schema(schema).assert_not_empty()
    pos = [orig_schema.index_of_key(x) for x in schema.names]
    if pos == list(range(len(orig_schema))):
        pos = []
    return schema, pos


def _input_schema(schema: Any) -> Schema:
    return schema if isinstance(schema, Schema) else Schema(schema)
