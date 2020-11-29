import json
from abc import ABC, abstractmethod
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
from fugue.exceptions import FugueDataFrameEmptyError, FugueDataFrameOperationError
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.pandas_like import PD_UTILS


class DataFrame(ABC):
    """Base class of Fugue DataFrame. Please read
    |DataFrameTutorial| to understand the concept

    :param schema: |SchemaLikeObject|
    :param metadata: dict-like object with string keys, default ``None``

    :Notice:

    This is an abstract class, and normally you don't construct it by yourself
    unless you are
    implementing a new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    _SHOW_LOCK = RLock()

    def __init__(self, schema: Any = None, metadata: Any = None):
        if not callable(schema):
            schema = _input_schema(schema).assert_not_empty()
            schema.set_readonly()
            self._schema: Union[Schema, Callable[[], Schema]] = schema
            self._schema_discovered = True
        else:
            self._schema: Union[Schema, Callable[[], Schema]] = schema  # type: ignore
            self._schema_discovered = False
        self._metadata = (
            metadata
            if isinstance(metadata, ParamDict)
            else ParamDict(metadata, deep=True)
        )
        self._metadata.set_readonly()
        self._lazy_schema_lock = RLock()

    @property
    def metadata(self) -> ParamDict:
        """Metadata of the dataframe"""
        return self._metadata

    @property
    def schema(self) -> Schema:
        """Schema of the dataframe"""
        if self._schema_discovered:
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
    def is_local(self) -> bool:  # pragma: no cover
        """Whether this dataframe is a :class:`.LocalDataFrame`"""
        return isinstance(self, LocalDataFrame)

    @abstractmethod
    def as_local(self) -> "LocalDataFrame":  # pragma: no cover
        """Convert this dataframe to a :class:`.LocalDataFrame`"""
        raise NotImplementedError

    @property
    @abstractmethod
    def is_bounded(self) -> bool:  # pragma: no cover
        """Whether this dataframe is bounded"""
        raise NotImplementedError

    @property
    @abstractmethod
    def num_partitions(self) -> int:  # pragma: no cover
        """Number of physical partitions of this dataframe.
        Please read |PartitionTutorial|
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def empty(self) -> bool:  # pragma: no cover
        """Whether this dataframe is empty"""
        raise NotImplementedError

    def assert_not_empty(self) -> None:
        """Assert this dataframe is not empty

        :raises FugueDataFrameEmptyError: if it is empty
        """

        assert_or_throw(not self.empty, FugueDataFrameEmptyError("dataframe is empty"))

    @abstractmethod
    def peek_array(self) -> Any:  # pragma: no cover
        """Peek the first row of the dataframe as array

        :raises FugueDataFrameEmptyError: if it is empty
        """
        raise NotImplementedError

    def peek_dict(self) -> Dict[str, Any]:
        """Peek the first row of the dataframe as dict

        :raises FugueDataFrameEmptyError: if it is empty
        """
        arr = self.peek_array()
        return {self.schema.names[i]: arr[i] for i in range(len(self.schema))}

    @abstractmethod
    def count(self) -> int:  # pragma: no cover
        """Get number of rows of this dataframe"""
        raise NotImplementedError

    def as_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame"""
        pdf = pd.DataFrame(self.as_array(), columns=self.schema.names)
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

        :Notice:

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

        :Notice:

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
            raise FugueDataFrameOperationError(e)
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
            raise FugueDataFrameOperationError(e)
        if len(schema) == 0:
            raise FugueDataFrameOperationError("must select at least one column")
        return self._select_cols(columns)

    def show(
        self,
        rows: int = 10,
        show_count: bool = False,
        title: Optional[str] = None,
        best_width: int = 100,
    ) -> None:
        """Print the dataframe to console

        :param rows: number of rows to print, defaults to 10
        :param show_count: whether to show dataframe count, defaults to False
        :param title: title of the dataframe, defaults to None
        :param best_width: max width of the output table, defaults to 100

        :Notice:

        When ``show_count`` is True, it can trigger expensive calculation for
        a distributed dataframe. So if you call this function directly, you may
        need to :func:`fugue.execution.execution_engine.ExecutionEngine.persist`
        the dataframe.
        """
        self._show(
            head_rows=self.head(rows),
            rows=rows,
            count=self.count() if show_count else -1,
            title=title,
            best_width=best_width,
        )

    def head(self, n: int, columns: Optional[List[str]] = None) -> List[Any]:
        """Get first n rows of the dataframe as 2-dimensional array

        :param n: number of rows
        :param columns: selected columns, defaults to None (all columns)
        :return: 2-dimensional array
        """
        res: List[Any] = []
        for row in self.as_array_iterable(columns, type_safe=True):
            if n < 1:
                break
            res.append(list(row))
            n -= 1
        return res

    def as_dict_iterable(
        self, columns: Optional[List[str]] = None
    ) -> Iterable[Dict[str, Any]]:
        """Convert to iterable of native python dicts

        :param columns: columns to extract, defaults to None
        :return: iterable of native python dicts

        :Notice:

        The default implementation enforces ``type_safe`` True
        """
        if columns is None:
            columns = self.schema.names
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
                "metadata": self.metadata,
            }
        )

    def __copy__(self) -> "DataFrame":
        return self

    def __deepcopy__(self, memo: Any) -> "DataFrame":
        return self

    def _show(
        self,
        head_rows: List[Any],
        rows: int = 10,
        count: int = -1,
        title: Optional[str] = None,
        best_width: int = 100,
    ) -> None:
        # TODO: this change is temporary, DataFramePrinter should separate
        n = rows
        if len(head_rows) < n:
            count = len(head_rows)
        with DataFrame._SHOW_LOCK:
            if title is not None and title != "":
                print(title)
            print(type(self).__name__)
            tb = _PrettyTable(self.schema, head_rows, best_width)
            print("\n".join(tb.to_string()))
            if count >= 0:
                print(f"Total count: {count}")
                print("")
            if len(self.metadata) > 0:
                print("Metadata:")
                try:
                    # try pretty print, but if not convertible to json, print original
                    print(self.metadata.to_json(indent=True))
                except Exception:  # pragma: no cover
                    print(self.metadata)
                print("")

    def _get_altered_schema(self, subschema: Any) -> Schema:
        sub = Schema(subschema)
        assert_or_throw(
            sub.names in self.schema,
            FugueDataFrameOperationError(f"{sub.names} are not all in {self.schema}"),
        )
        for k, v in sub.items():
            old_type = self.schema[k].type
            new_type = v.type
            if not old_type.equals(new_type):
                assert_or_throw(
                    not pa.types.is_struct(old_type)
                    and not pa.types.is_list(old_type)
                    and not pa.types.is_binary(old_type),
                    NotImplementedError(f"can't convert from {old_type}"),
                )
                assert_or_throw(
                    not pa.types.is_struct(new_type)
                    and not pa.types.is_list(new_type)
                    and not pa.types.is_binary(new_type),
                    NotImplementedError(f"can't convert to {new_type}"),
                )
        return Schema([(k, sub.get(k, v)) for k, v in self.schema.items()])


class LocalDataFrame(DataFrame):
    """Base class of all local dataframes. Please read
    :ref:`this <tutorial:/tutorials/schema_dataframes.ipynb#dataframe>`
    to understand the concept

    :param schema: a `schema-like <triad.collections.schema.Schema>`_ object
    :param metadata: dict-like object with string keys, default ``None``

    :Notice:

    This is an abstract class, and normally you don't construct it by yourself
    unless you are
    implementing a new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    def __init__(self, schema: Any = None, metadata: Any = None):
        super().__init__(schema=schema, metadata=metadata)

    @property
    def is_local(self) -> bool:
        """Always True because it's a LocalDataFrame"""
        return True

    def as_local(self) -> "LocalDataFrame":
        """Always return self, because it's a LocalDataFrame"""
        return self

    @property
    def num_partitions(self) -> int:  # pragma: no cover
        """Always 1 because it's a LocalDataFrame"""
        return 1


class LocalBoundedDataFrame(LocalDataFrame):
    """Base class of all local bounded dataframes. Please read
    :ref:`this <tutorial:/tutorials/schema_dataframes.ipynb#dataframe>`
    to understand the concept

    :param schema: |SchemaLikeObject|
    :param metadata: dict-like object with string keys, default ``None``

    :Notice:

    This is an abstract class, and normally you don't construct it by yourself
    unless you are
    implementing a new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    def __init__(self, schema: Any = None, metadata: Any = None):
        super().__init__(schema=schema, metadata=metadata)

    @property
    def is_bounded(self) -> bool:
        """Always True because it's a bounded dataframe"""
        return True


class LocalUnboundedDataFrame(LocalDataFrame):
    """Base class of all local unbounded dataframes. Read
    this <https://fugue-tutorials.readthedocs.io/
    en/latest/tutorials/schema_dataframes.html#DataFrame>`_ to understand the concept

    :param schema: |SchemaLikeObject|
    :param metadata: dict-like object with string keys, default ``None``

    :Notice:

    This is an abstract class, and normally you don't construct it by yourself
    unless you are
    implementing a new :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    def __init__(self, schema: Any = None, metadata: Any = None):
        super().__init__(schema=schema, metadata=metadata)

    @property
    def is_bounded(self):
        """Always True because it's an unbounded dataframe"""
        return False

    def count(self) -> int:
        """
        :raises InvalidOperationError: You can't count an unbounded dataframe
        """
        raise InvalidOperationError("Impossible to count an LocalUnboundedDataFrame")


class _PrettyTable(object):
    def __init__(
        self,  # noqa: C901
        schema: Schema,
        data: List[Any],
        best_width: int,
        truncate_width: int = 500,
    ):
        raw: List[List[str]] = []
        self.titles = str(schema).split(",")
        col_width_min = [len(t) for t in self.titles]
        col_width_max = list(col_width_min)
        self.col_width = list(col_width_min)
        # Convert all cells to string with truncation
        for row in data:
            raw_row: List[str] = []
            for i in range(len(schema)):
                d = self._cell_to_raw_str(row[i], truncate_width)
                col_width_max[i] = max(col_width_max[i], len(d))
                raw_row.append(d)
            raw.append(raw_row)
        # Adjust col width based on best_width
        # It find the remaining width after fill all cols with min widths,
        # and redistribute the remaining based on the diff between max and min widths
        dt = sorted(
            filter(  # noqa: C407
                lambda x: x[0] > 0,
                [(w - col_width_min[i], i) for i, w in enumerate(col_width_max)],
            )
        )
        if len(dt) > 0:
            remaining = max(0, best_width - sum(col_width_min) - len(col_width_min)) + 1
            total = sum(x[0] for x in dt)
            for diff, index in dt:
                if remaining <= 0:  # pragma: no cover
                    break
                given = remaining * diff // total
                remaining -= given
                self.col_width[index] += given
        # construct data -> List[List[List[str]]], make sure on the same row, each cell
        # has the same length of strings
        self.data = [
            [self._wrap(row[i], self.col_width[i]) for i in range(len(schema))]
            for row in raw
        ]
        blank = ["".ljust(self.col_width[i]) for i in range(len(schema))]
        for row in self.data:
            max_h = max(len(c) for c in row)
            for i in range(len(schema)):
                row[i] += [blank[i]] * (max_h - len(row[i]))

    def to_string(self) -> Iterable[str]:
        yield "|".join(
            self.titles[i].ljust(self.col_width[i]) for i in range(len(self.titles))
        )
        yield "+".join(
            "".ljust(self.col_width[i], "-") for i in range(len(self.titles))
        )
        for row in self.data:
            for tp in zip(*row):
                yield "|".join(tp)

    def _cell_to_raw_str(self, obj: Any, truncate_width: int) -> str:
        raw = "NULL" if obj is None else str(obj)
        if len(raw) > truncate_width:
            raw = raw[: max(0, truncate_width - 3)] + "..."
        return raw

    def _wrap(self, s: str, width: int) -> List[str]:
        res: List[str] = []
        start = 0
        while start < len(s):
            end = min(len(s), start + width)
            sub = s[start:end]
            if end < len(s):
                res.append(sub)
            else:
                res.append(sub.ljust(width, " "))
                break
            start += width
        return res


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
