import json
from abc import ABC, abstractmethod
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError


class DataFrame(ABC):
    SHOW_LOCK = RLock()

    def __init__(self, schema: Any = None, metadata: Any = None):
        if not callable(schema):
            schema = _input_schema(schema).assert_not_empty()
            schema.set_readonly()
            self._schema: Union[Schema, Callable[[], Schema]] = schema
            self._schema_discovered = True
        else:
            self._schema: Union[Schema, Callable[[], Schema]] = schema  # type: ignore
            self._schema_discovered = False
        self._metadata = ParamDict(metadata, deep=True)
        self._lazy_schema_lock = RLock()

    @property
    def metadata(self) -> ParamDict:
        return self._metadata

    @property
    def schema(self) -> Schema:
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
        return isinstance(self, LocalDataFrame)

    @abstractmethod
    def as_local(self) -> "LocalDataFrame":  # pragma: no cover
        raise NotImplementedError

    @property
    @abstractmethod
    def is_bounded(self) -> bool:  # pragma: no cover
        raise NotImplementedError

    # @abstractmethod
    # def as_local(self) -> "DataFrame":  # pragma: no cover
    #    raise NotImplementedError

    # @abstractmethod
    # def apply_schema(self, schema: Any) -> None:  # pragma: no cover
    #    raise NotImplementedError

    # @abstractmethod
    # def num_partitions(self) -> int:  # pragma: no cover
    #    raise NotImplementedError

    @property
    @abstractmethod
    def empty(self) -> bool:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def peek_array(self) -> Any:  # pragma: no cover
        raise NotImplementedError

    def peek_dict(self) -> Dict[str, Any]:
        arr = self.peek_array()
        return {self.schema.names[i]: arr[i] for i in range(len(self.schema))}

    @abstractmethod
    def count(self, persist: bool = False) -> int:  # pragma: no cover
        raise NotImplementedError

    def as_pandas(self) -> pd.DataFrame:
        pdf = pd.DataFrame(self.as_array(), columns=self.schema.names)
        return pdf.astype(dtype=self.schema.pd_dtype)

    # @abstractmethod
    # def as_pyarrow(self) -> pa.Table:  # pragma: no cover
    #    raise NotImplementedError

    @abstractmethod
    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def drop(self, cols: List[str]) -> "DataFrame":  # pragma: no cover
        raise NotImplementedError

    def show(
        self,
        n: int = 10,
        show_count: bool = False,
        title: Optional[str] = None,
        best_width: int = 100,
    ) -> None:
        arr: List[List[str]] = self.head(n)
        count = -1
        if len(arr) < n:
            count = len(arr)
        elif show_count:
            count = self.count()
        with DataFrame.SHOW_LOCK:
            if title is not None:
                print(title)
            print(type(self).__name__)
            tb = _PrettyTable(self.schema, arr, best_width)
            print("\n".join(tb.to_string()))
            if count >= 0:
                print(f"Total count: {count}")
                print("")
            if len(self.metadata) > 0:
                print("Metadata:")
                print(self.metadata.to_json(indent=True))
                print("")

    def head(self, n: int, columns: Optional[List[str]] = None) -> List[Any]:
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
        if columns is None:
            columns = self.schema.names
        idx = range(len(columns))
        for x in self.as_array_iterable(columns, type_safe=True):
            yield {columns[i]: x[i] for i in idx}

    def get_info_str(self) -> str:
        return json.dumps(
            {
                "schema": str(self.schema),
                "type": "{}.{}".format(
                    self.__class__.__module__, self.__class__.__name__
                ),
                "metadata": self.metadata,
            }
        )


class LocalDataFrame(DataFrame):
    def __init__(self, schema: Any = None, metadata: Any = None):
        super().__init__(schema=schema, metadata=metadata)

    @property
    def is_local(self):
        return True

    def as_local(self) -> "LocalDataFrame":
        return self


class LocalBoundedDataFrame(LocalDataFrame):
    def __init__(self, schema: Any = None, metadata: Any = None):
        super().__init__(schema=schema, metadata=metadata)

    @property
    def is_bounded(self):
        return True


class LocalUnboundedDataFrame(LocalDataFrame):
    def __init__(self, schema: Any = None, metadata: Any = None):
        super().__init__(schema=schema, metadata=metadata)

    @property
    def is_bounded(self):
        return False

    def count(self, persist: bool = False) -> int:
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
