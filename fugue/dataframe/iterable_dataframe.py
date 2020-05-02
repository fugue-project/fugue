from typing import Any, Iterable, List, Optional

from fugue.dataframe.dataframe import (
    DataFrame,
    LocalUnboundedDataFrame,
    _get_schema_change,
)
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.iter import EmptyAwareIterable, make_empty_aware
from triad.utils.pyarrow import apply_schema


class IterableDataFrame(LocalUnboundedDataFrame):
    def __init__(  # noqa: C901
        self, df: Any = None, schema: Any = None, metadata: Any = None
    ):
        if df is None:
            idf: Iterable[Any] = []
            orig_schema: Optional[Schema] = None
        elif isinstance(df, IterableDataFrame):
            idf = df.native
            orig_schema = df.schema
        elif isinstance(df, DataFrame):
            idf = df.as_array_iterable(type_safe=False)
            orig_schema = df.schema
        elif isinstance(df, Iterable):
            idf = df
            orig_schema = None
        else:
            raise ValueError(f"{df} is incompatible with IterableDataFrame")
        schema, pos = _get_schema_change(orig_schema, schema)
        super().__init__(schema, metadata)
        self._pos = pos
        self._native = make_empty_aware(self._preprocess(idf))

    @property
    def native(self) -> EmptyAwareIterable[Any]:
        return self._native

    @property
    def empty(self) -> bool:
        return self.native.empty()

    def peek_array(self) -> Any:
        return list(self.native.peek())

    def drop(self, cols: List[str]) -> DataFrame:
        try:
            schema = self.schema - cols
        except Exception as e:
            raise InvalidOperationError(str(e))
        if len(schema) == 0:
            raise InvalidOperationError("Can't remove all columns of a dataframe")
        return IterableDataFrame(self, schema)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return list(self.as_array_iterable(columns, type_safe=type_safe))

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        if columns is None:
            if not type_safe:
                for item in self.native:
                    yield item
            else:
                for item in apply_schema(
                    self.schema.pa_schema,
                    self.native,
                    copy=True,
                    deep=True,
                    str_as_json=True,
                ):
                    yield item
        else:
            df = IterableDataFrame(self, self.schema.extract(columns))
            for item in df.as_array_iterable(type_safe=type_safe):
                yield item

    def _preprocess(self, raw_data: Iterable[Any]) -> Iterable[Any]:
        if len(self._pos) == 0:
            for x in raw_data:
                yield x
        else:
            for x in raw_data:
                yield [x[i] for i in self._pos]
