from typing import Any, Dict, Iterable, List, Optional

from fugue.dataframe.dataframe import (
    DataFrame,
    LocalBoundedDataFrame,
    _get_schema_change,
    as_fugue_dataset,
)
from fugue.exceptions import FugueDataFrameOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.pyarrow import apply_schema


class ArrayDataFrame(LocalBoundedDataFrame):
    """DataFrame that wraps native python 2-dimensional arrays. Please read
    |DataFrameTutorial| to understand the concept

    :param df: 2-dimensional array, iterable of arrays, or
      :class:`~fugue.dataframe.dataframe.DataFrame`
    :param schema: |SchemaLikeObject|

    .. admonition:: Examples

        >>> a = ArrayDataFrame([[0,'a'],[1,'b']],"a:int,b:str")
        >>> b = ArrayDataFrame(a)
    """

    def __init__(self, df: Any = None, schema: Any = None):  # noqa: C901
        if df is None:
            super().__init__(schema)
            self._native = []
        elif isinstance(df, DataFrame):
            if schema is None:
                super().__init__(df.schema)
                self._native = df.as_array(type_safe=False)
            else:
                schema, _ = _get_schema_change(df.schema, schema)
                super().__init__(schema)
                self._native = df.as_array(schema.names, type_safe=False)
        elif isinstance(df, Iterable):
            super().__init__(schema)
            self._native = df if isinstance(df, List) else list(df)
        else:
            raise ValueError(f"{df} is incompatible with ArrayDataFrame")

    @property
    def native(self) -> List[Any]:
        """2-dimensional native python array"""
        return self._native

    @property
    def empty(self) -> bool:
        return self.count() == 0

    def peek_array(self) -> List[Any]:
        self.assert_not_empty()
        return list(self.native[0])

    def count(self) -> int:
        return len(self.native)

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        return ArrayDataFrame(self, self.schema - cols)

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        return ArrayDataFrame(self, self.schema.extract(keys))

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            schema = self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        return ArrayDataFrame(self.native, schema)

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        temp = ArrayDataFrame(self.native, new_schema).as_array(type_safe=True)
        return ArrayDataFrame(temp, new_schema)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        if not type_safe and columns is None:
            return self.native
        return list(self.as_array_iterable(columns, type_safe=type_safe))

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        if columns is None:
            pos = []
        else:
            pos = [self.schema.index_of_key(k) for k in columns]
            assert_or_throw(len(pos) > 0, "columns if set must be non empty")
        if not type_safe:
            for item in self._iter_cols(pos):
                yield item
        else:
            sub = self.schema if columns is None else self.schema.extract(columns)
            for item in apply_schema(
                sub.pa_schema,
                self._iter_cols(pos),
                copy=True,
                deep=True,
                str_as_json=True,
            ):
                yield item

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        res = ArrayDataFrame(self.native[:n], self.schema)
        return res if columns is None else res[columns]  # type: ignore

    def _iter_cols(self, pos: List[int]) -> Iterable[List[Any]]:
        if len(pos) == 0:
            for row in self.native:
                yield row
        else:
            for row in self.native:
                yield [row[p] for p in pos]


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, list), priority=0.9)
def _arr_to_fugue(df: List[Any], **kwargs: Any) -> ArrayDataFrame:
    return ArrayDataFrame(df, **kwargs)
