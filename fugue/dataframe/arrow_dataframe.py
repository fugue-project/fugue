from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from fugue.dataframe.dataframe import DataFrame, LocalBoundedDataFrame, _input_schema
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from fugue.exceptions import FugueDataFrameInitError, FugueDataFrameOperationError


class ArrowDataFrame(LocalBoundedDataFrame):
    """DataFrame that wraps :func:`pyarrow.Table <pa:pyarrow.table>`. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: 2-dimensional array, iterable of arrays,
      :func:`pyarrow.Table <pa:pyarrow.table>` or pandas DataFrame
    :param schema: |SchemaLikeObject|
    :param metadata: dict-like object with string keys, default ``None``

    :raises FugueDataFrameInitError: if the input is not compatible

    :Examples:

    >>> ArrowDataFrame([[0,'a'],[1,'b']],"a:int,b:str")
    >>> ArrowDataFrame(schema = "a:int,b:int")  # empty dataframe
    >>> ArrowDataFrame(pd.DataFrame([[0]],columns=["a"]))
    >>> ArrowDataFrame(ArrayDataFrame([[0]],"a:int).as_arrow())
    """

    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
        metadata: Any = None,
        pandas_df_wrapper: bool = False,
    ):
        try:
            if df is None:
                schema = _input_schema(schema).assert_not_empty()
                arr = [pa.array([])] * len(schema)
                self._native = pa.Table.from_arrays(arr, schema=schema.pa_schema)
                super().__init__(schema, metadata)
                return
            elif isinstance(df, pa.Table):
                assert_or_throw(
                    schema is None,
                    InvalidOperationError("can't reset schema for pa.Table"),
                )
                self._native = df
                super().__init__(Schema(df.schema), metadata)
                return
            elif isinstance(df, (pd.DataFrame, pd.Series)):
                if isinstance(df, pd.Series):
                    df = df.to_frame()
                pdf = df
                if schema is None:
                    self._native = pa.Table.from_pandas(
                        pdf,
                        schema=Schema(pdf).pa_schema,
                        preserve_index=False,
                        safe=True,
                    )
                    schema = Schema(self._native.schema)
                else:
                    schema = _input_schema(schema).assert_not_empty()
                    self._native = pa.Table.from_pandas(
                        pdf, schema=schema.pa_schema, preserve_index=False, safe=True
                    )
                super().__init__(schema, metadata)
                return
            elif isinstance(df, Iterable):
                schema = _input_schema(schema).assert_not_empty()
                # n = len(schema)
                # arr = []
                # for i in range(n):
                #     arr.append([])
                # for row in df:
                #     for i in range(n):
                #         arr[i].append(row[i])
                # cols = [pa.array(arr[i], type=schema.types[i]) for i in range(n)]
                # self._native = pa.Table.from_arrays(cols, schema=schema.pa_schema)
                pdf = pd.DataFrame(df, columns=schema.names)
                for f in schema.fields:
                    if pa.types.is_timestamp(f.type) or pa.types.is_date(f.type):
                        pdf[f.name] = pd.to_datetime(pdf[f.name])
                schema = _input_schema(schema).assert_not_empty()
                self._native = pa.Table.from_pandas(
                    pdf, schema=schema.pa_schema, preserve_index=False, safe=True
                )
                super().__init__(schema, metadata)
                return
            else:
                raise ValueError(f"{df} is incompatible with ArrowDataFrame")
        except Exception as e:
            raise FugueDataFrameInitError(e)

    @property
    def native(self) -> pa.Table:
        """:func:`pyarrow.Table <pa:pyarrow.table>`"""
        return self._native

    @property
    def empty(self) -> bool:
        return self.count() == 0

    def peek_array(self) -> Any:
        self.assert_not_empty()
        it = iter(self.as_array_iterable(type_safe=True))
        return next(it)

    def count(self) -> int:
        return self.native.shape[0]

    def as_pandas(self) -> pd.DataFrame:
        return self.native.to_pandas()

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        return ArrowDataFrame(self.native.drop(cols))

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        schema = self.schema.extract(keys)
        cols = [self.native.columns[self.schema.index_of_key(k)] for k in keys]
        table = pa.Table.from_arrays(cols, schema=schema.pa_schema)
        return ArrowDataFrame(table)

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            schema = self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError(e)
        df = pa.Table.from_arrays(self.native.columns, schema=schema.pa_schema)
        return ArrowDataFrame(df)

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        cols: List[pa.Array] = []
        for i in range(len(new_schema)):
            # TODO: this following logic may be generalized for entire arrow dataframe?
            col = self.native.columns[i]
            new_type = new_schema.get_value_by_index(i).type
            old_type = self.schema.get_value_by_index(i).type
            if new_type.equals(old_type):
                cols.append(col)
            elif pa.types.is_date(new_type):
                # -> date
                col = pa.Array.from_pandas(pd.to_datetime(col.to_pandas()).dt.date)
                cols.append(col)
            elif pa.types.is_timestamp(new_type):
                # -> datetime
                col = pa.Array.from_pandas(pd.to_datetime(col.to_pandas()))
                cols.append(col)
            elif pa.types.is_string(new_type):
                if pa.types.is_date(old_type):
                    # date -> str
                    series = pd.to_datetime(col.to_pandas()).dt.date
                    ns = series.isnull()
                    series = series.astype(str)
                    col = pa.Array.from_pandas(series.mask(ns, None))
                elif pa.types.is_timestamp(old_type):
                    # datetime -> str
                    series = pd.to_datetime(col.to_pandas())
                    ns = series.isnull()
                    series = series.astype(str)
                    col = pa.Array.from_pandas(series.mask(ns, None))
                elif pa.types.is_boolean(old_type):
                    # bool -> str
                    series = col.to_pandas()
                    ns = series.isnull()
                    series = (
                        series.mask(series == 0, "False")
                        .mask(series != 0, "True")
                        .mask(ns, None)
                    )
                    col = pa.Array.from_pandas(series)
                else:
                    col = col.cast(new_type, safe=True)
                cols.append(col)
            else:
                cols.append(col.cast(new_type, safe=True))
        df = pa.Table.from_arrays(cols, schema=new_schema.pa_schema)
        return ArrowDataFrame(df)

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        return self.native

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return list(self.as_array_iterable(columns, type_safe=type_safe))

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        if self.empty:
            return
        if columns is not None:
            for x in self[columns].as_array_iterable(type_safe=type_safe):
                yield x
        else:
            d = self.native.to_pydict()
            cols = [d[n] for n in self.schema.names]
            for arr in zip(*cols):
                yield list(arr)
