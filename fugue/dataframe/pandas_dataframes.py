from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
from fugue.dataframe.dataframe import (
    DataFrame,
    LocalBoundedDataFrame,
    _enforce_type,
    _input_schema,
)
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.pyarrow import apply_schema


class PandasDataFrame(LocalBoundedDataFrame):
    def __init__(  # noqa: C901
        self, df: Any = None, schema: Any = None, metadata: Any = None
    ):
        if df is None:
            schema = _input_schema(schema).assert_not_empty()
            pdf = pd.DataFrame([], columns=schema.names)
            pdf = pdf.astype(dtype=schema.pd_dtype)
        elif isinstance(df, PandasDataFrame):
            # TODO: This is useless if in this way and wrong
            pdf = df.native
            schema = None
        elif isinstance(df, (pd.DataFrame, pd.Series)):
            if isinstance(df, pd.Series):
                df = df.to_frame()
            pdf = df
            schema = None if schema is None else _input_schema(schema)
        elif isinstance(df, Iterable):
            assert_arg_not_none(schema, msg=f"schema can't be None for iterable input")
            schema = _input_schema(schema).assert_not_empty()
            pdf = pd.DataFrame(df, columns=schema.names)
            pdf = _enforce_type(pdf, schema)
            super().__init__(schema, metadata)
            self._native = pdf
            return
        else:
            raise ValueError(f"{df} is incompatible with PandasDataFrame")
        pdf, schema = self._apply_schema(pdf, schema)
        super().__init__(schema, metadata)
        self._native = pdf

    @property
    def native(self) -> pd.DataFrame:
        return self._native

    @property
    def empty(self) -> bool:
        return self.native.empty

    def peek_array(self) -> Any:
        return self.native.iloc[0].values.tolist()

    def count(self, persist: bool = False) -> int:
        return self.native.shape[0]

    def as_pandas(self) -> pd.DataFrame:
        return self._native

    def drop(self, cols: List[str]) -> DataFrame:
        try:
            schema = self.schema - cols
        except Exception as e:
            raise InvalidOperationError(str(e))
        if len(schema) == 0:
            raise InvalidOperationError("Can't remove all columns of a dataframe")
        return PandasDataFrame(self.native.drop(cols, axis=1), schema)

    def rename(self, columns: Dict[str, str]) -> "DataFrame":
        df = self.native.rename(columns=columns)
        schema = self.schema.rename(columns)
        return PandasDataFrame(df, schema)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return list(self.as_array_iterable(columns, type_safe=type_safe))

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        if self._native.shape[0] == 0:
            return
        sub = self.schema if columns is None else self.schema.extract(columns)
        df = self._native[sub.names]
        if not type_safe or all(
            not isinstance(x, (pa.StructType, pa.ListType)) for x in self.schema.types
        ):
            for arr in df.itertuples(index=False, name=None):
                yield list(arr)
        else:  # TODO: If schema has nested types, the conversion will be much slower
            for arr in apply_schema(
                self.schema.pa_schema,
                df.itertuples(index=False, name=None),
                copy=True,
                deep=True,
                str_as_json=True,
            ):
                yield arr

    def _apply_schema(
        self, pdf: pd.DataFrame, schema: Optional[Schema]
    ) -> Tuple[pd.DataFrame, Schema]:
        assert_or_throw(
            pdf.empty or type(pdf.index) == pd.RangeIndex,
            ValueError("Pandas datafame must have default index"),
        )
        if pdf.columns.dtype == "object":  # pdf has named schema
            pschema = _input_schema(pdf)
            if schema is None or pschema == schema:
                return pdf, pschema.assert_not_empty()
            pdf = pdf[schema.assert_not_empty().names]
        else:  # pdf has no named schema
            schema = _input_schema(schema).assert_not_empty()
            assert_or_throw(
                pdf.shape[1] == len(schema),
                ValueError(f"Pandas datafame column count doesn't match {schema}"),
            )
            pdf.columns = schema.names
        return _enforce_type(pdf, schema), schema
