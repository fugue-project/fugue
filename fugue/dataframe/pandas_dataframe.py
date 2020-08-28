from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from fugue.dataframe.dataframe import (
    DataFrame,
    LocalBoundedDataFrame,
    _enforce_type,
    _input_schema,
)
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.pandas_like import PD_UTILS
from fugue.exceptions import FugueDataFrameInitError, FugueDataFrameOperationError


class PandasDataFrame(LocalBoundedDataFrame):
    """DataFrame that wraps pandas DataFrame. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: 2-dimensional array, iterable of arrays or pandas DataFrame
    :param schema: |SchemaLikeObject|
    :param metadata: dict-like object with string keys, default ``None``
    :param pandas_df_wrapper: if this is a simple wrapper, default False

    :raises FugueDataFrameInitError: if the input is not compatible

    :Examples:

    >>> PandasDataFrame([[0,'a'],[1,'b']],"a:int,b:str")
    >>> PandasDataFrame(schema = "a:int,b:int")  # empty dataframe
    >>> PandasDataFrame(pd.DataFrame([[0]],columns=["a"]))
    >>> PandasDataFrame(ArrayDataFrame([[0]],"a:int).as_pandas())

    :Notice:

    If ``pandas_df_wrapper`` is True, then the constructor will not do any type check
    otherwise, it will enforce type according to the input schema after the construction
    """

    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
        metadata: Any = None,
        pandas_df_wrapper: bool = False,
    ):
        try:
            apply_schema = True
            if df is None:
                schema = _input_schema(schema).assert_not_empty()
                df = []
            if isinstance(df, PandasDataFrame):
                # TODO: This is useless if in this way and wrong
                pdf = df.native
                schema = None
            elif isinstance(df, (pd.DataFrame, pd.Series)):
                if isinstance(df, pd.Series):
                    df = df.to_frame()
                pdf = df
                schema = None if schema is None else _input_schema(schema)
                if pandas_df_wrapper and schema is not None:
                    apply_schema = False
            elif isinstance(df, Iterable):
                schema = _input_schema(schema).assert_not_empty()
                pdf = pd.DataFrame(df, columns=schema.names)
                pdf = _enforce_type(pdf, schema)
                apply_schema = False
            else:
                raise ValueError(f"{df} is incompatible with PandasDataFrame")
            if apply_schema:
                pdf, schema = self._apply_schema(pdf, schema)
            super().__init__(schema, metadata)
            self._native = pdf
        except Exception as e:
            raise FugueDataFrameInitError(e)

    @property
    def native(self) -> pd.DataFrame:
        """Pandas DataFrame"""
        return self._native

    @property
    def empty(self) -> bool:
        return self.native.empty

    def peek_array(self) -> Any:
        self.assert_not_empty()
        return self.native.iloc[0].values.tolist()

    def count(self) -> int:
        return self.native.shape[0]

    def as_pandas(self) -> pd.DataFrame:
        return self._native

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        cols = (self.schema - cols).names
        return self._select_cols(cols)

    def _select_cols(self, cols: List[Any]) -> DataFrame:
        schema = self.schema.extract(cols)
        return PandasDataFrame(
            self.native[schema.names], schema, pandas_df_wrapper=True
        )

    def rename(self, columns: Dict[str, str]) -> "DataFrame":
        try:
            schema = self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError(e)
        df = self.native.rename(columns=columns)
        return PandasDataFrame(df, schema, pandas_df_wrapper=True)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return list(self.as_array_iterable(columns, type_safe=type_safe))

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        for row in PD_UTILS.as_array_iterable(
            self.native,
            schema=self.schema.pa_schema,
            columns=columns,
            type_safe=type_safe,
        ):
            yield row

    def _apply_schema(
        self, pdf: pd.DataFrame, schema: Optional[Schema]
    ) -> Tuple[pd.DataFrame, Schema]:
        PD_UTILS.ensure_compatible(pdf)
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
