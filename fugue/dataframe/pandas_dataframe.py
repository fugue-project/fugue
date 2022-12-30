from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.pandas_like import PD_UTILS

from fugue.dataset.api import (
    as_fugue_dataset,
    as_local,
    as_local_bounded,
    count,
    get_num_partitions,
    is_bounded,
    is_empty,
    is_local,
)
from fugue.exceptions import FugueDataFrameOperationError

from .api import (
    drop_columns,
    get_column_names,
    get_schema,
    head,
    is_df,
    rename,
    select_columns,
)
from .dataframe import DataFrame, LocalBoundedDataFrame, _input_schema


class PandasDataFrame(LocalBoundedDataFrame):
    """DataFrame that wraps pandas DataFrame. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: 2-dimensional array, iterable of arrays or pandas DataFrame
    :param schema: |SchemaLikeObject|
    :param pandas_df_wrapper: if this is a simple wrapper, default False

    .. admonition:: Examples

        >>> PandasDataFrame([[0,'a'],[1,'b']],"a:int,b:str")
        >>> PandasDataFrame(schema = "a:int,b:int")  # empty dataframe
        >>> PandasDataFrame(pd.DataFrame([[0]],columns=["a"]))
        >>> PandasDataFrame(ArrayDataFrame([[0]],"a:int).as_pandas())

    .. note::

        If ``pandas_df_wrapper`` is True, then the constructor will not do any type
        check otherwise, it will enforce type according to the input schema after
        the construction
    """

    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
        pandas_df_wrapper: bool = False,
    ):
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
            pdf = PD_UTILS.enforce_type(pdf, schema.pa_schema, null_safe=True)
            if PD_UTILS.empty(pdf):
                for k, v in schema.items():
                    pdf[k] = pdf[k].astype(v.type.to_pandas_dtype())
            apply_schema = False
        else:
            raise ValueError(f"{df} is incompatible with PandasDataFrame")
        if apply_schema:
            pdf, schema = self._apply_schema(pdf, schema)
        super().__init__(schema)
        self._native = pdf

    @property
    def native(self) -> pd.DataFrame:
        """Pandas DataFrame"""
        return self._native

    def native_as_df(self) -> pd.DataFrame:
        return self._native

    @property
    def empty(self) -> bool:
        return self.native.empty

    def peek_array(self) -> List[Any]:
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

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            schema = self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        df = self.native.rename(columns=columns)
        return PandasDataFrame(df, schema, pandas_df_wrapper=True)

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        new_pdf = pd.DataFrame(self.native.to_dict(orient="series"))
        for k, v in new_schema.items():
            if not v.type.equals(self.schema[k].type):
                old_type = self.schema[k].type
                new_type = v.type
                # int -> str
                if pa.types.is_integer(old_type) and pa.types.is_string(new_type):
                    series = new_pdf[k]
                    ns = series.isnull()
                    series = series.fillna(0).astype(int).astype(str)
                    new_pdf[k] = series.mask(ns, None)
                # bool -> str
                elif pa.types.is_boolean(old_type) and pa.types.is_string(new_type):
                    series = new_pdf[k]
                    ns = series.isnull()
                    positive = series != 0
                    new_pdf[k] = "False"
                    new_pdf[k] = new_pdf[k].mask(positive, "True").mask(ns, None)
        return PandasDataFrame(new_pdf, new_schema)

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

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        pdf = self.native if columns is None else self.native[columns]
        schema = self.schema if columns is None else self.schema.extract(columns)
        return PandasDataFrame(pdf.head(n), schema=schema, pandas_df_wrapper=True)

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
                lambda: ValueError(
                    f"Pandas datafame column count doesn't match {schema}"
                ),
            )
            pdf.columns = schema.names
        return PD_UTILS.enforce_type(pdf, schema.pa_schema, null_safe=True), schema


@as_local.candidate(lambda df: isinstance(df, pd.DataFrame))
def _pd_as_local(df: pd.DataFrame) -> pd.DataFrame:
    return df


@as_local_bounded.candidate(lambda df: isinstance(df, pd.DataFrame))
def _pd_as_local_bounded(df: pd.DataFrame) -> pd.DataFrame:
    return df


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, pd.DataFrame))
def _pd_as_fugue_df(df: pd.DataFrame, **kwargs: Any) -> "PandasDataFrame":
    return PandasDataFrame(df, **kwargs)


@is_df.candidate(lambda df: isinstance(df, pd.DataFrame))
def _pd_is_df(df: pd.DataFrame) -> bool:
    return True


@count.candidate(lambda df: isinstance(df, pd.DataFrame))
def _pd_count(df: pd.DataFrame) -> int:
    return df.shape[0]


@is_bounded.candidate(lambda df: isinstance(df, pd.DataFrame))
def _pd_is_bounded(df: pd.DataFrame) -> bool:
    return True


@is_empty.candidate(lambda df: isinstance(df, pd.DataFrame))
def _pd_is_empty(df: pd.DataFrame) -> bool:
    return df.shape[0] == 0


@is_local.candidate(lambda df: isinstance(df, pd.DataFrame))
def _pd_is_local(df: pd.DataFrame) -> bool:
    return True


@get_num_partitions.candidate(lambda df: isinstance(df, pd.DataFrame))
def _get_pandas_num_partitions(df: pd.DataFrame) -> int:
    return 1


@get_column_names.candidate(lambda df: isinstance(df, pd.DataFrame))
def _get_pandas_dataframe_columns(df: pd.DataFrame) -> List[Any]:
    return list(df.columns)


@get_schema.candidate(lambda df: isinstance(df, pd.DataFrame))
def _get_pandas_dataframe_schema(df: pd.DataFrame) -> Schema:
    return Schema(df)


@rename.candidate(lambda df, *args, **kwargs: isinstance(df, pd.DataFrame))
def _rename_pandas_dataframe(
    df: pd.DataFrame, columns: Dict[str, Any], as_fugue: bool = False
) -> Any:
    if len(columns) == 0:
        return df
    _assert_no_missing(df, columns.keys())
    return _adjust_df(df.rename(columns=columns), as_fugue=as_fugue)


@drop_columns.candidate(lambda df, *args, **kwargs: isinstance(df, pd.DataFrame))
def _drop_pd_columns(
    df: pd.DataFrame, columns: List[str], as_fugue: bool = False
) -> Any:
    cols = [x for x in df.columns if x not in columns]
    if len(cols) == 0:
        raise FugueDataFrameOperationError("cannot drop all columns")
    if len(cols) + len(columns) != len(df.columns):
        _assert_no_missing(df, columns)
    return _adjust_df(df[cols], as_fugue=as_fugue)


@select_columns.candidate(lambda df, *args, **kwargs: isinstance(df, pd.DataFrame))
def _select_pd_columns(
    df: pd.DataFrame, columns: List[Any], as_fugue: bool = False
) -> Any:
    if len(columns) == 0:
        raise FugueDataFrameOperationError("must select at least one column")
    _assert_no_missing(df, columns)
    return _adjust_df(df[columns], as_fugue=as_fugue)


@head.candidate(lambda df, *args, **kwargs: isinstance(df, pd.DataFrame))
def _pd_head(
    df: pd.DataFrame,
    n: int,
    columns: Optional[List[str]] = None,
    as_fugue: bool = False,
) -> pd.DataFrame:
    if columns is not None:
        df = df[columns]
    return _adjust_df(df.head(n), as_fugue=as_fugue)


def _adjust_df(res: pd.DataFrame, as_fugue: bool):
    return res if not as_fugue else PandasDataFrame(res)


def _assert_no_missing(df: pd.DataFrame, columns: Iterable[Any]) -> None:
    missing = [x for x in columns if x not in df.columns]
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")
