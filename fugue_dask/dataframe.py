from typing import Any, Dict, Iterable, List, Optional, Tuple

import dask.dataframe as dd
import pandas
import pyarrow as pa
from triad.collections.schema import Schema
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.pyarrow import to_pandas_dtype

from fugue.dataframe import (
    ArrowDataFrame,
    DataFrame,
    LocalBoundedDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.dataframe import _input_schema
from fugue.exceptions import FugueDataFrameOperationError
from fugue.plugins import (
    as_local_bounded,
    count,
    drop_columns,
    get_column_names,
    get_num_partitions,
    head,
    is_bounded,
    is_df,
    is_empty,
    is_local,
    rename,
    select_columns,
)

from ._utils import DASK_UTILS, get_default_partitions


class DaskDataFrame(DataFrame):
    """DataFrame that wraps Dask DataFrame. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: :class:`dask:dask.dataframe.DataFrame`,
      pandas DataFrame or list or iterable of arrays
    :param schema: |SchemaLikeObject| or :class:`spark:pyspark.sql.types.StructType`,
      defaults to None.
    :param num_partitions: initial number of partitions for the dask dataframe
      defaults to 0 to get the value from `fugue.dask.default.partitions`
    :param type_safe: whether to cast input data to ensure type safe, defaults to True

    .. note::

        For :class:`dask:dask.dataframe.DataFrame`, schema must be None
    """

    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
        num_partitions: int = 0,
        type_safe=True,
    ):
        if num_partitions <= 0:
            num_partitions = get_default_partitions()
        if df is None:
            schema = _input_schema(schema).assert_not_empty()
            df = []
        if isinstance(df, DaskDataFrame):
            super().__init__(df.schema)
            self._native: dd.DataFrame = df._native
            return
        elif isinstance(df, (dd.DataFrame, dd.Series)):
            if isinstance(df, dd.Series):
                df = df.to_frame()
            pdf = df
            schema = None if schema is None else _input_schema(schema)
        elif isinstance(df, (pandas.DataFrame, pandas.Series)):
            if isinstance(df, pandas.Series):
                df = df.to_frame()
            pdf = dd.from_pandas(df, npartitions=num_partitions, sort=False)
            schema = None if schema is None else _input_schema(schema)
        elif isinstance(df, Iterable):
            schema = _input_schema(schema).assert_not_empty()
            t = PandasDataFrame(df, schema)
            pdf = dd.from_pandas(t.native, npartitions=num_partitions, sort=False)
            type_safe = False
        else:
            raise ValueError(f"{df} is incompatible with DaskDataFrame")
        pdf, schema = self._apply_schema(pdf, schema, type_safe)
        super().__init__(schema)
        self._native = pdf

    @property
    def native(self) -> dd.DataFrame:
        """The wrapped Dask DataFrame"""
        return self._native

    def native_as_df(self) -> dd.DataFrame:
        return self._native

    @property
    def is_local(self) -> bool:
        return False

    def as_local_bounded(self) -> LocalBoundedDataFrame:
        res = PandasDataFrame(self.as_pandas(), self.schema)
        if self.has_metadata:
            res.reset_metadata(self.metadata)
        return res

    @property
    def is_bounded(self) -> bool:
        return True

    @property
    def empty(self) -> bool:
        return DASK_UTILS.empty(self.native)

    @property
    def num_partitions(self) -> int:
        return _dd_get_num_partitions(self.native)

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        cols = (self.schema - cols).names
        return self._select_cols(cols)

    def _select_cols(self, cols: List[Any]) -> DataFrame:
        schema = self.schema.extract(cols)
        return DaskDataFrame(self.native[schema.names], schema, type_safe=False)

    def peek_array(self) -> List[Any]:
        self.assert_not_empty()
        return self.as_pandas().iloc[0].values.tolist()

    def persist(self, **kwargs: Any) -> "DaskDataFrame":
        self._native = self.native.persist(**kwargs)
        self._native.count().compute()
        return self

    def count(self) -> int:
        return self.native.shape[0].compute()

    def as_pandas(self) -> pandas.DataFrame:
        return self.native.compute().reset_index(drop=True)

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            schema = self.schema.rename(columns)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        df = self.native.rename(columns=columns)
        return DaskDataFrame(df, schema, type_safe=False)

    def alter_columns(self, columns: Any) -> DataFrame:
        new_schema = self._get_altered_schema(columns)
        if new_schema == self.schema:
            return self
        new_pdf = self.native.assign()
        pd_types = to_pandas_dtype(new_schema.pa_schema)
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
                # str -> bool
                elif pa.types.is_string(old_type) and pa.types.is_boolean(new_type):
                    series = new_pdf[k]
                    ns = series.isnull()
                    new_pdf[k] = (
                        series.fillna("true")
                        .apply(lambda x: None if x is None else x.lower())
                        .mask(ns, None)
                    )
                elif pa.types.is_integer(new_type):
                    series = new_pdf[k]
                    ns = series.isnull()
                    series = series.fillna(0).astype(pd_types[k])
                    new_pdf[k] = series.mask(ns, None)
                else:
                    series = new_pdf[k]
                    ns = series.isnull()
                    series = series.astype(pd_types[k])
                    new_pdf[k] = series.mask(ns, None)
        return DaskDataFrame(new_pdf, new_schema, type_safe=True)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        df: DataFrame = self
        if columns is not None:
            df = df[columns]
        return ArrowDataFrame(df.as_pandas(), schema=df.schema).as_array(
            type_safe=type_safe
        )

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        yield from self.as_array(columns=columns, type_safe=type_safe)

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        ddf = self.native if columns is None else self.native[columns]
        schema = self.schema if columns is None else self.schema.extract(columns)
        return PandasDataFrame(ddf.head(n, compute=True, npartitions=-1), schema=schema)

    def _apply_schema(
        self, pdf: dd.DataFrame, schema: Optional[Schema], type_safe: bool = True
    ) -> Tuple[dd.DataFrame, Schema]:
        if not type_safe:
            assert_arg_not_none(pdf, "pdf")
            assert_arg_not_none(schema, "schema")
            return pdf, schema
        DASK_UTILS.ensure_compatible(pdf)
        if pdf.columns.dtype == "object":  # pdf has named schema
            pschema = Schema(DASK_UTILS.to_schema(pdf))
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
        return DASK_UTILS.enforce_type(pdf, schema.pa_schema, null_safe=True), schema


@is_df.candidate(lambda df: isinstance(df, dd.DataFrame))
def _dd_is_df(df: dd.DataFrame) -> bool:
    return True


@get_num_partitions.candidate(lambda df: isinstance(df, dd.DataFrame))
def _dd_get_num_partitions(df: dd.DataFrame) -> int:
    return df.npartitions


@count.candidate(lambda df: isinstance(df, dd.DataFrame))
def _dd_count(df: dd.DataFrame) -> int:
    return df.shape[0].compute()


@is_bounded.candidate(lambda df: isinstance(df, dd.DataFrame))
def _dd_is_bounded(df: dd.DataFrame) -> bool:
    return True


@is_empty.candidate(lambda df: isinstance(df, dd.DataFrame))
def _dd_is_empty(df: dd.DataFrame) -> bool:
    return DASK_UTILS.empty(df)


@is_local.candidate(lambda df: isinstance(df, dd.DataFrame))
def _dd_is_local(df: dd.DataFrame) -> bool:
    return False


@as_local_bounded.candidate(lambda df: isinstance(df, dd.DataFrame))
def _dd_as_local(df: dd.DataFrame) -> bool:
    return df.compute()


@get_column_names.candidate(lambda df: isinstance(df, dd.DataFrame))
def _get_dask_dataframe_columns(df: dd.DataFrame) -> List[Any]:
    return list(df.columns)


@rename.candidate(lambda df, *args, **kwargs: isinstance(df, dd.DataFrame))
def _rename_dask_dataframe(df: dd.DataFrame, columns: Dict[str, Any]) -> dd.DataFrame:
    if len(columns) == 0:
        return df
    _assert_no_missing(df, columns.keys())
    return df.rename(columns=columns)


@drop_columns.candidate(lambda df, *args, **kwargs: isinstance(df, dd.DataFrame))
def _drop_dd_columns(
    df: dd.DataFrame, columns: List[str], as_fugue: bool = False
) -> Any:
    cols = [x for x in df.columns if x not in columns]
    if len(cols) == 0:
        raise FugueDataFrameOperationError("cannot drop all columns")
    if len(cols) + len(columns) != len(df.columns):
        _assert_no_missing(df, columns)
    return _adjust_df(df[cols], as_fugue=as_fugue)


@select_columns.candidate(lambda df, *args, **kwargs: isinstance(df, dd.DataFrame))
def _select_dd_columns(
    df: dd.DataFrame, columns: List[Any], as_fugue: bool = False
) -> Any:
    if len(columns) == 0:
        raise FugueDataFrameOperationError("must select at least one column")
    _assert_no_missing(df, columns)
    return _adjust_df(df[columns], as_fugue=as_fugue)


@head.candidate(lambda df, *args, **kwargs: isinstance(df, dd.DataFrame))
def _dd_head(
    df: dd.DataFrame,
    n: int,
    columns: Optional[List[str]] = None,
    as_fugue: bool = False,
) -> pandas.DataFrame:
    if columns is not None:
        df = df[columns]
    res = df.head(n, compute=True, npartitions=-1)
    return PandasDataFrame(res) if as_fugue else res


def _assert_no_missing(df: dd.DataFrame, columns: Iterable[Any]) -> None:
    missing = set(columns) - set(df.columns)
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")


def _adjust_df(res: dd.DataFrame, as_fugue: bool):
    return res if not as_fugue else DaskDataFrame(res)
