from typing import Any, Dict, Iterable, List, Optional, Tuple

import dask.dataframe as pd
import pandas
import pyarrow as pa
from fugue.dataframe import DataFrame, LocalDataFrame, PandasDataFrame
from fugue.dataframe.dataframe import _input_schema
from fugue.exceptions import FugueDataFrameInitError, FugueDataFrameOperationError
from triad.collections.schema import Schema
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.pyarrow import to_pandas_dtype

from fugue_dask._constants import (
    FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS,
    FUGUE_DASK_DEFAULT_CONF,
)
from fugue_dask._utils import DASK_UTILS


class DaskDataFrame(DataFrame):
    """DataFrame that wraps Dask DataFrame. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: :class:`dask:dask.dataframe.DataFrame`,
      pandas DataFrame or list or iterable of arrays
    :param schema: |SchemaLikeObject| or :class:`spark:pyspark.sql.types.StructType`,
      defaults to None.
    :param metadata: |ParamsLikeObject|, defaults to None
    :param num_partitions: initial number of partitions for the dask dataframe
      defaults to 0 to get the value from `fugue.dask.dataframe.default.partitions`
    :param type_safe: whether to cast input data to ensure type safe, defaults to True

    :raises FugueDataFrameInitError: if the input is not compatible

    .. note::

        For :class:`dask:dask.dataframe.DataFrame`, schema must be None
    """

    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
        metadata: Any = None,
        num_partitions: int = 0,
        type_safe=True,
    ):
        try:
            if num_partitions <= 0:
                num_partitions = FUGUE_DASK_DEFAULT_CONF[
                    FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS
                ]
            if df is None:
                schema = _input_schema(schema).assert_not_empty()
                df = []
            if isinstance(df, DaskDataFrame):
                super().__init__(
                    df.schema, df.metadata if metadata is None else metadata
                )
                self._native: pd.DataFrame = df._native
                return
            elif isinstance(df, (pd.DataFrame, pd.Series)):
                if isinstance(df, pd.Series):
                    df = df.to_frame()
                pdf = df
                schema = None if schema is None else _input_schema(schema)
            elif isinstance(df, (pandas.DataFrame, pandas.Series)):
                if isinstance(df, pandas.Series):
                    df = df.to_frame()
                pdf = pd.from_pandas(df, npartitions=num_partitions, sort=False)
                schema = None if schema is None else _input_schema(schema)
            elif isinstance(df, Iterable):
                schema = _input_schema(schema).assert_not_empty()
                t = PandasDataFrame(df, schema)
                pdf = pd.from_pandas(t.native, npartitions=num_partitions, sort=False)
                type_safe = False
            else:
                raise ValueError(f"{df} is incompatible with DaskDataFrame")
            pdf, schema = self._apply_schema(pdf, schema, type_safe)
            super().__init__(schema, metadata)
            self._native = pdf
        except Exception as e:
            raise FugueDataFrameInitError from e

    @property
    def native(self) -> pd.DataFrame:
        """The wrapped Dask DataFrame

        :rtype: :class:`dask:dask.dataframe.DataFrame`
        """
        return self._native

    @property
    def is_local(self) -> bool:
        return False

    def as_local(self) -> LocalDataFrame:
        return PandasDataFrame(self.as_pandas(), self.schema, self.metadata)

    @property
    def is_bounded(self) -> bool:
        return True

    @property
    def empty(self) -> bool:
        return DASK_UTILS.empty(self.native)

    @property
    def num_partitions(self) -> int:
        return self.native.npartitions

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        cols = (self.schema - cols).names
        return self._select_cols(cols)

    def _select_cols(self, cols: List[Any]) -> DataFrame:
        schema = self.schema.extract(cols)
        return DaskDataFrame(self.native[schema.names], schema, type_safe=False)

    def peek_array(self) -> Any:
        self.assert_not_empty()
        return self.as_pandas().iloc[0].values.tolist()

    def persist(self, **kwargs: Any) -> "DaskDataFrame":
        self._native = self.native.persist(**kwargs)
        return self

    def count(self) -> int:
        return self.as_pandas().shape[0]

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
        return PandasDataFrame(df.as_pandas(), schema=df.schema).as_array(
            type_safe=type_safe
        )

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        yield from self.as_array(columns=columns, type_safe=type_safe)

    def head(self, n: int, columns: Optional[List[str]] = None) -> List[Any]:
        """Get first n rows of the dataframe as 2-dimensional array
        :param n: number of rows
        :param columns: selected columns, defaults to None (all columns)
        :return: 2-dimensional array
        """
        tdf = PandasDataFrame(
            self.native.head(n, compute=True, npartitions=-1), schema=self.schema
        )
        return tdf.head(n, columns=columns)

    def _apply_schema(
        self, pdf: pd.DataFrame, schema: Optional[Schema], type_safe: bool = True
    ) -> Tuple[pd.DataFrame, Schema]:
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
