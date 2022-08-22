from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import ray
import ray.data as rd
from fugue.dataframe import ArrowDataFrame, DataFrame, LocalDataFrame, PandasDataFrame
from fugue.dataframe.dataframe import _input_schema
from fugue.exceptions import FugueDataFrameEmptyError, FugueDataFrameOperationError
from triad.collections.schema import Schema

from ._ray_utils import build_empty, get_dataset_format, _build_empty_arrow


class RayDataFrame(DataFrame):
    """DataFrame that wraps Ray DataSet. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: :class:`dask:dask.dataframe.DataFrame`,
      pandas DataFrame or list or iterable of arrays
    :param schema: |SchemaLikeObject| or :class:`spark:pyspark.sql.types.StructType`,
      defaults to None.
    :param metadata: |ParamsLikeObject|, defaults to None
    :param num_partitions: initial number of partitions for the dask dataframe
      defaults to 0 to get the value from `fugue.dask.dataframe.default.partitions`
    :param type_safe: whether to cast input data to ensure type safe, defaults to True

    .. note::

        For :class:`dask:dask.dataframe.DataFrame`, schema must be None
    """

    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
        metadata: Any = None,
        internal_schema: bool = False,
    ):
        if internal_schema:
            schema = _input_schema(schema).assert_not_empty()
        if df is None:
            schema = _input_schema(schema).assert_not_empty()
            super().__init__(schema, metadata)
            self._native = build_empty(schema)
            return
        if isinstance(df, rd.Dataset):
            fmt = get_dataset_format(df)
            if fmt is None:  # empty:
                schema = _input_schema(schema).assert_not_empty()
                super().__init__(schema, metadata)
                self._native = build_empty(schema)
                return
            elif fmt == "pandas":
                rdf = rd.from_arrow_refs(df.to_arrow_refs())
            elif fmt == "arrow":
                rdf = df
            else:
                raise NotImplementedError(
                    f"Ray Dataset in {fmt} format is not supported"
                )
        elif isinstance(df, pa.Table):
            rdf = rd.from_arrow(df)
            if schema is None:
                schema = df.schema
        elif isinstance(df, RayDataFrame):
            rdf = df._native
            if schema is None:
                schema = df.schema
        elif isinstance(df, (pd.DataFrame, pd.Series)):
            if isinstance(df, pd.Series):
                df = df.to_frame()
            rdf = rd.from_arrow(ArrowDataFrame(df).native)
        elif isinstance(df, Iterable):
            schema = _input_schema(schema).assert_not_empty()
            t = ArrowDataFrame(df, schema)
            rdf = rd.from_arrow(t.as_arrow())
        elif isinstance(df, DataFrame):
            rdf = rd.from_arrow(df.as_arrow(type_safe=True))
            if schema is None:
                schema = df.schema
        else:
            raise ValueError(f"{df} is incompatible with DaskDataFrame")
        rdf, schema = self._apply_schema(rdf, schema, internal_schema)
        super().__init__(schema, metadata)
        self._native = rdf

    @property
    def native(self) -> rd.Dataset:
        """The wrapped ray Dataset"""
        return self._native

    @property
    def is_local(self) -> bool:
        return False

    def as_local(self) -> LocalDataFrame:
        adf = self.as_arrow()
        if adf.shape[0] == 0:
            return ArrowDataFrame([], self.schema)
        return ArrowDataFrame(adf)

    @property
    def is_bounded(self) -> bool:
        return True

    @property
    def empty(self) -> bool:
        return len(self.native.take(1)) == 0

    @property
    def num_partitions(self) -> int:
        return self.native.num_blocks()

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        cols = (self.schema - cols).names
        return self._select_cols(cols)

    def _select_cols(self, cols: List[Any]) -> DataFrame:
        rdf = self.native.map_batches(lambda b: b.select(cols), batch_format="pyarrow")
        return RayDataFrame(rdf, self.schema.extract(cols), internal_schema=True)

    def peek_array(self) -> Any:
        data = self.native.limit(1).to_pandas().values.tolist()
        if len(data) == 0:
            raise FugueDataFrameEmptyError
        return data[0]

    def persist(self, **kwargs: Any) -> "RayDataFrame":
        return RayDataFrame(
            self.native.fully_executed(),
            schema=self.schema,
            metadata=self.metadata,
            internal_schema=True,
        )

    def count(self) -> int:
        return self.native.count()

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        def get_tables() -> Iterable[pa.Table]:
            empty = True
            for block in self.native.get_internal_block_refs():
                tb = ray.get(block)
                if tb.shape[0] > 0:
                    yield tb
                    empty = False
            if empty:
                yield _build_empty_arrow(self.schema)

        return pa.concat_tables(get_tables())

    def as_pandas(self) -> pd.DataFrame:
        return self.as_arrow().to_pandas()

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            new_schema = self.schema.rename(columns)
            new_cols = new_schema.names
        except Exception as e:
            raise FugueDataFrameOperationError from e
        rdf = self.native.map_batches(
            lambda b: b.rename_columns(new_cols), batch_format="pyarrow"
        )
        return RayDataFrame(rdf, schema=new_schema, internal_schema=True)

    def alter_columns(self, columns: Any) -> DataFrame:
        def _alter(
            table: pa.Table,
        ) -> pa.Table:  # pragma: no cover (pytest can't capture)
            return ArrowDataFrame(table).alter_columns(columns).native  # type: ignore

        new_schema = self._get_altered_schema(columns)
        rdf = self.native.map_batches(_alter, batch_format="pyarrow")
        return RayDataFrame(rdf, schema=new_schema, internal_schema=True)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        df: DataFrame = self
        if columns is not None:
            df = df[columns]
        adf = df.as_arrow()
        if adf.shape[0] == 0:
            return []
        return ArrowDataFrame(adf).as_array(type_safe=type_safe)

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
        df: DataFrame = self
        if columns is not None:
            df = df[columns]
        pdf = df.native.limit(n).to_pandas()  # type: ignore
        if len(pdf) == 0:
            return []
        return PandasDataFrame(pdf, schema=df.schema).head(n)

    def _apply_schema(
        self, rdf: rd.Dataset, schema: Optional[Schema], internal_schema: bool
    ) -> Tuple[rd.Dataset, Schema]:
        if internal_schema:
            return rdf, schema
        if get_dataset_format(rdf) is None:  # empty
            schema = _input_schema(schema).assert_not_empty()
            return build_empty(schema), schema
        if schema is None or schema == rdf.schema(fetch_if_missing=True):
            return rdf, rdf.schema(fetch_if_missing=True)

        def _alter(table: pa.Table) -> pa.Table:  # pragma: no cover
            return ArrowDataFrame(table).alter_columns(schema).native  # type: ignore

        return rdf.map_batches(_alter, batch_format="pyarrow"), schema
