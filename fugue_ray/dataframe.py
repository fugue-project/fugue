from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import ray
import ray.data as rd
from triad.collections.schema import Schema

from fugue.dataframe import ArrowDataFrame, DataFrame, LocalBoundedDataFrame
from fugue.dataframe.dataframe import _input_schema
from fugue.exceptions import FugueDataFrameOperationError, FugueDatasetEmptyError
from fugue.plugins import (
    as_local_bounded,
    get_column_names,
    get_num_partitions,
    is_df,
    rename,
)

from ._constants import _ZERO_COPY
from ._utils.dataframe import build_empty, get_dataset_format, materialize, to_schema


class RayDataFrame(DataFrame):
    """DataFrame that wraps Ray DataSet. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: :class:`ray:ray.data.Dataset`, :class:`pa:pyarrow.Table`,
      :class:`pd:pandas.DataFrame`,
      Fugue :class:`~fugue.dataframe.dataframe.DataFrame`,
      or list or iterable of arrays
    :param schema: |SchemaLikeObject|, defaults to None. If the schema
      is different from the ``df`` schema, then type casts will happen.
    :param internal_schema: for internal schema, it means the schema
      is guaranteed by the provider to be consistent with the schema of
      ``df``, so no type cast will happen. Defaults to False. This is
      for internal use only.
    """

    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
        internal_schema: bool = False,
    ):
        metadata: Any = None
        if internal_schema:
            schema = _input_schema(schema).assert_not_empty()
        if df is None:
            schema = _input_schema(schema).assert_not_empty()
            super().__init__(schema)
            self._native = build_empty(schema)
            return
        if isinstance(df, rd.Dataset):
            fmt, df = get_dataset_format(df)
            if fmt is None:  # empty:
                schema = _input_schema(schema).assert_not_empty()
                super().__init__(schema)
                self._native = build_empty(schema)
                return
            elif fmt == "pandas":
                rdf = rd.from_arrow_refs(df.to_arrow_refs())
            elif fmt == "arrow":
                rdf = df
            else:  # pragma: no cover
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
            metadata = None if not df.has_metadata else df.metadata
        elif isinstance(df, (pd.DataFrame, pd.Series)):
            if isinstance(df, pd.Series):
                df = df.to_frame()
            adf = ArrowDataFrame(df)
            rdf = rd.from_arrow(adf.native)
            if schema is None:
                schema = adf.schema
        elif isinstance(df, Iterable):
            schema = _input_schema(schema).assert_not_empty()
            t = ArrowDataFrame(df, schema)
            rdf = rd.from_arrow(t.as_arrow())
        elif isinstance(df, DataFrame):
            rdf = rd.from_arrow(df.as_arrow(type_safe=True))
            if schema is None:
                schema = df.schema
            metadata = None if not df.has_metadata else df.metadata
        else:
            raise ValueError(f"{df} is incompatible with RayDataFrame")
        rdf, schema = self._apply_schema(rdf, schema, internal_schema)
        super().__init__(schema)
        self._native = rdf
        if metadata is not None:
            self.reset_metadata(metadata)

    @property
    def native(self) -> rd.Dataset:
        """The wrapped ray Dataset"""
        return self._native

    def native_as_df(self) -> rd.Dataset:
        return self._native

    @property
    def is_local(self) -> bool:
        return False

    def as_local_bounded(self) -> LocalBoundedDataFrame:
        adf = self.as_arrow()
        if adf.shape[0] == 0:
            res = ArrowDataFrame([], self.schema)
        else:
            res = ArrowDataFrame(adf)
        if self.has_metadata:
            res.reset_metadata(self.metadata)
        return res

    @property
    def is_bounded(self) -> bool:
        return True

    @property
    def empty(self) -> bool:
        return len(self.native.take(1)) == 0

    @property
    def num_partitions(self) -> int:
        return _rd_num_partitions(self.native)

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        cols = (self.schema - cols).names
        return self._select_cols(cols)

    def _select_cols(self, cols: List[Any]) -> DataFrame:
        if cols == self.columns:
            return self
        rdf = self.native.map_batches(
            lambda b: b.select(cols),
            batch_format="pyarrow",
            **_ZERO_COPY,
            **self._remote_args(),
        )
        return RayDataFrame(rdf, self.schema.extract(cols), internal_schema=True)

    def peek_array(self) -> List[Any]:
        data = self.native.limit(1).to_pandas().values.tolist()
        if len(data) == 0:
            raise FugueDatasetEmptyError
        return data[0]

    def persist(self, **kwargs: Any) -> "RayDataFrame":
        # TODO: it mutates the dataframe, is this a good bahavior
        self._native = materialize(self._native)
        return self

    def count(self) -> int:
        return self.native.count()

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        return pa.concat_tables(_get_arrow_tables(self.native))

    def as_pandas(self) -> pd.DataFrame:
        return self.as_arrow().to_pandas()

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            new_schema = self.schema.rename(columns)
            new_cols = new_schema.names
        except Exception as e:
            raise FugueDataFrameOperationError from e
        rdf = self.native.map_batches(
            lambda b: b.rename_columns(new_cols),
            batch_format="pyarrow",
            **_ZERO_COPY,
            **self._remote_args(),
        )
        return RayDataFrame(rdf, schema=new_schema, internal_schema=True)

    def alter_columns(self, columns: Any) -> DataFrame:
        def _alter(
            table: pa.Table,
        ) -> pa.Table:  # pragma: no cover (pytest can't capture)
            return ArrowDataFrame(table).alter_columns(columns).native  # type: ignore

        new_schema = self._get_altered_schema(columns)
        if self.schema == new_schema:
            return self
        rdf = self.native.map_batches(
            _alter, batch_format="pyarrow", **_ZERO_COPY, **self._remote_args()
        )
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

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        if columns is not None:
            return self[columns].head(n)
        pdf = RayDataFrame(self.native.limit(n), schema=self.schema)
        return pdf.as_local()  # type: ignore

    def _apply_schema(
        self, rdf: rd.Dataset, schema: Optional[Schema], internal_schema: bool
    ) -> Tuple[rd.Dataset, Schema]:
        if internal_schema:
            return rdf, schema
        fmt, rdf = get_dataset_format(rdf)
        if fmt is None:  # empty
            schema = _input_schema(schema).assert_not_empty()
            return build_empty(schema), schema
        if schema is None or schema == to_schema(rdf.schema(fetch_if_missing=True)):
            return rdf, to_schema(rdf.schema(fetch_if_missing=True))

        def _alter(table: pa.Table) -> pa.Table:  # pragma: no cover
            return ArrowDataFrame(table).alter_columns(schema).native  # type: ignore

        return (
            rdf.map_batches(
                _alter, batch_format="pyarrow", **_ZERO_COPY, **self._remote_args()
            ),
            schema,
        )

    def _remote_args(self) -> Dict[str, Any]:
        return {"num_cpus": 1}


@is_df.candidate(lambda df: isinstance(df, rd.Dataset))
def _rd_is_df(df: rd.Dataset) -> bool:
    return True


@get_num_partitions.candidate(lambda df: isinstance(df, rd.Dataset))
def _rd_num_partitions(df: rd.Dataset) -> int:
    return df.num_blocks()


@as_local_bounded.candidate(lambda df: isinstance(df, rd.Dataset))
def _rd_as_local(df: rd.Dataset) -> bool:
    return pa.concat_tables(_get_arrow_tables(df))


@get_column_names.candidate(lambda df: isinstance(df, rd.Dataset))
def _get_ray_dataframe_columns(df: rd.Dataset) -> List[Any]:
    if hasattr(df, "columns"):  # higher version of ray
        return df.columns(fetch_if_missing=True)
    else:  # pragma: no cover
        fmt, _ = get_dataset_format(df)
        if fmt == "pandas":
            return list(df.schema(True).names)
        elif fmt == "arrow":
            return df.schema(fetch_if_missing=True).names
        raise NotImplementedError(f"{fmt} is not supported")  # pragma: no cover


@rename.candidate(lambda df, *args, **kwargs: isinstance(df, rd.Dataset))
def _rename_ray_dataframe(df: rd.Dataset, columns: Dict[str, Any]) -> rd.Dataset:
    if len(columns) == 0:
        return df
    cols = _get_ray_dataframe_columns(df)
    missing = set(columns.keys()) - set(cols)
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")
    new_cols = [columns.get(name, name) for name in cols]
    return df.map_batches(
        lambda b: b.rename_columns(new_cols), batch_format="pyarrow", **_ZERO_COPY
    )


def _get_arrow_tables(df: rd.Dataset) -> Iterable[pa.Table]:
    last_empty: Any = None
    empty = True
    for block in df.get_internal_block_refs():
        tb = ray.get(block)
        if tb.shape[0] > 0:
            yield tb
            empty = False
        else:
            last_empty = tb
    if empty:
        yield last_empty
