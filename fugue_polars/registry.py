from typing import Any, Iterable, Iterator, Optional, no_type_check

import polars as pl
import pyarrow as pa
from triad import Schema, make_empty_aware
from triad.utils.pyarrow import get_alter_func

from fugue import (
    ArrowDataFrame,
    DataFrame,
    IterableArrowDataFrame,
    LocalDataFrameIterableDataFrame,
)
from fugue.dev import LocalDataFrameParam, fugue_annotated_param
from .polars_dataframe import PolarsDataFrame
from fugue.plugins import as_fugue_dataset


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, pl.DataFrame))
def _pl_as_fugue_df(df: pl.DataFrame, **kwargs: Any) -> PolarsDataFrame:
    return PolarsDataFrame(df, **kwargs)


@fugue_annotated_param(pl.DataFrame)
class _PolarsParam(LocalDataFrameParam):
    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        return pl.from_arrow(df.as_arrow())

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, pl.DataFrame)
        return _to_adf(output, schema=schema)

    def count(self, df: Any) -> int:  # pragma: no cover
        return df.shape[0]

    def format_hint(self) -> Optional[str]:
        return "pyarrow"


@fugue_annotated_param(
    Iterable[pl.DataFrame],
    matcher=lambda x: x == Iterable[pl.DataFrame] or x == Iterator[pl.DataFrame],
)
class _IterablePolarsParam(LocalDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[pa.Table]:
        if not isinstance(df, LocalDataFrameIterableDataFrame):
            yield pl.from_arrow(df.as_arrow())
        else:  # pragma: no cover # spark code coverage can't be included
            for sub in df.native:
                yield pl.from_arrow(sub.as_arrow())

    @no_type_check
    def to_output_df(
        self, output: Iterable[pl.DataFrame], schema: Any, ctx: Any
    ) -> DataFrame:
        def dfs(_schema: Schema) -> Iterable[ArrowDataFrame]:
            if output is not None:
                for df in output:
                    yield _to_adf(df, _schema)

        _schema: Optional[Schema] = (
            None
            if schema is None
            else (schema if isinstance(schema, Schema) else Schema(schema))
        )
        _dfs = make_empty_aware(dfs(_schema))
        if not _dfs.empty:
            return IterableArrowDataFrame(_dfs)
        return IterableArrowDataFrame([], schema=_schema)

    @no_type_check
    def count(self, df: Iterable[pl.DataFrame]) -> int:  # pragma: no cover
        return sum(_.shape[0] for _ in df)

    def format_hint(self) -> Optional[str]:
        return "pyarrow"


def _to_adf(output: pl.DataFrame, schema: Any) -> ArrowDataFrame:
    adf = output.to_arrow()
    if schema is None:  # pragma: no cover
        return ArrowDataFrame(adf)
    _schema = schema if isinstance(schema, Schema) else Schema(schema)
    f = get_alter_func(adf.schema, _schema.pa_schema, safe=False)
    return ArrowDataFrame(f(adf))
