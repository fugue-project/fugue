from fugue.dev import annotated_param, LocalDataFrameParam
from fugue import DataFrame, LocalDataFrameIterableDataFrame, ArrowDataFrame
from triad import Schema, make_empty_aware
import polars as pl
import pyarrow as pa
from typing import Any, Optional, Iterable, no_type_check, Iterator


@annotated_param(pl.DataFrame)
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


@annotated_param(
    Iterable[pl.DataFrame],
    matcher=lambda x: x == Iterable[pl.DataFrame] or x == Iterator[pl.DataFrame],
)
class _IterablePolarsParam(LocalDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[pa.Table]:
        if not isinstance(df, LocalDataFrameIterableDataFrame):
            yield pl.from_arrow(df.as_arrow())
        else:
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
            return LocalDataFrameIterableDataFrame(_dfs)
        return LocalDataFrameIterableDataFrame([], schema=_schema)

    @no_type_check
    def count(self, df: Iterable[pl.DataFrame]) -> int:
        return sum(_.shape[0] for _ in df)

    def format_hint(self) -> Optional[str]:
        return "pyarrow"


def _to_adf(output: pl.DataFrame, schema: Any) -> ArrowDataFrame:
    adf = ArrowDataFrame(output.to_arrow())
    if schema is None:
        return adf
    _schema = schema if isinstance(schema, Schema) else Schema(schema)
    if adf.schema == _schema:
        return adf
    return adf[_schema.names].alter_columns(_schema)  # type: ignore
