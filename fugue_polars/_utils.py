import polars as pl
from triad import Schema

from fugue.dataframe.arrow_dataframe import _build_empty_arrow


def build_empty_pl(schema: Schema) -> pl.DataFrame:
    return pl.from_arrow(_build_empty_arrow(schema))
