import polars as pl
from triad import Schema


def build_empty_pl(schema: Schema) -> pl.DataFrame:
    return pl.from_arrow(schema.create_empty_arrow_table())
