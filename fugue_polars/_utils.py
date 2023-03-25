import polars as pl
import pyarrow as pa
from triad import Schema
from triad.utils.pyarrow import get_alter_func

from fugue.dataframe.arrow_dataframe import _build_empty_arrow


def pl_as_arrow(df: pl.DataFrame) -> pa.Table:
    adf = df.to_arrow()
    schema = convert_schema(adf.schema)
    func = get_alter_func(adf.schema, schema, safe=False)
    return func(adf)


def to_schema(df: pl.DataFrame) -> Schema:
    return Schema(convert_schema(pl.DataFrame(schema=df.schema).to_arrow().schema))


def build_empty_pl(schema: Schema) -> pl.DataFrame:
    return pl.from_arrow(_build_empty_arrow(schema))


def convert_schema(schema: pa.Schema) -> pa.Schema:
    fields = [convert_field(f) for f in schema]
    return pa.schema(fields)


def convert_field(field: pa.Field) -> pa.Field:
    tp = convert_type(field.type)
    if tp == field.type:
        return field
    return pa.field(field.name, tp)


def convert_type(tp: pa.DataType) -> pa.DataType:
    if pa.types.is_struct(tp):
        return pa.struct([convert_field(f) for f in tp])
    if pa.types.is_list(tp) or pa.types.is_large_list(tp):
        return pa.list_(convert_type(tp.value_type))
    if pa.types.is_map(tp):  # pragma: no cover
        return pa.map_(convert_type(tp.key_type), convert_type(tp.value_type))
    if pa.types.is_large_string(tp):
        return pa.string()
    if pa.types.is_large_binary(tp):
        return pa.binary()
    return tp
