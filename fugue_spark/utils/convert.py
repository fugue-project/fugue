from typing import Any, Iterable, List, Tuple

import pyarrow as pa
import pyspark.sql as ps
import pyspark.sql.types as pt
from pyarrow.types import is_struct
from triad.collections import Schema
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP


def to_spark_schema(obj: Any) -> pt.StructType:
    assert_arg_not_none(obj, "schema")
    if isinstance(obj, pt.StructType):
        return obj
    if isinstance(obj, ps.DataFrame):
        return obj.schema
    return _from_arrow_schema(Schema(obj).pa_schema)


def to_schema(obj: Any) -> Schema:
    assert_arg_not_none(obj, "obj")
    if isinstance(obj, pt.StructType):
        return Schema(_to_arrow_schema(obj))
    if isinstance(obj, ps.DataFrame):
        return to_schema(obj.schema)
    return Schema(obj)


def to_cast_expression(
    schema1: Any, schema2: Any, allow_name_mismatch: bool
) -> Tuple[bool, List[str]]:
    schema1 = to_spark_schema(schema1)
    schema2 = to_spark_schema(schema2)
    assert_or_throw(
        len(schema1) == len(schema2),
        ValueError(f"schema mismatch: {schema1}, {schema2}"),
    )
    expr: List[str] = []
    has_cast = False
    for i in range(len(schema1)):
        name_match = schema1[i].name == schema2[i].name
        assert_or_throw(
            name_match or allow_name_mismatch,
            ValueError(f"schema name mismatch: {schema1}, {schema2}"),
        )
        if schema1[i].dataType != schema2[i].dataType:
            type2 = schema2[i].dataType.simpleString()
            expr.append(f"CAST({schema1[i].name} AS {type2}) {schema2[i].name}")
            has_cast = True
        else:
            if schema1[i].name != schema2[i].name:
                expr.append(f"{schema1[i].name} AS {schema2[i].name}")
                has_cast = True
            else:
                expr.append(schema1[i].name)
    return has_cast, expr


def to_select_expression(schema_from: Any, schema_to: Any) -> List[str]:
    schema1 = to_spark_schema(schema_from)
    if isinstance(schema_to, List):
        return [schema1[n].name for n in schema_to]
    schema2 = to_spark_schema(schema_to)
    sub = pt.StructType([schema1[x.name] for x in schema2.fields])
    _, expr = to_cast_expression(sub, schema2, allow_name_mismatch=False)
    return expr


def to_type_safe_input(rows: Iterable[ps.Row], schema: Schema) -> Iterable[List[Any]]:
    idx = [p for p, t in enumerate(schema.types) if pa.types.is_struct(t)]
    if len(idx) == 0:
        for row in rows:
            yield list(row)
    else:
        for row in rows:
            r = list(row)
            for i in idx:
                if r[i] is not None:
                    r[i] = r[i].asDict()
            yield r


# TODO: the following function always set nullable to true,
# but should we use field.nullable?
def _to_arrow_type(dt: pt.DataType) -> pa.DataType:
    if isinstance(dt, pt.TimestampType):
        return TRIAD_DEFAULT_TIMESTAMP
    if isinstance(dt, pt.StructType):
        fields = [
            pa.field(
                # field.name, _to_arrow_type(field.dataType), nullable=field.nullable
                field.name,
                _to_arrow_type(field.dataType),
                nullable=True,
            )
            for field in dt
        ]
        return pa.struct(fields)
    return pt.to_arrow_type(dt)


def _to_arrow_schema(schema: pt.StructType) -> pa.Schema:
    fields = [
        # pa.field(field.name, _to_arrow_type(field.dataType), nullable=field.nullable)
        pa.field(field.name, _to_arrow_type(field.dataType), nullable=True)
        for field in schema
    ]
    return pa.schema(fields)


def _from_arrow_type(dt: pa.DataType) -> pt.DataType:
    if is_struct(dt):
        return pt.StructType(
            [
                pt.StructField(
                    # field.name, _from_arrow_type(field.type), nullable=field.nullable
                    field.name,
                    _from_arrow_type(field.type),
                    nullable=True,
                )
                for field in dt
            ]
        )
    return pt.from_arrow_type(dt)


def _from_arrow_schema(schema: pa.Schema) -> pt.StructType:
    return pt.StructType(
        [
            pt.StructField(
                # field.name, _from_arrow_type(field.type), nullable=field.nullable
                field.name,
                _from_arrow_type(field.type),
                nullable=True,
            )
            for field in schema
        ]
    )
