from typing import Any, Iterable, List, Tuple

import pyarrow as pa
import pyspark.sql as ps
import pyspark.sql.types as pt

try:  # pyspark < 3
    from pyspark.sql.types import from_arrow_type, to_arrow_type  # type: ignore

    # https://issues.apache.org/jira/browse/SPARK-29041
    pt._acceptable_types[pt.BinaryType] = (bytearray, bytes)  # type: ignore  # pragma: no cover  # noqa: E501  # pylint: disable=line-too-long
except ImportError:  # pyspark >=3
    from pyspark.sql.pandas.types import from_arrow_type, to_arrow_type
from pyarrow.types import is_list, is_struct, is_timestamp
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
        lambda: ValueError(f"schema mismatch: {schema1}, {schema2}"),
    )
    expr: List[str] = []
    has_cast = False
    for i in range(len(schema1)):
        name_match = schema1[i].name == schema2[i].name
        assert_or_throw(
            name_match or allow_name_mismatch,
            lambda: ValueError(f"schema name mismatch: {schema1}, {schema2}"),
        )
        if schema1[i].dataType != schema2[i].dataType:
            type2 = schema2[i].dataType.simpleString()
            if isinstance(schema1[i].dataType, pt.FractionalType) and isinstance(
                schema2[i].dataType, (pt.StringType, pt.IntegralType)
            ):
                expr.append(
                    f"CAST(IF(isnan({schema1[i].name}) OR {schema1[i].name} IS NULL"
                    f", NULL, {schema1[i].name})"
                    f" AS {type2}) {schema2[i].name}"
                )
            else:
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
    struct_idx = [p for p, t in enumerate(schema.types) if pa.types.is_struct(t)]
    complex_list_idx = [
        p
        for p, t in enumerate(schema.types)
        if pa.types.is_list(t) and pa.types.is_nested(t.value_type)
    ]
    if len(struct_idx) == 0 and len(complex_list_idx) == 0:
        for row in rows:
            yield list(row)
    elif len(complex_list_idx) == 0:
        for row in rows:
            r = list(row)
            for i in struct_idx:
                if r[i] is not None:
                    r[i] = r[i].asDict(recursive=True)
            yield r
    else:
        for row in rows:
            data = row.asDict(recursive=True)
            r = [data[n] for n in schema.names]
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
    return to_arrow_type(dt)


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
    elif is_list(dt):
        if is_timestamp(dt.value_type):
            raise TypeError(  # pragma: no cover
                "Spark: unsupported type in conversion from Arrow: " + str(dt)
            )
        return pt.ArrayType(_from_arrow_type(dt.value_type))
    return from_arrow_type(dt)


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
