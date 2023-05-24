from datetime import date, datetime
from typing import Any, Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
from triad import Schema
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP
from triad.utils.schema import quote_name

_DUCK_TYPES_TO_PA: Dict[str, pa.DataType] = {
    "BIGINT": pa.int64(),
    "BOOLEAN": pa.bool_(),
    "BLOB": pa.binary(),
    "DATE": pa.date32(),
    "DOUBLE": pa.float64(),
    "INTEGER": pa.int32(),
    "FLOAT": pa.float32(),
    "SMALLINT": pa.int16(),
    "TIMESTAMP": TRIAD_DEFAULT_TIMESTAMP,
    "TINYINT": pa.int8(),
    "UBIGINT": pa.uint64(),
    "UINTEGER": pa.uint32(),
    "USMALLINT": pa.uint16(),
    "UTINYINT": pa.uint8(),
    "VARCHAR": pa.string(),
    "TIME": pa.time32("ms"),
}

_PA_TYPES_TO_DUCK: Dict[pa.DataType, str] = {
    v: k
    for k, v in list(_DUCK_TYPES_TO_PA.items())
    + [("VARCHAR", pa.large_string()), ("BLOB", pa.large_binary())]
}


def encode_column_name(name: str) -> str:
    return quote_name(name, quote='"')


def encode_column_names(names: Iterable[str]) -> Iterable[str]:
    for name in names:
        yield encode_column_name(name)


def encode_schema_names(schema: Schema) -> Iterable[str]:
    return encode_column_names(schema.names)


def encode_value_to_expr(value: Any) -> str:  # noqa: C901
    if isinstance(value, list):
        return "[" + ", ".join(encode_value_to_expr(x) for x in value) + "]"
    if isinstance(value, dict):
        return (
            "{"
            + ", ".join(
                encode_value_to_expr(k) + ": " + encode_value_to_expr(v)
                for k, v in value.items()
            )
            + "}"
        )
    if pd.isna(value):
        return "NULL"
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, str):
        return "E" + repr(value)
    if isinstance(value, bytes):
        return repr(value)[1:] + "::BLOB"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return "TIMESTAMP '" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(value, date):
        return "DATE '" + value.strftime("%Y-%m-%d") + "'"

    raise NotImplementedError(value)


def to_duck_type(tp: pa.DataType) -> str:
    try:
        if pa.types.is_struct(tp):
            inner = ",".join(f.name + " " + to_duck_type(f.type) for f in tp)
            return f"STRUCT({inner})"
        if pa.types.is_map(tp):
            k = to_duck_type(tp.key_type)
            v = to_duck_type(tp.item_type)
            return f"MAP({k},{v})"
        if pa.types.is_list(tp):
            inner = to_duck_type(tp.value_type)
            return f"{inner}[]"
        if pa.types.is_decimal(tp):
            return f"DECIMAL({tp.precision}, {tp.scale})"
        return _PA_TYPES_TO_DUCK[tp]
    except Exception:  # pragma: no cover
        raise ValueError(f"can't convert {tp} to DuckDB data type")


def to_pa_type(duck_type_raw: Any) -> pa.DataType:
    try:
        duck_type = str(duck_type_raw)  # for duckdb >= 0.8.0
        if duck_type.endswith("[]"):
            return pa.list_(to_pa_type(duck_type[:-2]))
        p = duck_type.find("(")
        if p > 0:
            tp = duck_type[:p]
            if tp == "STRUCT":
                fields = [
                    pa.field(k, to_pa_type(v.strip()))
                    for k, v in _split_comma(duck_type[p + 1 : -1])
                ]
                return pa.struct(fields)
            if tp == "MAP":
                fields = [
                    to_pa_type(t.strip())
                    for t, _ in _split_comma(duck_type[p + 1 : -1], split_char=None)
                ]
                return pa.map_(fields[0], fields[1])
            if tp != "DECIMAL":
                raise Exception
            pair = duck_type[p + 1 : -1].split(",", 1)
            return pa.decimal128(int(pair[0]), int(pair[1]))
        if duck_type == "HUGEINT":
            return pa.int64()
        if duck_type in _DUCK_TYPES_TO_PA:
            return _DUCK_TYPES_TO_PA[duck_type]
        raise Exception
    except Exception:
        raise ValueError(f"{duck_type} is not supported")


def _split_comma(
    expr: str,
    split_char: Optional[str] = " ",
    left_char: str = "(",
    right_char: str = ")",
) -> Iterable[Tuple[str, str]]:
    lv = 0
    start = 0
    for i in range(len(expr)):
        if expr[i] == left_char:
            lv += 1
        elif expr[i] == right_char:
            lv -= 1
        elif lv == 0 and expr[i] == ",":
            if split_char is None:
                yield expr[start:i].strip(), ""
            else:
                x = expr[start:i].strip().split(split_char, 1)
                yield x[0], x[1]
            start = i + 1
    if split_char is None:
        yield expr[start : len(expr)].strip(), ""
    else:
        x = expr[start : len(expr)].strip().split(split_char, 1)
        yield x[0], x[1]
