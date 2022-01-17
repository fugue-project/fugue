from datetime import datetime, date
import pyarrow as pa
from typing import Any, Dict, Iterable, Tuple
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP
from uuid import uuid4
import pandas as pd
import numpy as np

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

_PA_TYPES_TO_DUCK: Dict[pa.DataType, str] = {v: k for k, v in _DUCK_TYPES_TO_PA.items()}


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


def get_temp_df_name() -> str:
    return "_" + str(uuid4())[:5]


def to_duck_type(tp: pa.DataType) -> str:
    try:
        if pa.types.is_struct(tp):
            inner = ",".join(f.name + ": " + to_duck_type(f.type) for f in tp)
            return f"STRUCT<{inner}>"
        if pa.types.is_list(tp):
            inner = to_duck_type(tp.value_type)
            return f"LIST<{inner}>"
        if pa.types.is_decimal(tp):
            return f"DECIMAL({tp.precision}, {tp.scale})"
        return _PA_TYPES_TO_DUCK[tp]
    except Exception:  # pragma: no cover
        raise ValueError(f"can't convert {tp} to DuckDB data type")


def to_pa_type(duck_type: str) -> pa.DataType:
    try:
        p = duck_type.find("<")
        if p > 0:
            tp = duck_type[:p]
            if tp == "LIST":
                itp = to_pa_type(duck_type[p + 1 : -1])
                return pa.list_(itp)
            if tp == "STRUCT":
                fields = [
                    pa.field(k, to_pa_type(v.strip()))
                    for k, v in _split_comma(duck_type[p + 1 : -1])
                ]
                return pa.struct(fields)
            raise Exception
        p = duck_type.find("(")
        if p > 0:
            if duck_type[:p] != "DECIMAL":
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


def _split_comma(expr: str) -> Iterable[Tuple[str, str]]:
    lv = 0
    start = 0
    for i in range(len(expr)):
        if expr[i] == "<":
            lv += 1
        elif expr[i] == ">":
            lv -= 1
        elif lv == 0 and expr[i] == ",":
            x = expr[start:i].strip().split(":", 1)
            yield x[0], x[1]
            start = i + 1
    x = expr[start : len(expr)].strip().split(":", 1)
    yield x[0], x[1]
