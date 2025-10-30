import pickle
from typing import Any, Iterable, List, Tuple, Optional


import pandas as pd
import pyarrow as pa
import pyspark
import pyspark.sql as ps
import pyspark.sql.types as pt
from packaging import version
from pyarrow.types import is_list, is_struct, is_timestamp
from pyspark.sql.pandas.types import (
    from_arrow_schema,
    from_arrow_type,
    to_arrow_schema,
    to_arrow_type,
)
from triad.collections import Schema
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP, cast_pa_table
from triad.utils.schema import quote_name

import fugue.api as fa
from fugue import DataFrame

from .misc import is_spark_dataframe

try:
    from pyspark.sql.types import TimestampNTZType  # pylint: disable-all
except ImportError:  # pragma: no cover
    # pyspark < 3.2
    from pyspark.sql.types import TimestampType as TimestampNTZType

_PYSPARK_ARROW_FRIENDLY = version.parse(pyspark.__version__) >= version.parse("3.3")


def pandas_udf_can_accept(schema: Schema, is_input: bool) -> bool:
    try:
        # pyspark's own from_arrow_schema to_arrow_schema
        # can validate if a type can be supported by pandas udf
        if not is_input and any(pa.types.is_struct(t) for t in schema.types):
            return False
        to_arrow_schema(from_arrow_schema(schema.pa_schema))
        return True
    except Exception:  # pragma: no cover
        return False


def to_spark_schema(obj: Any) -> pt.StructType:
    assert_arg_not_none(obj, "schema")
    if isinstance(obj, pt.StructType):
        return obj
    if is_spark_dataframe(obj):
        return obj.schema
    return _from_arrow_schema(Schema(obj).pa_schema)


def to_schema(obj: Any) -> Schema:
    assert_arg_not_none(obj, "obj")
    if isinstance(obj, pt.StructType):
        return Schema(_to_arrow_schema(obj))
    if is_spark_dataframe(obj):
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
        n1, n2 = quote_name(schema1[i].name, quote="`"), quote_name(
            schema2[i].name, quote="`"
        )
        if schema1[i].dataType != schema2[i].dataType:
            type2 = schema2[i].dataType.simpleString()
            if isinstance(schema1[i].dataType, pt.FractionalType) and isinstance(
                schema2[i].dataType, (pt.StringType, pt.IntegralType)
            ):
                expr.append(
                    f"CAST(IF(isnan({n1}) OR {n1} IS NULL"
                    f", NULL, {n1})"
                    f" AS {type2}) {n2}"
                )
            else:
                expr.append(f"CAST({n1} AS {type2}) {n2}")
            has_cast = True
        else:
            if schema1[i].name != schema2[i].name:
                expr.append(f"{n1} AS {n2}")
                has_cast = True
            else:
                expr.append(n1)
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
    else:  # pragma: no cover
        for row in rows:
            data = row.asDict(recursive=True)
            r = [data[n] for n in schema.names]
            yield r


def to_spark_df(session: ps.SparkSession, df: Any, schema: Any = None) -> ps.DataFrame:
    if schema is not None and not isinstance(schema, pt.StructType):
        schema = to_spark_schema(schema)
    if isinstance(df, pd.DataFrame):
        if pd.__version__ >= "2" and session.version < "3.4":  # pragma: no cover
            # pyspark < 3.4 does not support pandas 2 when doing
            # createDataFrame, see this issue:
            # https://stackoverflow.com/a/75926954/12309438
            # this is a workaround with the cost of memory and speed.
            if schema is None:
                schema = to_spark_schema(fa.get_schema(df))
            df = fa.as_fugue_df(df).as_array(type_safe=True)
        return pd_to_spark_df(session, df, schema=schema)
    if isinstance(df, DataFrame):
        if pd.__version__ >= "2" and session.version < "3.4":  # pragma: no cover
            if schema is None:
                schema = to_spark_schema(df.schema)
            return session.createDataFrame(df.as_array(type_safe=True), schema=schema)
        return pd_to_spark_df(session, df.as_pandas(), schema=schema)
    else:
        return session.createDataFrame(df, schema=schema)


def pd_to_spark_df(
    session: ps.SparkSession, df: pd.DataFrame, schema: Any = None
) -> ps.DataFrame:
    if _PYSPARK_ARROW_FRIENDLY:
        return session.createDataFrame(df, schema=schema)
    else:  # pragma: no cover
        # Not efficient for pyspark<3.3
        return session.createDataFrame(df.astype(object), schema=schema)


def to_pandas(df: ps.DataFrame) -> pd.DataFrame:
    def _to_df() -> pd.DataFrame:
        if version.parse(pd.__version__) < version.parse("2.0.0") or not any(
            isinstance(x.dataType, (pt.TimestampType, TimestampNTZType))
            for x in df.schema.fields
        ):
            return df.toPandas()
        else:  # pragma: no cover

            def serialize(dfs):
                for df in dfs:
                    data = pickle.dumps(df)
                    yield pd.DataFrame([[data]], columns=["data"])

            sdf = df.mapInPandas(serialize, schema="data binary")
            return pd.concat(pickle.loads(x.data) for x in sdf.collect())

    pdf = _to_df()
    if hasattr(pdf, "attrs") and "metrics" in pdf.attrs:  # pragma: no cover
        del pdf.attrs["metrics"]
    return pdf


def to_arrow(df: ps.DataFrame) -> pa.Table:
    schema = to_schema(df.schema)
    destruct: Optional[bool] = None
    try:
        jconf = df.sparkSession._jconf
        if jconf.arrowPySparkEnabled() and pandas_udf_can_accept(
            schema, is_input=False
        ):
            destruct = jconf.arrowPySparkSelfDestructEnabled()
    except Exception:  # pragma: no cover
        # older spark does not have this config
        pass
    if destruct is not None and hasattr(df, "_collect_as_arrow"):
        batches = df._collect_as_arrow(split_batches=destruct)
        if len(batches) == 0:
            return schema.create_empty_arrow_table()
        table = pa.Table.from_batches(batches)
        del batches
        return cast_pa_table(table, schema.pa_schema)
    else:  # pragma: no cover
        # df.toPandas has bugs on nested types
        return pa.Table.from_pylist(df.collect(), schema=schema.pa_schema)


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
    if isinstance(dt, pt.ArrayType):
        return pa.list_(_to_arrow_type(dt.elementType))
    if isinstance(dt, pt.MapType):
        return pa.map_(_to_arrow_type(dt.keyType), _to_arrow_type(dt.valueType))
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
