from datetime import datetime

import fugue.column.functions as f
import pyarrow as pa
from fugue.column import col, lit, null
from triad import Schema
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP


def test_is_agg():
    assert f.is_agg(f.first(col("a")))
    assert f.is_agg(f.first(col("a")).alias("x"))
    assert f.is_agg(f.first(col("a")).distinct())
    assert f.is_agg(f.first(col("a") + 1))
    assert f.is_agg(f.first(col("a")) + 1)
    assert f.is_agg((f.first(col("a")) < 1).alias("x"))
    assert f.is_agg(col("a") * f.first(col("a")) + 1)

    assert not f.is_agg(col("a"))
    assert not f.is_agg(lit("a"))
    assert not f.is_agg(col("a") + col("b"))
    assert not f.is_agg(null())


def test_coalesce():
    schema = Schema("a:int,b:str,c:bool,d:double")

    expr = f.coalesce(col("a"), 1, None, col("b") + col("c"))
    assert "COALESCE(a,1,NULL,+(b,c))" == str(expr)
    assert expr.infer_schema(schema) is None

    expr = f.min(col("a"))
    assert "MIN(a)" == str(expr)
    assert pa.int32() == expr.infer_schema(schema)

    expr = f.min(lit(1.1))
    assert "MIN(1.1)" == str(expr)
    assert pa.float64() == expr.infer_schema(schema)

    expr = f.max(col("a"))
    assert "MAX(a)" == str(expr)
    assert pa.int32() == expr.infer_schema(schema)

    expr = f.max(lit(1.1))
    assert "MAX(1.1)" == str(expr)
    assert pa.float64() == expr.infer_schema(schema)

    expr = f.first(col("a"))
    assert "FIRST(a)" == str(expr)
    assert pa.int32() == expr.infer_schema(schema)

    expr = f.first(lit(1.1))
    assert "FIRST(1.1)" == str(expr)
    assert pa.float64() == expr.infer_schema(schema)

    expr = f.last(col("a"))
    assert "LAST(a)" == str(expr)
    assert pa.int32() == expr.infer_schema(schema)

    expr = f.last(lit(1.1))
    assert "LAST(1.1)" == str(expr)
    assert pa.float64() == expr.infer_schema(schema)

    expr = f.avg(col("a"))
    assert "AVG(a)" == str(expr)
    assert expr.infer_schema(schema) is None

    expr = f.sum(col("a"))
    assert "SUM(a)" == str(expr)
    assert expr.infer_schema(schema) is None

    expr = f.count(col("a"))
    assert "COUNT(a)" == str(expr)
    assert expr.infer_schema(schema) is None
