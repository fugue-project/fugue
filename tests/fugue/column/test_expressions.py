import pyarrow as pa
from fugue.column import col, function, lit, null
from fugue.column.expressions import _get_column_mentions
from fugue.column.functions import coalesce
from pytest import raises
from triad import Schema, to_uuid


def test_named_col():
    assert "*" == str(col("*"))
    assert col("*").wildcard
    assert "" == col("*").infer_alias().output_name
    raises(ValueError, lambda: col("*").alias("x"))
    raises(ValueError, lambda: col("*").cast("long"))

    assert "a" == str(col("a"))
    assert not col("a").wildcard
    assert "a" == str(col(col("a")))
    assert "ab AS xx" == str(col("ab").alias("xx"))
    assert "ab AS xx" == str(col("ab", "xx").cast(None))
    assert "CAST(ab AS long) AS xx" == str(col("ab", "xx").cast("long"))

    assert "ab AS xx" == str(col("ab").alias("xx"))

    assert "ab AS xx" == str(col("ab").alias("xx"))
    assert "CAST(ab AS long) AS xx" == str(col("ab").alias("xx").cast(int))

    raises(NotImplementedError, lambda: col([1, 2]))

    assert to_uuid(col("a")) != to_uuid(col("b"))
    assert to_uuid(col("a")) != to_uuid(col("a").alias("v"))
    assert to_uuid(col("a")) != to_uuid(col("a").cast(int))
    assert to_uuid(col("a").cast(int).alias("v")) == to_uuid(
        col("a").alias("v").cast(int)
    )

    assert "" == col("a").infer_alias().as_name
    assert "a" == str(col("a").infer_alias())
    assert "a" == col("a").cast(int).infer_alias().as_name
    c = col("a").cast(int).infer_alias()
    assert "CAST(a AS long) AS a" == str(c)
    c = col("a").cast(int).alias("x").infer_alias()
    assert "CAST(a AS long) AS x" == str(c)


def test_lit_col():
    assert "NULL" == str(lit(None))
    assert "TRUE" == str(null().is_null())
    assert "FALSE" == str(null().not_null())

    assert "'a'" == str(lit("a"))
    assert "'a\"\\'\\\\'" == str(lit("a\"'\\"))
    assert "'a' AS x" == str(lit("a", "x"))
    assert "TRUE" == str(lit("a").not_null())
    assert "FALSE" == str(lit("a").is_null())

    assert "1.1" == str(lit(1.1))
    assert "11" == str(lit(11))
    assert "TRUE" == str(lit(True))
    assert "FALSE" == str(lit(False))

    assert "1 AS xx" == str(lit(1).alias("xx"))
    assert "'ab' AS xx" == str(lit("ab").alias("xx"))

    raises(NotImplementedError, lambda: lit([1, 2]))

    assert to_uuid(lit("a")) != to_uuid(col("a"))
    assert to_uuid(lit(1)) != to_uuid(lit("1"))
    assert to_uuid(null()) == to_uuid(null())
    assert to_uuid(null()) != to_uuid(lit(1))
    assert to_uuid(lit("a")) != to_uuid(lit("a").alias("v"))
    assert to_uuid(lit("a")) != to_uuid(lit("a").cast(int))
    assert to_uuid(lit("a").cast(int).alias("v")) == to_uuid(
        lit("a").alias("v").cast(int)
    )


def test_unary_op():
    assert "-(a)" == str(-col("a"))
    assert "a" == (-col("a")).infer_alias().output_name
    assert "a" == str(+col("a"))
    assert "~(a)" == str(~col("a"))
    assert "IS_NULL(a)" == str(col("a").is_null())
    assert "NOT_NULL(a)" == str(col("a").not_null())

    assert "NOT_NULL(a) AS xx" == str(col("a").not_null().alias("xx"))
    assert "NOT_NULL(a)" == str(col("a").not_null())
    assert "NOT_NULL(a) AS xx" == str(col("a").not_null().alias("xx"))

    assert "a" == col("a").not_null().infer_alias().output_name
    assert "NOT_NULL(a) AS a" == str(col("a").not_null().infer_alias())

    assert to_uuid(col("a").not_null()) == to_uuid(col("a").not_null())
    assert to_uuid(col("a").not_null()) != to_uuid(col("a").is_null())


def test_binary_op():
    assert "+(ab,1)" == str(col("ab") + 1)
    assert "+(ab,x)" == str(col("ab") + col("x"))
    assert "+('x',a)" == str("x" + col("a"))
    assert "+('x','a')" == str("x" + lit("a"))
    assert "-(a,1)" == str(col("a") - 1)
    assert "-(1.1,a)" == str(1.1 - col("a"))
    assert "*(a,1)" == str(col("a") * 1)
    assert "*(1.1,a)" == str(1.1 * col("a"))
    assert "/(a,1)" == str(col("a") / 1)
    assert "/(1.1,a)" == str(1.1 / col("a"))

    assert "+(ab,1)" == str((col("ab") + 1))
    assert "+(ab,1) AS xx" == str((col("ab") + 1).alias("xx"))

    assert "+(ab,1) AS xx" == str((col("ab") + 1).alias("xx"))

    assert "&(a,TRUE)" == str(col("a") & True)
    assert "&(TRUE,a)" == str(True & col("a"))
    assert "&(a,FALSE)" == str(col("a") & False)
    assert "&(FALSE,a)" == str(False & col("a"))

    assert "|(a,TRUE)" == str(col("a") | True)
    assert "|(TRUE,a)" == str(True | col("a"))
    assert "|(a,FALSE)" == str(col("a") | False)
    assert "|(FALSE,a)" == str(False | col("a"))

    assert "<(a,1)" == str(col("a") < 1)
    assert "<(a,b)" == str(col("a") < col("b"))
    assert ">(a,1.1)" == str(1.1 < col("a"))
    assert "<(1.1,a)" == str(lit(1.1) < col("a"))
    assert "<=(a,1)" == str(col("a") <= 1)
    assert ">=(a,1.1)" == str(1.1 <= col("a"))
    assert ">(a,1)" == str(col("a") > 1)
    assert "<(a,1.1)" == str(1.1 > col("a"))
    assert ">=(a,1)" == str(col("a") >= 1)
    assert "<=(a,1.1)" == str(1.1 >= col("a"))

    assert "==(a,1)" == str(col("a") == 1)
    assert "==(a,1.1)" == str(1.1 == col("a"))
    assert "!=(a,1)" == str(col("a") != 1)
    assert "!=(a,1.1)" == str(1.1 != col("a"))


def test_comb():
    assert "-(+(a,*(10,b)),/(c,d))" == str(
        (col("a") + 10 * col("b")) - col("c") / col("d")
    )
    assert "|(==(a,1.1),&(&(b,~(c)),TRUE))" == str(
        (1.1 == col("a")) | col("b") & ~col("c") & True
    )


def test_function():
    expr = function("f", col("x") + col("z"), col("y"), 1, 1.1, False, "t")
    assert "f(+(x,z),y,1,1.1,FALSE,'t')" == str(expr)
    assert "f(+(x,z),y,1,1.1,FALSE,'t') AS x" == str(expr.alias("x"))


def test_coalesce():
    expr = coalesce(col("x") + col("z"), col("y"), 1, 1.1, False, "t")
    assert "COALESCE(+(x,z),y,1,1.1,FALSE,'t')" == str(expr)
    assert "COALESCE(+(x,z),y,1,1.1,FALSE,'t') AS x" == str(expr.alias("x"))


def test_get_column_mentions():
    expr = (col("a") + col("b")) * function("x", col("b"), a=col("c"), b=lit(1))
    assert set(["a", "b", "c"]) == set(_get_column_mentions(expr))


def test_schema_inference():
    schema = Schema("a:int,b:str,c:bool,d:double")
    assert pa.int32() == col("a").infer_type(schema)
    assert pa.int32() == (-col("a")).infer_type(schema)
    assert pa.int64() == (-col("a")).cast(int).infer_type(schema)
    assert pa.int64() == (-col("a").cast(int)).infer_type(schema)
    assert pa.string() == col("b").infer_type(schema)
    assert (-col("b")).infer_type(schema) is None
    assert (~col("b")).infer_type(schema) is None
    assert pa.bool_() == col("c").infer_type(schema)
    assert pa.bool_() == (~col("c")).alias("x").infer_type(schema)
    assert pa.float64() == col("d").infer_type(schema)
    assert pa.float64() == (-col("d").alias("x")).infer_type(schema)
    assert col("x").infer_type(schema) is None
    assert pa.string() == col("x").cast(str).infer_type(schema)
    assert col("*").infer_type(schema) is None

    assert pa.bool_() == (col("a") < col("d")).infer_type(schema)
    assert pa.bool_() == (col("a") > col("d")).infer_type(schema)
    assert pa.bool_() == (col("a") <= col("d")).infer_type(schema)
    assert pa.bool_() == (col("a") >= col("d")).infer_type(schema)
    assert pa.bool_() == (col("a") == col("d")).infer_type(schema)
    assert pa.bool_() == (col("a") != col("d")).infer_type(schema)
    assert pa.bool_() == (~(col("a") != col("d"))).infer_type(schema)
    assert pa.int64() == (~(col("a") != col("d"))).cast(int).infer_type(schema)

    assert (col("a") - col("d")).infer_type(schema) is None

    assert pa.int64() == lit(1).infer_type(schema)
    assert pa.string() == lit("a").infer_type(schema)
    assert pa.bool_() == lit(False).infer_type(schema)
    assert pa.string() == lit(False).cast(str).infer_type(schema)
    assert pa.float64() == lit(2.2).infer_type(schema)
    assert null().infer_type(schema) is None
    assert pa.string() == null().cast(str).infer_type(schema)

    assert function("a", col("a").cast("int")).infer_type(schema) is None
    assert pa.string() == function("a", col("a").cast("int")).cast(str).infer_type(
        schema
    )
