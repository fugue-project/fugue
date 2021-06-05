from fugue.column import coalesce, col, function, lit, null
from pytest import raises


def test_named_col():
    assert "a" == str(col("a"))
    assert "a" == str(col(col("a")))
    assert "ab AS xx" == str(col("ab").alias("xx"))

    assert "DISTINCT ab" == str(col("ab").distinct())
    assert "ab AS xx" == str(col("ab").alias("xx"))

    assert "DISTINCT ab AS xx" == str(col("ab").alias("xx").distinct())
    assert "DISTINCT ab AS xx" == str(col("ab").distinct().alias("xx"))

    raises(NotImplementedError, lambda: col([1, 2]))


def test_lit_col():
    assert "NULL" == str(lit(None))
    assert "TRUE" == str(null().is_null())
    assert "FALSE" == str(null().not_null())

    assert '"a"' == str(lit("a"))
    assert "TRUE" == str(lit("a").not_null())
    assert "FALSE" == str(lit("a").is_null())

    assert "1.1" == str(lit(1.1))
    assert "11" == str(lit(11))
    assert "TRUE" == str(lit(True))
    assert "FALSE" == str(lit(False))

    assert "1 AS xx" == str(lit(1).alias("xx").distinct())
    assert '"ab" AS xx' == str(lit("ab").distinct().alias("xx"))

    raises(NotImplementedError, lambda: lit([1, 2]))


def test_unary_op():
    assert "-(a)" == str(-col("a"))
    assert "a" == str(+col("a"))
    assert "~(a)" == str(~col("a"))
    assert "IS_NULL(a)" == str(col("a").is_null())
    assert "NOT_NULL(a)" == str(col("a").not_null())

    assert "NOT_NULL(a) AS xx" == str(col("a").not_null().alias("xx"))
    assert "DISTINCT NOT_NULL(a)" == str(col("a").not_null().distinct())
    assert "DISTINCT NOT_NULL(a) AS xx" == str(
        col("a").not_null().alias("xx").distinct()
    )
    assert "DISTINCT NOT_NULL(a) AS xx" == str(
        col("a").not_null().distinct().alias("xx")
    )


def test_binary_op():
    assert "+(ab,1)" == str(col("ab") + 1)
    assert "+(ab,x)" == str(col("ab") + col("x"))
    assert '+("x",a)' == str("x" + col("a"))
    assert '+("x","a")' == str("x" + lit("a"))
    assert "-(a,1)" == str(col("a") - 1)
    assert "-(1.1,a)" == str(1.1 - col("a"))
    assert "*(a,1)" == str(col("a") * 1)
    assert "*(1.1,a)" == str(1.1 * col("a"))
    assert "/(a,1)" == str(col("a") / 1)
    assert "/(1.1,a)" == str(1.1 / col("a"))

    assert "DISTINCT +(ab,1)" == str((col("ab") + 1).distinct())
    assert "+(ab,1) AS xx" == str((col("ab") + 1).alias("xx"))

    assert "DISTINCT +(ab,1) AS xx" == str((col("ab") + 1).alias("xx").distinct())
    assert "DISTINCT +(ab,1) AS xx" == str((col("ab") + 1).distinct().alias("xx"))

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
    assert 'f(+(x,z),y,1,1.1,FALSE,"t")' == str(expr)
    assert 'DISTINCT f(+(x,z),y,1,1.1,FALSE,"t") AS x' == str(
        expr.distinct().alias("x")
    )
    assert 'DISTINCT f(+(x,z),y,1,1.1,FALSE,"t") AS x' == str(
        expr.alias("x").distinct()
    )


def test_coalesce():
    expr = coalesce(col("x") + col("z"), col("y"), 1, 1.1, False, "t")
    assert 'COALESCE(+(x,z),y,1,1.1,FALSE,"t")' == str(expr)
    assert 'DISTINCT COALESCE(+(x,z),y,1,1.1,FALSE,"t") AS x' == str(
        expr.distinct().alias("x")
    )
    assert 'DISTINCT COALESCE(+(x,z),y,1,1.1,FALSE,"t") AS x' == str(
        expr.alias("x").distinct()
    )
