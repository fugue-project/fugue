from fugue.column import SelectColumns, agg, col, function, lit, null
from fugue.column.expressions import _BinaryOpExpr, _is_agg
from fugue.column.functions import coalesce, first
from pytest import raises


def test_named_col():
    assert "a" == str(col("a"))
    assert "a" == str(col(col("a")))
    assert "ab AS xx" == str(col("ab").alias("xx"))
    assert "ab AS xx" == str(col("ab", "xx"))

    assert "DISTINCT ab" == str(col("ab").distinct())
    assert "ab AS xx" == str(col("ab").alias("xx"))

    assert "DISTINCT ab AS xx" == str(col("ab").alias("xx").distinct())
    assert "DISTINCT ab AS xx" == str(col("ab").distinct().alias("xx"))

    raises(NotImplementedError, lambda: col([1, 2]))


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

    assert "1 AS xx" == str(lit(1).alias("xx").distinct())
    assert "'ab' AS xx" == str(lit("ab").distinct().alias("xx"))

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
    assert "+('x',a)" == str("x" + col("a"))
    assert "+('x','a')" == str("x" + lit("a"))
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
    assert "f(+(x,z),y,1,1.1,FALSE,'t')" == str(expr)
    assert "DISTINCT f(+(x,z),y,1,1.1,FALSE,'t') AS x" == str(
        expr.distinct().alias("x")
    )
    assert "DISTINCT f(+(x,z),y,1,1.1,FALSE,'t') AS x" == str(
        expr.alias("x").distinct()
    )


def test_coalesce():
    expr = coalesce(col("x") + col("z"), col("y"), 1, 1.1, False, "t")
    assert "COALESCE(+(x,z),y,1,1.1,FALSE,'t')" == str(expr)
    assert "DISTINCT COALESCE(+(x,z),y,1,1.1,FALSE,'t') AS x" == str(
        expr.distinct().alias("x")
    )
    assert "DISTINCT COALESCE(+(x,z),y,1,1.1,FALSE,'t') AS x" == str(
        expr.alias("x").distinct()
    )


def test_is_agg():
    assert _is_agg(first(col("a")))
    assert _is_agg(first(col("a")).alias("x"))
    assert _is_agg(first(col("a")).distinct())
    assert _is_agg(first(col("a") + 1))
    assert _is_agg(first(col("a")) + 1)
    assert _is_agg((first(col("a")) < 1).alias("x"))
    assert _is_agg(col("a") * first(col("a")) + 1)

    assert not _is_agg(col("a"))
    assert not _is_agg(lit("a"))
    assert not _is_agg(col("a") + col("b"))
    assert not _is_agg(null())

    assert _is_agg(agg("x", 1))


def test_select_columns():
    # not all with names
    cols = SelectColumns(col("a"), lit(1, "b"), col("bb") + col("cc"), first(col("c")))
    raises(ValueError, lambda: cols.assert_all_with_names())

    # duplicated names
    cols = SelectColumns(col("a").alias("b"), lit(1, "b"))
    raises(ValueError, lambda: cols.assert_all_with_names())

    # with *, all cols must have alias
    cols = SelectColumns(col("*"), col("a"))
    raises(ValueError, lambda: cols.assert_all_with_names())

    # * can be used at most once
    raises(ValueError, lambda: SelectColumns(col("*"), col("*"), col("a").alias("p")))

    # * can't be used with aggregation
    raises(ValueError, lambda: SelectColumns(col("*"), first(col("a")).alias("x")))

    cols = SelectColumns(
        col("aa").alias("a"),
        lit(1, "b"),
        (col("bb") + col("cc")).alias("c"),
        first(col("c")).alias("d"),
    ).assert_all_with_names()
    assert not cols.simple
    assert 1 == len(cols.simple_cols)
    assert "aa AS a" == str(cols.simple_cols[0])
    assert cols.has_literals
    assert 1 == len(cols.literals)
    assert "1 AS b" == str(cols.literals[0])
    assert cols.has_agg
    assert 1 == len(cols.non_agg_funcs)
    assert "+(bb,cc) AS c" == str(cols.non_agg_funcs[0])
    assert 1 == len(cols.agg_funcs)
    assert "FIRST(c) AS d" == str(cols.agg_funcs[0])
    assert 2 == len(cols.group_keys)  # a, c
    assert "aa" == cols.group_keys[0].output_name
    assert "" == cols.group_keys[1].output_name
    assert isinstance(cols.group_keys[1], _BinaryOpExpr)

    cols = SelectColumns(col("a")).assert_no_wildcard()
    assert cols.simple
    assert not cols.has_literals
    assert not cols.has_agg
