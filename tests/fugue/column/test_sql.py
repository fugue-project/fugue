import fugue.column.functions as f
from fugue.column import SQLExpressionGenerator, col, function, lit, null
from fugue.column.expressions import SelectColumns
from pytest import raises


def test_basic():
    gen = SQLExpressionGenerator()
    assert "a" == gen.generate(col("a"))
    assert "a AS bc" == gen.generate(col("a").alias("bc"))
    assert "DISTINCT a" == gen.generate(col("a").distinct())
    assert "DISTINCT a AS bc" == gen.generate(col("a").alias("bc").distinct())

    assert "'a'" == gen.generate(lit("a"))
    assert "'a' AS bc" == gen.generate(lit("a").alias("bc"))


def test_select_exprs():
    gen = SQLExpressionGenerator()
    assert "(a+2)*3" == gen.generate((col("a") + 2) * 3)
    assert "(-a+2)*3" == gen.generate((-col("a") + 2) * 3)
    assert "(a*2)/3 AS x" == gen.generate(((col("a") * 2) / 3).alias("x"))
    assert "DISTINCT (a+2)*-3 AS x" == gen.generate(
        ((col("a") + 2) * -3).distinct().alias("x")
    )


def test_conditions():
    gen = SQLExpressionGenerator()
    assert "(a=-1) AND (b>=c)" == gen.generate(
        (col("a") == -1) & (col("b") >= col("c"))
    )
    assert "TRUE AND (b>=c)" == gen.generate(True & (col("b") >= col("c")))
    assert "TRUE AND NOT (b>=c)" == gen.generate(True & ~(col("b") >= col("c")))
    assert "TRUE OR (b>=c) IS NOT NULL" == gen.generate(
        True | (col("b") >= col("c")).not_null()
    )


def test_functions():
    gen = SQLExpressionGenerator()
    assert "COALESCE(a,b+c,(d+e)-1,NULL) IS NULL" == gen.generate(
        f.coalesce(
            col("a"), col("b") + col("c"), col("d") + col("e") - 1, null()
        ).is_null()
    )
    assert (
        "MY(MIN(x),MAX(y+1),AVG(z),2,aa=FIRST(a),bb=LAST('b'),cc=COUNT(DISTINCT *)) AS x"
        == gen.generate(
            function(
                "MY",
                f.min(col("x")),
                f.max(col("y") + 1),
                f.avg(col("z")),
                2,
                aa=f.first(col("a")),
                bb=f.last(lit("b")),
                cc=f.count(col("*").distinct()),
            ).alias("x")
        )
    )

    def dummy(expr):
        yield "DUMMY"

    gen.add_func_handler("MY", dummy)
    assert "DISTINCT DUMMY AS x" == gen.generate(
        function("MY", 2, 3).distinct().alias("x")
    )


def test_where():
    gen = SQLExpressionGenerator()
    assert "SELECT * FROM x WHERE (a<5) AND b IS NULL" == gen.where(
        (col("a") < 5) & col("b").is_null(), "x"
    )
    assert "SELECT * FROM x WHERE a<5" == gen.where((col("a") < 5).alias("x"), "x")

    raises(ValueError, lambda: gen.where(f.max(col("a")), "x"))


def test_select():
    gen = SQLExpressionGenerator()

    # no aggregation
    cols = SelectColumns(col("*"))
    assert "SELECT * FROM x" == gen.select(cols, "x")

    cols = SelectColumns(col("a"), lit(1).alias("b"), (col("b") + col("c")).alias("x"))
    where = (col("a") > 5).alias("aa")
    assert "SELECT a, 1 AS b, b+c AS x FROM t WHERE a>5" == gen.select(
        cols, "t", where=where
    )

    # aggregation without literals
    cols = SelectColumns(f.max(col("c")).alias("c"), col("a", "aa"), col("b"))
    assert "SELECT MAX(c) AS c, a AS aa, b FROM t GROUP BY a, b" == gen.select(
        cols, "t"
    )

    where = col("a") < 10
    having = (col("aa") > 5).alias("aaa")
    assert (
        "SELECT MAX(c) AS c, a AS aa, b FROM t WHERE a<10 GROUP BY a, b HAVING aa>5"
        == gen.select(cols, "t", where=where, having=having)
    )

    # a is not in selected columns
    having = col("a") > 5
    raises(ValueError, lambda: gen.select(cols, "t", having=having))

    cols = SelectColumns(
        f.min(col("c") + 1).alias("c"), f.avg(col("d") + col("e")).alias("d")
    )
    assert "SELECT MIN(c+1) AS c, AVG(d+e) AS d FROM t" == gen.select(cols, "t")

    # aggregation with literals
    cols = SelectColumns(
        lit(1, "k"), f.max(col("c")).alias("c"), lit(2, "j"), col("a", "aa"), col("b")
    )
    assert (
        "SELECT 1 AS k, c, 2 AS j, aa, b FROM (SELECT MAX(c) AS c, a AS aa, b FROM t GROUP BY a, b)"
        == gen.select(cols, "t")
    )

    cols = SelectColumns(lit(1, "k"), f.max(col("c")).alias("c"), lit(2, "j"))
    assert "SELECT 1 AS k, c, 2 AS j FROM (SELECT MAX(c) AS c FROM t)" == gen.select(
        cols, "t"
    )
