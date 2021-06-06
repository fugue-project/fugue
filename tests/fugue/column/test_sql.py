from fugue.column import SQLExpressionGenerator, col, lit, null, function
import fugue.column.functions as f


def test_basic():
    gen = SQLExpressionGenerator()
    assert "a" == gen.generate(col("a"))
    assert "a AS bc" == gen.generate(col("a").alias("bc"))
    assert "DISTINCT a" == gen.generate(col("a").distinct())
    assert "DISTINCT a AS bc" == gen.generate(col("a").alias("bc").distinct())

    assert "'a'" == gen.generate(lit("a"))
    assert "'a' AS bc" == gen.generate(lit("a").alias("bc"))


def test_select():
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
    assert "MY(MIN(x),MAX(y+1),AVG(z),2,aa=FIRST(a),bb=LAST('b')) AS x" == gen.generate(
        function(
            "MY",
            f.min(col("x")),
            f.max(col("y") + 1),
            f.avg(col("z")),
            2,
            aa=f.first(col("a")),
            bb=f.last(lit("b")),
        ).alias("x")
    )

    def dummy(expr):
        yield "DUMMY"

    gen.add_func_handler("MY", dummy)
    assert "DISTINCT DUMMY AS x" == gen.generate(
        function("MY", 2, 3).distinct().alias("x")
    )