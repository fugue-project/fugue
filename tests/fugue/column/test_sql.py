import fugue.column.functions as f
from fugue.column import (
    SelectColumns,
    SQLExpressionGenerator,
    SQLExpressionHiddenCastGenerator,
    col,
    function,
    lit,
    null,
)
from fugue.column.expressions import _BinaryOpExpr
from pytest import raises
from triad import Schema


def test_select_columns():
    # not all with names
    cols = SelectColumns(
        col("a"), lit(1, "b"), col("bb") + col("cc"), f.first(col("c"))
    )
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
    raises(ValueError, lambda: SelectColumns(col("*"), f.first(col("a")).alias("x")))

    cols = SelectColumns(
        col("aa").alias("a"),
        lit(1, "b"),
        (col("bb") + col("cc")).alias("c"),
        f.first(col("c")).alias("d"),
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

    cols = SelectColumns(col("x"), col("*"), col("y") + col("z"))
    cols = cols.replace_wildcard(Schema("a:int,b:int"))
    assert "x" == str(cols.all_cols[0])


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
        f.min(col("c") + 1).alias("c"),
        f.avg(col("d") + col("e")).cast(int).alias("d"),
    )
    assert "SELECT MIN(c+1) AS c, CAST(AVG(d+e) AS long) AS d FROM t" == gen.select(
        cols, "t"
    )

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


def test_correct_select_schema():
    schema = Schema("a:double,b:str")
    gen = SQLExpressionGenerator()

    sc = SelectColumns(col("*"), col("c"))
    output = Schema("a:int,b:int,c:str")
    c = gen.correct_select_schema(schema, sc, output)
    assert c == "a:double,b:str,c:str"

    sc = SelectColumns(f.count(col("*")).alias("t"), col("c").alias("a"))
    output = Schema("t:int,a:str")
    c = gen.correct_select_schema(schema, sc, output)
    assert c == "t:int,a:str"

    sc = SelectColumns((col("a") + col("b")).cast(str).alias("a"), lit(1, "c"))
    output = Schema("a:int,c:str")
    c = gen.correct_select_schema(schema, sc, output)
    assert c == "a:str,c:long"


def test_no_cast():
    gen = SQLExpressionHiddenCastGenerator()
    cols = SelectColumns(
        f.max(col("c")).cast("long").alias("c"), col("a", "aa"), col("b")
    )
    assert "SELECT MAX(c) AS c, a AS aa, b FROM t GROUP BY a, b" == gen.select(
        cols, "t"
    )
