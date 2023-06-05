from triad import to_uuid

from fugue.collections.sql import StructuredRawSQL, TempTableName, transpile_sql


@transpile_sql.candidate(
    lambda sql, from_dialect, to_dialect: from_dialect == "x" and to_dialect == "y"
)
def _dummy_transpile(sql, from_dialect, to_dialect):
    return sql.lower()


def test_transpile():
    assert 'SELECT "x" FROM tb' == transpile_sql(
        "SELECT `x` FROM tb", from_dialect="spark", to_dialect="duckdb"
    )
    assert "SELECT `x` FROM tb" == transpile_sql(
        "SELECT `x` FROM tb", from_dialect="spark", to_dialect=None
    )
    assert "SELECT `x` FROM tb" == transpile_sql(
        "SELECT `x` FROM tb", from_dialect=None, to_dialect=None
    )
    assert "SELECT `x` FROM tb" == transpile_sql(
        "SELECT `x` FROM tb", from_dialect=None, to_dialect="duckdb"
    )
    assert 'select "x" from tb' == transpile_sql(
        'SELECT "x" FROM tb', from_dialect="x", to_dialect="y"
    )


def test_parse_sql():
    def parse(sql):
        parts = StructuredRawSQL.from_expr(sql)._statements
        return "".join([p[1] if not p[0] else "!" + p[1] + "!" for p in parts])

    t1 = TempTableName()
    t2 = TempTableName()
    assert parse("") == ""
    assert parse(f"{t1}") == f"!{t1.key}!"
    assert parse(f" {t1} ") == f" !{t1.key}! "
    assert parse(f"SELECT * FROM {t1}") == f"SELECT * FROM !{t1.key}!"
    assert (
        parse(f"SELECT * FROM {t1} NATURAL JOIN {t2}")
        == f"SELECT * FROM !{t1.key}! NATURAL JOIN !{t2.key}!"
    )
    assert (
        parse(f"SELECT {t1}.* FROM {t1} NATURAL JOIN {t2} WHERE {t2}.x<1")
        == f"SELECT !{t1.key}!.* FROM !{t1.key}! "
        f"NATURAL JOIN !{t2.key}! WHERE !{t2.key}!.x<1"
    )

    q = StructuredRawSQL.from_expr("SELECT * FROM abc", dialect="y")
    assert q.dialect == "y"


def test_structured():
    s = [
        (False, "SELECT * FROM"),
        (True, "tb1"),
        (False, "NATURAL JOIN"),
        (True, "tb2"),
    ]
    q = StructuredRawSQL(s, dialect="x")
    assert q.dialect == "x"
    assert q.construct() == "SELECT * FROM tb1 NATURAL JOIN tb2"
    assert q.construct(dialect="y") == "select * from tb1 natural join tb2"
    assert (
        q.construct({"tb1": "tt"}, dialect="y") == "select * from tt natural join tb2"
    )
    assert (
        q.construct(lambda x: x + "_", dialect="y")
        == "select * from tb1_ natural join tb2_"
    )


def test_uid():
    s1 = [(False, "SELECT * FROM"), (True, "tb1")]
    s2 = [(False, "SELECT * FROM"), (True, "tb1")]
    s3 = [(False, "SELECT * from"), (True, "tb1")]
    assert to_uuid(StructuredRawSQL(s1)) == to_uuid(StructuredRawSQL(s1))
    assert to_uuid(StructuredRawSQL(s1)) != to_uuid(StructuredRawSQL(s1, dialect="x"))
    assert to_uuid(StructuredRawSQL(s1)) == to_uuid(StructuredRawSQL(s2))
    assert to_uuid(StructuredRawSQL(s1)) != to_uuid(StructuredRawSQL(s3))
