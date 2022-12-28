from fugue._utils.sql import get_temp_tb_name, parse_sql


def test_parse_sql():
    def parse(sql):
        parts = parse_sql(sql)
        return "".join([p[1] if not p[0] else "!" + p[1] + "!" for p in parts])

    t1 = get_temp_tb_name()
    t2 = get_temp_tb_name()
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
