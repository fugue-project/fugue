from fugue_sql._utils import fill_sql_template


def test_fill_sql_template():
    data = {"a": 1, "b": "x"}
    assert "a=select " == fill_sql_template("a=select ", data)
    assert "1x1" == fill_sql_template("{{a}}{{b}}{{a}}", data)
    assert "" == fill_sql_template("", data)
    assert "%s" == fill_sql_template("%s", data)
    assert "%%s" == fill_sql_template("%%s", data)
    assert "1%%sx1" == fill_sql_template("{{a}}%%s{{b}}{{a}}", data)
