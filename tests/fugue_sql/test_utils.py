from fugue_sql._utils import fill_sql_template


def test_fill_sql_template():
    data = {"a": 1, "b": "x"}
    assert "a=select " == fill_sql_template("a=select ", data)
    assert "a=select * from b like '{%'" == fill_sql_template(
        "a=select * from b like '{%'", data
    )
    assert 'a=select * from b like "{%"' == fill_sql_template(
        'a=select * from b like "{%"', data
    )
    assert "a=select * from b like \"{%'{%'\"" == fill_sql_template(
        "a=select * from b like \"{%'{%'\"", data
    )
    assert 'a=select * from b like "%}"' == fill_sql_template(
        'a=select * from b like "%}"', data
    )
    assert (
        "a=select * from b like \"{% for env in ('dev', 'prod') %}\""
        == fill_sql_template(
            "a=select * from b like \"{% for env in ('dev', 'prod') %}\"", data
        )
    )
    assert (
        "a=select * from b like \"{% for env in ('dev', 'prod') %}{% endfor %}\""
        == fill_sql_template(
            "a=select * from b like \"{% for env in ('dev', 'prod') %}{% endfor %}\"",
            data,
        )
    )

    assert "a=select * from b like '{%'" == fill_sql_template(
        "a=select * from b like '{%'", data
    )
    assert "a=select * from b like '%}'" == fill_sql_template(
        "a=select * from b like '%}'", data
    )
    assert (
        "a=select * from b like '{% for env in ('dev', 'prod') %}'"
        == fill_sql_template(
            "a=select * from b like '{% for env in ('dev', 'prod') %}'", data
        )
    )
    assert (
        "a=select * from b like '{% for env in ('dev', 'prod') %}{% endfor %}'"
        == fill_sql_template(
            "a=select * from b like '{% for env in ('dev', 'prod') %}{% endfor %}'",
            data,
        )
    )

    assert "1x1" == fill_sql_template("{{a}}{{b}}{{a}}", data)
    assert "" == fill_sql_template("", data)
    assert "%s" == fill_sql_template("%s", data)
    assert "%%s" == fill_sql_template("%%s", data)
    assert "1%%sx1" == fill_sql_template("{{a}}%%s{{b}}{{a}}", data)

    assert "1" == fill_sql_template("{{a}}", {"a": 1, "self": 2})
