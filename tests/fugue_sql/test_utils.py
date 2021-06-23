from fugue_sql._utils import fill_sql_template


def test_fill_sql_template():
    data = {"a": 1, "b": "x"}
    assert "select * from tbl where a = 1 and b = 'x'" == fill_sql_template(
        "select * from tbl where a = {{a}} and b = '{{b}}'", data
    )
    assert """select * from tbl where a = 1 and b = "x" """ == fill_sql_template(
        """select * from tbl where a = {{a}} and b = "{{b}}" """, data
    )

    assert """select * where b="%x" """ == fill_sql_template(
        """select * where b="%{{b}}" """, data
    )
    assert """select * where b="x%" """ == fill_sql_template(
        """select * where b="{{b}}%" """, data
    )

    assert """select * b like "{}%{}" """ == fill_sql_template(
        """select * b like "{}%{}" """, data
    )
    assert """select * b like '%}' """ == fill_sql_template(
        """select * b like '%}' """, data
    )
    assert """select * where b="%x" """ == fill_sql_template(
        """select * where b="%{{b}}" """, data
    )
    assert """select * where b="x%" """ == fill_sql_template(
        """select * where b="{{b}}%" """, data
    )

    assert "a=select " == fill_sql_template("a=select ", data)

    # try single quotes for finding json patterns
    assert "a=select * from b like '{%'" == fill_sql_template(
        "a=select * from b like '{%'", data
    )
    assert "a=select * from b like '%}'" == fill_sql_template(
        "a=select * from b like '%}'", data
    )

    # try double quotes for finding json patterns
    assert 'a=select * from b like "%}"' == fill_sql_template(
        'a=select * from b like "%}"', data
    )
    assert 'a=select * from b like "{%"' == fill_sql_template(
        'a=select * from b like "{%"', data
    )

    assert "1x1" == fill_sql_template("{{a}}{{b}}{{a}}", data)
    assert "" == fill_sql_template("", data)
    assert "%s" == fill_sql_template("%s", data)
    assert "%%s" == fill_sql_template("%%s", data)
    assert "1%%sx1" == fill_sql_template("{{a}}%%s{{b}}{{a}}", data)
    assert "1" == fill_sql_template("{{a}}", {"a": 1, "self": 2})


def test_fill_sql_template_array():
    data = {"a": [0, 1, 2]}
    assert """select * from tbl where a in ('0','1','2')""" == fill_sql_template(
        """select * from tbl where a in (
            {%- for i in a -%}
                {%- if loop.index0 < loop.length - 1 -%}'{{-i-}}',
                {%- else -%}'{{-i-}}'
                {%- endif -%}
            {%- endfor -%}
            )""",
        data,
    )

    def upper(word):
        return word.upper()

    data = {"a": ["a", "b", "c"]}
    assert """select * from tbl where a in ('A','B','C')""" == fill_sql_template(
        """select * from tbl where a in (
            {%- for i in a -%}
                {%- if loop.index0 < loop.length - 1 -%}'{{-i|upper-}}',
                {%- else -%}'{{-i|upper-}}'
                {%- endif -%}
            {%- endfor -%}
            )""",
        data,
    )
