from fugue_sql.exceptions import FugueSQLSyntaxError
from fugue_sql._parse import FugueSQL, _detect_case_issue
from pytest import raises
from tests.fugue_sql.utils import (
    bad_single_syntax,
    bad_syntax,
    good_single_syntax,
    good_syntax,
)


def test_detect_case_issue():
    assert not _detect_case_issue("", 0.9)
    assert not _detect_case_issue("--*&^^", 0.9)
    assert _detect_case_issue("abc", 0.9)
    assert _detect_case_issue("absdfAsdfsdc", 0.9)
    assert not _detect_case_issue("ABC", 0.9)
    assert _detect_case_issue("ABCa", 0.1)


def test_case_config_error():
    # with a hint of config issue
    bad_syntax("a = select a where a==10", ignore_case=False, match=r".*ignore_case.*")
    # without a hint of config issue
    bad_syntax("a = SELECT a WHERr a==10", ignore_case=False, match=r"^no viable.*")


def test_assign_syntax():
    # simple assign not enabled
    bad_single_syntax("a = select a where a==10", ignore_case=True, simple_assign=False)
    # when simple assign enabled, comparison using = is still valid
    good_single_syntax("a = select a where a=10", ignore_case=True, simple_assign=True)
    good_single_syntax("a = select a where a==10", ignore_case=True, simple_assign=True)
    # multiple expression test
    good_syntax(
        """
        a = select a where a==10
        b=select x""",
        ignore_case=True,
        simple_assign=True,
    )


def test_partition_syntax():
    good_single_syntax(
        "a = transform",
        ["", "hash", "even", "rand"],
        "  prepartition 100 using a",
        ignore_case=True,
        simple_assign=True,
    )
    good_single_syntax(
        "a = transform prepartition 100 ",
        ["by a,b,c"],
        ["presort a asc, b desc"],
        " using a",
        ignore_case=True,
        simple_assign=True,
    )
    good_single_syntax(
        "a = transform prepartition by a",
        ["presort a asc, b desc"],
        " using a",
        ignore_case=True,
        simple_assign=True,
    )


def test_persist_broadcast_checkpoint_syntax():
    good_single_syntax(
        "a = select a",
        ["", "lazy"],
        [
            "",
            "PERSIst",
            'persist (level="a12")',
            "weak checkpoint (level='a.b.c')",
            "checkpoint",
            "deterministic checkpoint 'x'",
            "deterministic checkpoint 'x' prepartition by a single (level=1)",
        ],
        ["BROADCAst"],
        ignore_case=True,
        simple_assign=True,
    )
    good_single_syntax(
        "a = select a",
        ["", "checkpoint", "deterministic checkpoint 'x'"],
        ["BROADCAst"],
        ignore_case=True,
        simple_assign=True,
    )


def test_select_syntax():
    # TODO: add a??
    bad_single_syntax("SELECT a FROM", ignore_case=False, ansi_sql=True)
    good_single_syntax(["a:=", ""], "SELECT a", ["FROM sx"], ignore_case=False)
    bad_single_syntax("select a", ["from sx"], ignore_case=False)
    good_single_syntax(
        "select a", ["from sx"], ["where a=10 and a==10"], ignore_case=True
    )

    # nested
    good_single_syntax("SELECT a FROM (TRANSFORM USING x)", ["AS t"], ignore_case=False)
    good_single_syntax(
        """
    SELECT a FROM
        (TRANSFORM USING x) AS x INNER JOIN (TRANSFORM USING x) AS y
        ON x.a = b.a
    """,
        ignore_case=False,
    )

    # no from
    good_syntax("select *", ignore_case=True, simple_assign=True)
    good_syntax("select * where a=100", ignore_case=True, simple_assign=True)


def test_schema_syntax():
    good_syntax(
        [
            "*",
            "a:int",
            "a:int,*",
            "*,a:int",
            "a:int,*,b:int",
            "*-a,b",
            "*~c",
            "*+c:str,d:int",
            "*,k:str+a:str,b:str-c~x",
        ],
        ignore_case=False,
        rule="fugueWildSchema",
    )


def test_transform_syntax():
    # test data sources and nested
    good_single_syntax(
        "a = transform ",
        ["", "a,b", "(transform using x)"],
        "using x",
        ignore_case=True,
        simple_assign=True,
    )
    # extensions
    good_single_syntax(
        "transform",
        ["prepartition by x"],
        " using ",
        ["x", "x.Y.z"],
        ignore_case=True,
        simple_assign=True,
    )
    # params
    good_single_syntax(
        "a= transform using x",
        ["", 'params a:10,b="20"'],
        ignore_case=True,
        simple_assign=True,
    )
    # params
    good_single_syntax(
        "a= transform using x",
        ["params"],
        [
            "{a:10,b:{x:10,y:true,z:false,w:null}}",
            "(a=10,b=True)",
            "(a:10,b:{x:10,y:true,z:false,w:null})",
        ],
        ignore_case=True,
        simple_assign=True,
    )
    # schemas
    good_single_syntax(
        "a= transform using x",
        ["", "schema a : int,b:{x:int,y:[str]},c:[int]"],
        ignore_case=True,
        simple_assign=True,
    )
    # schemas bad list
    bad_single_syntax(
        "a= transform using x schema a:int,c:[int,str]",
        ignore_case=True,
        simple_assign=True,
    )


def test_process_create_syntax():
    good_single_syntax(
        "a = create using a.b.c(a=1,b=2) schema a:int",
        ignore_case=True,
        simple_assign=True,
    )
    good_single_syntax(
        "process",
        ["", "a,b", "(create using x)"],
        ["prepartition by x"],
        "using a.b.c(a=1,b=2,) schema a:int",
        ignore_case=True,
        simple_assign=True,
    )


def test_output_syntax():
    good_single_syntax(
        "output",
        ["", "a,b", "(create using x)"],
        ["prepartition by x"],
        "using a.b.c",
        ["(a=1,b=2,)"],
        ignore_case=True,
        simple_assign=True,
    )
    good_single_syntax(
        "print",
        ["100 rows", "from", "a,b", "(create using x)"],
        ["rowcount"],
        ["title 'abc'"],
        ignore_case=True,
        simple_assign=True,
    )


def test_zip_syntax():
    good_single_syntax(
        "a = zip a,b",
        ["", "cross", "inner", "left outer", "right  outer", "full outer"],
        ["by x"],
        ["presort y desc"],
        ignore_case=True,
        simple_assign=True,
    )


def test_load_save_syntax():
    good_single_syntax(
        "load",
        ["", "csv", "parquet", "json"],
        '"x"',
        ["(a=1)"],
        ["", "columns a,b", "columns a:int,b:int"],
        ignore_case=True,
        simple_assign=True,
    )
    good_single_syntax(
        "save",
        ["a"],
        ["to", "overwrite", "append"],
        '"x"',
        ignore_case=True,
        simple_assign=True,
    )
    good_single_syntax(
        "save to",
        ["single"],
        ["", "csv", "parquet", "json"],
        '"x"',
        ignore_case=True,
        simple_assign=True,
    )
    good_single_syntax(
        "save to 'x'", ["(header=True)"], ignore_case=True, simple_assign=True
    )
