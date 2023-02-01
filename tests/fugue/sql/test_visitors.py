import json

from fugue_sql_antlr import FugueSQLParser
from pytest import raises
from triad.collections.schema import Schema

from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.sql._visitors import _Extensions, _VisitorBase
from fugue.exceptions import FugueSQLSyntaxError
from fugue.workflow.workflow import FugueWorkflow

_PARSE_MODE = "auto"


def test_json():
    def assert_eq(expr, expected):
        sql = FugueSQLParser(
            expr,
            "fugueJsonValue",
            ignore_case=True,
            parse_mode=_PARSE_MODE,
        )
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        if expected is None:
            assert obj is None
        else:
            assert expected == obj

    # int
    assert_eq("1", 1)
    # str
    assert_eq("'1'", "1")
    assert_eq('"1"', "1")
    assert_eq('"\\"x"', '"x')
    assert_eq("'\\'x'", "'x")
    # bool
    assert_eq("True", True)
    assert_eq("falsE", False)
    assert_eq("falsE", False)
    # null
    assert_eq("Null", None)
    # dict
    assert_eq("{}", dict())
    assert_eq("{a:1\n}", {"a": 1})
    assert_eq("{aA:1.5,}", {"aA": 1.5})
    assert_eq("{'a':.5}", {"a": 0.5})
    assert_eq("('a':.5)", {"a": 0.5})
    assert_eq("(a:1,b=2)", {"a": 1, "b": 2})
    # array
    assert_eq("[]", list())
    assert_eq("[\n1,'a'\n]", [1, "a"])
    assert_eq("[\n1,'a'\n,]", [1, "a"])
    # python arg like
    assert_eq("(a=1,b='x',c=True,d={x:3},)", dict(a=1, b="x", c=True, d=dict(x=3)))


def test_extension():
    def assert_eq(expr, expected=None):
        sql = FugueSQLParser(
            expr,
            "fugueExtension",
            ignore_case=True,
            parse_mode=_PARSE_MODE,
        )
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        assert expected == obj

    assert_eq("abc", "abc")
    assert_eq("abc . def", "abc.def")
    assert_eq("abc . def. ` ` ", "abc.def.` `")
    assert_eq(" x :abc . def. ` ` ", ("x", "abc.def.` `"))
    assert_eq("`x:y ` : abc . def. ` ` ", ("x:y ", "abc.def.` `"))


def test_schema():
    def assert_eq(expr, expected=None):
        sql = FugueSQLParser(
            expr,
            "fugueSchema",
            ignore_case=True,
            parse_mode=_PARSE_MODE,
        )
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        if expected is None:
            expected = expr
        assert Schema(expected) == obj

    assert_eq("a:int,b:str")
    assert_eq("a:int,b:string", "a:int,b:str")
    assert_eq("a:int,B:str,c:[int]")
    assert_eq("a:int,b:str,C:{A:int,b:str}")
    assert_eq("a:int,\nb:str,C:[{A:int,b:str}]")
    assert_eq("a:<int,str>,b:<int,[str]>,c:[<int,{b:str}>]")
    assert_eq(" ` * ` : int , `` : string ", "` * `:int,``:str")


def test_wild_schema():
    def assert_eq(expr, expected=None):
        sql = FugueSQLParser(
            expr,
            "fugueWildSchema",
            ignore_case=True,
            parse_mode=_PARSE_MODE,
        )
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        if expected is None:
            expected = expr
        assert expected == obj

    assert_eq("a:int,b:str")
    assert_eq("a:int, b: string", "a:int,b:str")
    assert_eq("a:int32, *", "a:int,*")
    assert_eq("*  ", "*")
    assert_eq("*,a:int32", "*,a:int")
    assert_eq("a:int32, *, \nb:string", "a:int,*,b:str")
    # transformations
    assert_eq(
        "a:int32, *, B:string +x:str, k:int\n-Y, z -t~ w , x",
        "a:int,*,B:str+x:str,k:int-Y,z-t~w,x",
    )
    # special chars
    assert_eq(
        " ` * ` :int32, *, `` : string +x:str, k:int\n-Y, z -t~ w , x",
        "` * `:int,*,``:str+x:str,k:int-Y,z-t~w,x",
    )


def test_pre_partition():
    def assert_eq(expr, expected):
        sql = FugueSQLParser(
            expr,
            "fuguePrepartition",
            ignore_case=True,
            parse_mode=_PARSE_MODE,
        )
        v = _VisitorBase(sql)
        obj = json.dumps(v.visit(sql.tree).jsondict)
        assert json.dumps(expected.jsondict) == obj

    assert_eq("prepartition 100", PartitionSpec(num=100))
    assert_eq(
        "prepartition ROWCOUNT*\n3+ (12.5-CONCUrrency/2)",
        PartitionSpec(num="ROWCOUNT*3+(12.5-CONCURRENCY/2)"),
    )
    assert_eq("prepartition by a, b", PartitionSpec(by=["a", "b"]))
    assert_eq(
        "HASH PrePARTITION 100 BY a,b",
        PartitionSpec(algo="hash", num=100, by=["a", "b"]),
    )
    assert_eq(
        "EVEN prepartition 100 BY a,b",
        PartitionSpec(algo="even", num=100, by=["a", "b"]),
    )
    assert_eq(
        "rand prepartition 100 BY a,b",
        PartitionSpec(algo="rand", num=100, by=["a", "b"]),
    )
    assert_eq(
        "prepartition 100 presort a,\nb\ndesc",
        PartitionSpec(num=100, presort="a asc, b desc"),
    )


def test_params():
    def assert_eq(expr, expected):
        sql = FugueSQLParser(
            expr,
            "fugueParams",
            ignore_case=True,
            parse_mode=_PARSE_MODE,
        )
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        assert expected == obj

    assert_eq("params a:10,b:'x'", dict(a=10, b="x"))
    assert_eq("params {a:10,b:'x'}", dict(a=10, b="x"))
    assert_eq("params (a:10,b:'x')", dict(a=10, b="x"))
    assert_eq("{a:10,b:'x'}", dict(a=10, b="x"))
    assert_eq("(a=1,b='x',c=True,d={x:3},)", dict(a=1, b="x", c=True, d=dict(x=3)))


def test_single_output_common_expr():
    def assert_eq(expr, using, params, schema):
        sql = FugueSQLParser(
            expr,
            "fugueSingleOutputExtensionCommon",
            ignore_case=True,
            parse_mode=_PARSE_MODE,
        )
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        assert using == obj["fugueUsing"]
        if params is None:
            assert "params" not in obj
        else:
            assert params == obj["params"]
        if schema is None:
            assert "schema" not in obj
        else:
            assert obj["schema"] == schema

    assert_eq("using a.B.c", "a.B.c", None, None)
    assert_eq("using a.B.c()", "a.B.c", {}, None)
    assert_eq("using a.B.c{}", "a.B.c", {}, None)
    assert_eq("using a.B.c(a=1,b=2)", "a.B.c", dict(a=1, b=2), None)
    assert_eq(
        "using a.B.c params a:1,b:2 schema a:int,b:int",
        "a.B.c",
        dict(a=1, b=2),
        "a:int,b:int",
    )


def test_assignment():
    def assert_eq(expr, varname, sign):
        sql = FugueSQLParser(
            expr,
            "fugueAssignment",
            ignore_case=True,
            parse_mode=_PARSE_MODE,
        )
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        assert (varname, sign) == obj

    assert_eq("aA=", "aA", "=")
    # assert_eq("aA??", "aA", "??")
