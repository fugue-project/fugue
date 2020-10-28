import json

from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.workflow.workflow import FugueWorkflow
from fugue_sql.exceptions import FugueSQLSyntaxError
from fugue_sql._parse import FugueSQL
from fugue_sql._visitors import _Extensions, _VisitorBase
from pytest import raises
from triad.collections.schema import Schema


def test_json():
    def assert_eq(expr, expected):
        sql = FugueSQL(expr, "fugueJsonValue", ignore_case=True)
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


def test_schema():
    def assert_eq(expr, expected=None):
        sql = FugueSQL(expr, "fugueSchema", ignore_case=True)
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


def test_wild_schema():
    def assert_eq(expr, expected=None):
        sql = FugueSQL(expr, "fugueWildSchema", ignore_case=True)
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
    with raises(FugueSQLSyntaxError):
        assert_eq("*,a:int32, *, \nb:string")


def test_pre_partition():
    def assert_eq(expr, expected):
        sql = FugueSQL(expr, "fuguePrepartition", ignore_case=True)
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
        sql = FugueSQL(expr, "fugueParams", ignore_case=True)
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
        sql = FugueSQL(expr, "fugueSingleOutputExtensionCommon", ignore_case=True)
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        assert using == obj["using"]
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
        sql = FugueSQL(expr, "fugueAssignment", ignore_case=True, simple_assign=True)
        v = _VisitorBase(sql)
        obj = v.visit(sql.tree)
        assert (varname, sign) == obj

    assert_eq("aA:=", "aA", ":=")
    assert_eq("aA=", "aA", "=")
    # assert_eq("aA??", "aA", "??")
