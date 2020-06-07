from fugue_sql.antlr import FugueSQLListener
from fugue_sql.exceptions import FugueSQLSyntaxError
from fugue_sql.parse import FugueSQL
from pytest import raises
from tests.fugue_sql.utils import (bad_single_syntax, bad_syntax,
                                   good_single_syntax, good_syntax)


def test_assign_syntax():
    # simple assign not enabled
    bad_single_syntax(
        "a = select a where a==10",
        ignore_case=True,
        simple_assign=False)
    # when simple assign enabled, comparison using = is still valid
    good_single_syntax(
        "a = select a where a=10",
        ignore_case=True,
        simple_assign=True)
    good_single_syntax(
        "a = select a where a==10",
        ignore_case=True,
        simple_assign=True)
    # multiple expression test
    good_syntax(
        """
        a = select a where a==10 
        b=select x""",
        ignore_case=True,
        simple_assign=True)


def test_partition_syntax():
    good_single_syntax(
        "a = ", ["", "hash", "even", "rand"], "  partition 100 ", " select a",
        ignore_case=True,
        simple_assign=True)
    good_single_syntax(
        "a = partition 100 ", ["by a,b,c"], ["presort a asc, b desc"], " select a",
        ignore_case=True,
        simple_assign=True)
    good_single_syntax(
        "a = partition by a", ["presort a asc, b desc"], " select a",
        ignore_case=True,
        simple_assign=True)


def test_persist_broadcast_syntax():
    good_single_syntax(
        "a = select a", ["PERSIst", "persist a12", ""], ["BROADCAst"],
        ignore_case=True,
        simple_assign=True)


def test_select_syntax():
    good_single_syntax(["a:=", "a??", ""], "SELECT a", ["FROM sx"], ignore_case=False)
    bad_single_syntax('select a', ["from sx"], ignore_case=False)
    good_single_syntax(
        'select a',
        ["from sx"],
        ["where a=10 and a==10"],
        ignore_case=True)


def test_transform_syntax():
    # test data sources and nested
    good_single_syntax(
        "a = transform ",
        ["", "a,b", "(transform using x)"],
        "using x",
        ignore_case=True,
        simple_assign=True)
    # extensions
    good_single_syntax(
        "a = transform using ",
        ["x", "x.Y.z"],
        ignore_case=True,
        simple_assign=True)
    # params
    good_single_syntax(
        "a= transform using x",
        ["", "params a:10,b=\"20\""],
        ignore_case=True,
        simple_assign=True)
    # params
    good_single_syntax(
        "a= transform using x",
        ["params"],
        ["{a:10,b:{x:10,y:true,z:false,w:null}}",
         "(a=10,b=True)",
         "(a:10,b:{x:10,y:true,z:false,w:null})"],
        ignore_case=True,
        simple_assign=True)
    # schemas
    good_single_syntax(
        "a= transform using x",
        ["", "schema a : int,b:{x:int,y:[str]},c:[int]"],
        ignore_case=True,
        simple_assign=True)
    # schemas bad list
    bad_single_syntax(
        "a= transform using x schema a:int,c:[int,str]",
        ignore_case=True,
        simple_assign=True)


def test_process_create_syntax():
    good_single_syntax(
        "a = create using a.b.c(a=1,b=2) schema a:int",
        ignore_case=True,
        simple_assign=True)
    good_single_syntax(
        "a = process",
        ["", "a,b", "(create using x)"],
        "using a.b.c(a=1,b=2) schema a:int",
        ignore_case=True,
        simple_assign=True)
