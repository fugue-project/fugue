from fugue_spark._utils.convert import (to_cast_expression, to_schema,
                                       to_select_expression, to_spark_schema)
from pytest import raises


def test_schema_conversion(spark_session):
    def test(expr):
        assert to_schema(to_spark_schema(expr)) == expr

    test("a:int,b:long,c:[int],d:datetime,e:date,f:decimal(3,4),g:str")
    test("a:{a:[int],b:[str]}")
    # test("a:[{a:int}]") TODO: this is not supported by spark, should we support?
    s = to_spark_schema(to_spark_schema("a:int"))
    assert to_spark_schema(s) is s

    df = spark_session.createDataFrame([[1]], schema=to_spark_schema("a:int"))
    assert to_schema(to_spark_schema(df)) == "a:int"
    assert to_schema(df) == "a:int"
    assert to_schema(dict(a=str)) == "a:str"


def test_to_cast_expression():
    # length mismatch
    raises(ValueError, lambda: to_cast_expression(
        "a:int,b:int", "a:int", False))
    assert (False, ["a", "b"]) == to_cast_expression(
        "a:int,b:int", "a:int,b:int", False)
    assert (False, ["a", "b"]) == to_cast_expression(
        "a:int,b:int", "a:int,b:int", True)
    assert (True, ["aa AS a", "b"]) == to_cast_expression(
        "aa:int,b:int", "a:int,b:int", True)
    raises(ValueError, lambda: to_cast_expression(
        "aa:int,b:int", "a:int,b:int", False))
    assert (True, ["CAST(a AS int) a", "b"]) == to_cast_expression(
        "a:long,b:int", "a:int,b:int", True)
    assert (True, ["CAST(aa AS int) a", "b"]) == to_cast_expression(
        "aa:long,b:int", "a:int,b:int", True)


def test_to_select_expression():
    assert to_select_expression("a:int,b:str", ["b", "a"]) == ["b", "a"]
    raises(KeyError, lambda: to_select_expression("a:int,b:str", ["b", "x"]))
    assert to_select_expression("a:int,b:str", "b:str,a:int") == ["b", "a"]
    assert to_select_expression("a:int,b:str", "b:str,a:long") == [
        "b", "CAST(a AS bigint) a"]
    assert to_select_expression("a:int,b:double,c:float", "a:str,b:str,c:long") == [
        "CAST(a AS string) a",
        "CAST(IF(isnan(b) OR b IS NULL, NULL, b) AS string) b",
        "CAST(IF(isnan(c) OR c IS NULL, NULL, c) AS bigint) c"]
    raises(KeyError, lambda: to_select_expression("a:int,b:str",  "b:str,x:int"))
