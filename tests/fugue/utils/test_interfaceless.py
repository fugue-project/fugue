from pytest import raises

from fugue._utils.interfaceless import (
    is_class_method,
    parse_comment_annotation,
    parse_output_schema_from_comment,
)


def test_parse_comment_annotation():
    def a():
        pass

    # asdfasdf
    def b():
        pass

    # asdfasdf
    # schema : s:int
    # # # schema : a : int,b:str
    # schema : a : str ,b:str
    # asdfasdf
    def c():
        pass

    # schema:
    def d():
        pass

    assert parse_comment_annotation(a, "schema") is None
    assert parse_comment_annotation(b, "schema") is None
    assert "a : str ,b:str" == parse_comment_annotation(c, "schema")
    assert "" == parse_comment_annotation(d, "schema")


def test_parse_output_schema_from_comment():
    def a():
        pass

    # asdfasdf
    def b():
        pass

    # asdfasdf
    # schema : s : int # more comment
    # # # schema : a :  int,b:str
    # asdfasdf
    def c():
        pass

    # schema:
    def d():
        pass

    assert parse_output_schema_from_comment(a) is None
    assert parse_output_schema_from_comment(b) is None
    assert "s:int" == parse_output_schema_from_comment(c).replace(" ", "")
    raises(SyntaxError, lambda: parse_output_schema_from_comment(d))


def test_is_class_method():
    def f1():
        pass

    class F(object):
        def f2(self):
            pass

    assert not is_class_method(f1)
    assert is_class_method(F.f2)
    assert not is_class_method(F().f2)
