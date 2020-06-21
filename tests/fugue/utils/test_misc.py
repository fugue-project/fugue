from fugue._utils.misc import get_attribute
from pytest import raises


def test_get_attribute():
    class C(object):
        pass

    c = C()
    assert "x" not in c.__dict__
    assert 0 == get_attribute(c, "x", int)
    assert 0 == c.x
    assert 0 == get_attribute(c, "x", int)
    c.x = 10
    assert 10 == get_attribute(c, "x", int)
    raises(TypeError, lambda: get_attribute(c, "x", str))
