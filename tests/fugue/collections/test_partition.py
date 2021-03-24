import json

from fugue.collections.partition import parse_presort_exp, PartitionSpec
from fugue.constants import KEYWORD_CORECOUNT, KEYWORD_ROWCOUNT
from pytest import raises
from triad.collections.schema import Schema
from triad.utils.hash import to_uuid
from triad.collections.dict import IndexedOrderedDict

def test_parse_presort_exp():

    assert parse_presort_exp(None) == IndexedOrderedDict()
    assert parse_presort_exp(IndexedOrderedDict([('c', True)])) == IndexedOrderedDict([('c', True)])
    assert parse_presort_exp("c") == IndexedOrderedDict([('c', True)])
    assert parse_presort_exp("         c") == IndexedOrderedDict([('c', True)])
    assert parse_presort_exp("c           desc")  == IndexedOrderedDict([('c', False)])
    assert parse_presort_exp("b desc, c asc")  == IndexedOrderedDict([('b', False), ('c', True)])
    assert parse_presort_exp("DESC DESC, ASC ASC") == IndexedOrderedDict([('DESC', False), ('ASC', True)])
    assert parse_presort_exp([("b", False),("c", True)]) == IndexedOrderedDict([('b', False), ('c', True)])
    assert parse_presort_exp("B DESC, C ASC")  == IndexedOrderedDict([('B', False), ('C', True)])
    assert parse_presort_exp("b desc, c asc") == IndexedOrderedDict([('b', False), ('c', True)])
    

    with raises(SyntaxError):
        parse_presort_exp("b dsc, c asc") # mispelling of desc

    with raises(SyntaxError):
        parse_presort_exp("c true") # string format needs desc/asc

    with raises(SyntaxError):
        parse_presort_exp("c true, c true") # cannot contain duplicates

    with raises(SyntaxError):
        parse_presort_exp([("b", "desc"),("c", "asc")]) # instead of desc and asc, needs to be bool


def test_partition_spec():
    p = PartitionSpec()
    assert [] == p.partition_by
    "0" == p.num_partitions
    {} == p.presort
    "hash" == p.algo
    assert p.empty

    p = PartitionSpec(None)
    assert p.empty
    p2 = PartitionSpec(p)
    assert p2.empty

    p = PartitionSpec(json.dumps(dict(partition_by=["a", "b", "c"], num_partitions=1)))
    assert ["a", "b", "c"] == p.partition_by
    assert "1" == p.num_partitions
    assert {} == p.presort
    assert "hash" == p.algo
    assert not p.empty

    p = PartitionSpec(dict(by=["a", "b", "c"], presort="d asc,e desc"))
    assert ["a", "b", "c"] == p.partition_by
    assert "0" == p.num_partitions
    assert dict(d=True, e=False) == p.presort
    assert "hash" == p.algo
    assert not p.empty

    p = PartitionSpec(by=["a", "b", "c"], num=5, presort="d,e desc", algo="EvEN")
    assert ["a", "b", "c"] == p.partition_by
    assert "5" == p.num_partitions
    assert dict(d=True, e=False) == p.presort
    assert "even" == p.algo
    assert not p.empty

    p = PartitionSpec(
        partition_by=["a", "b", "c"],
        presort="d,e desc",
        algo="EvEN",
        num_partitions="ROWCOUNT*3",
        row_limit=4,
        size_limit="5k",
    )
    p2 = PartitionSpec(p)
    assert p2.jsondict == p.jsondict
    assert "d ASC,e DESC" == p2.presort_expr
    assert not p.empty
    assert not p2.empty
    print(p)
    print(f"{p}")

    assert PartitionSpec("per_row") == PartitionSpec(num="ROWCOUNT", algo="even")
    assert PartitionSpec(by="abc") == PartitionSpec(by=["abc"])

    # partition by overlaps with presort
    raises(
        SyntaxError,
        lambda: PartitionSpec(
            partition_by=["a", "b", "c"], presort="a asc,e desc", algo="EvEN"
        ),
    )

    # partition by has dups
    raises(SyntaxError, lambda: PartitionSpec(partition_by=["a", "b", "b"]))

    # partition by has dups
    raises(SyntaxError, lambda: PartitionSpec(partition_by=["a", "b", "b"]))

    # partition by is invalid
    raises(SyntaxError, lambda: PartitionSpec(partition_by=123))

    # bad input
    raises(TypeError, lambda: PartitionSpec(1))

    # bad presort
    raises(SyntaxError, lambda: PartitionSpec(presort="a xsc,e desc"))
    raises(SyntaxError, lambda: PartitionSpec(presort="a asc,a desc"))
    raises(SyntaxError, lambda: PartitionSpec(presort="a b asc,a desc"))
    raises(SyntaxError, lambda: PartitionSpec(presort=["a asc", ("b", True)]))
    raises(SyntaxError, lambda: PartitionSpec(presort=[("a", "asc"), "b"]))
    raises(SyntaxError, lambda: PartitionSpec(presort=[("a",), ("b")]))
    raises(SyntaxError, lambda: PartitionSpec(presort=["a", ["b", True]]))

    p = PartitionSpec(dict(partition_by=["a"], presort="d asc,e desc"))
    assert dict(a=True, d=True, e=False) == p.get_sorts(
        Schema("a:int,b:int,d:int,e:int")
    )
    p = PartitionSpec(dict(partition_by=["e", "a"], presort="d asc"))
    assert p.get_key_schema(Schema("a:int,b:int,d:int,e:int")) == "e:int,a:int"

    # modification
    a = PartitionSpec(by=["a", "b"])
    b = PartitionSpec(a, by=["a"], num=2)
    assert ["a", "b"] == a.partition_by
    assert "0" == a.num_partitions
    assert ["a"] == b.partition_by
    assert "2" == b.num_partitions

    a = PartitionSpec(by=["a"], presort="b DESC, c")
    b = PartitionSpec(by=["a"], presort="c,b DESC")
    assert a.presort != b.presort
    c = PartitionSpec(b, presort=a.presort)
    assert a.presort == c.presort
    c = PartitionSpec(b, presort=[("b", False), ("c", True)])
    assert a.presort == c.presort

    a = PartitionSpec(by=["a"], presort="b DESC, c")
    b = PartitionSpec(by=["a"], presort=[("c", True), ("b", False)])
    c = PartitionSpec(by=["a"], presort=["c", ("b", False)])
    d = PartitionSpec(by=["a"], presort=[("b", False), ("c", True)])
    assert a.presort != b.presort
    assert b.presort == c.presort
    assert a.presort == d.presort

    a = PartitionSpec(by=["a"], presort=["b", "c"])
    b = PartitionSpec(by=["a"], presort=[("b"), ("c")])
    c = PartitionSpec(by=["a"], presort=[("b", True), ("c", True)])
    d = PartitionSpec(by=["a"], presort="b asc, c")
    assert a.presort == b.presort
    assert a.presort == c.presort
    assert a.presort == d.presort

    # test eq
    a = PartitionSpec(by=["a"], presort=["b", "c"])
    b = PartitionSpec(presort="b, c asc", by=["a"])
    c = PartitionSpec(num=10, by=["a"], presort=["b", "c"])
    d = PartitionSpec(num=10, by=["a"], presort=["c", "b"])
    assert a == a
    assert a == dict(presort=["b", "c"], by=["a"])
    assert a == b
    assert a != c
    assert c != d


def test_partition_cursor():
    p = PartitionSpec(dict(partition_by=["b", "a"]))
    s = Schema("a:int,b:int,c:int,d:int")
    c = p.get_cursor(s, 2)
    pt = p.get_partitioner(s)  # this part is well covered in spark section
    assert c.row_schema == s
    assert c.key_schema == "b:int,a:int"

    c.set([1, 2, 2, 2], 5, 6)
    assert [2, 1] == c.key_value_array
    assert dict(a=1, b=2) == c.key_value_dict
    assert 2 == c["c"]
    assert [1, 2, 2, 2] == c.row
    assert 5 == c.partition_no
    assert 2 == c.physical_partition_no
    assert 6 == c.slice_no


def test_get_num_partitions():
    p = PartitionSpec(dict(partition_by=["b", "a"]))
    assert 0 == p.get_num_partitions()

    p = PartitionSpec(dict(partition_by=["b", "a"], num=123))
    assert 123 == p.get_num_partitions()

    p = PartitionSpec(dict(partition_by=["b", "a"], num="(x + Y) * 2"))
    assert 6 == p.get_num_partitions(x=lambda: 1, Y=lambda: 2)
    raises(Exception, lambda: p.get_num_partitions(x=lambda: 1))

    p = PartitionSpec(dict(partition_by=["b", "a"], num="min(ROWCOUNT,CORECOUNT)"))
    assert 90 == p.get_num_partitions(
        **{KEYWORD_ROWCOUNT: lambda: 100, KEYWORD_CORECOUNT: lambda: 90}
    )


def test_determinism():
    a = PartitionSpec(num=0)
    b = PartitionSpec()
    assert to_uuid(a) == to_uuid(b)

    a = PartitionSpec(by=["a"], num=2)
    b = PartitionSpec(num="2", by=["a"])
    assert to_uuid(a) == to_uuid(b)

    a = PartitionSpec(by=["a", "b"])
    b = PartitionSpec(by=["b", "a"])
    assert to_uuid(a) != to_uuid(b)
