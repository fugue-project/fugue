from fugue_duckdb._utils import to_pa_type, to_duck_type
import pyarrow as pa
import duckdb
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP
from pytest import raises


def test_type_conversion():
    con = duckdb.connect()

    def assert_(tp):
        dt = con.from_arrow_table(pa.Table.from_pydict(dict(a=pa.nulls(2, tp)))).types[
            0
        ]
        assert to_pa_type(dt) == tp
        dt = to_duck_type(tp)
        assert to_pa_type(dt) == tp

    def assert_many(*tps):
        for tp in tps:
            assert_(tp)
            assert_(pa.list_(tp))

    assert_(pa.bool_())
    assert_many(pa.int8(), pa.int16(), pa.int32(), pa.int64())
    assert_many(pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64())
    assert_many(pa.float32(), pa.float64())
    assert_(TRIAD_DEFAULT_TIMESTAMP)
    assert_(pa.decimal128(2, 3))
    assert_(pa.binary())
    assert_(pa.string())

    # nested
    assert_(pa.struct([pa.field("x", pa.int64())]))
    assert_(pa.struct([pa.field("x", pa.int64()), pa.field("yy", pa.string())]))
    assert_(pa.list_(pa.struct([pa.field("x", pa.int64())])))
    assert_(
        pa.struct([pa.field("x", pa.int64()), pa.field("yy", pa.list_(pa.int64()))])
    )

    raises(ValueError, lambda: to_pa_type(""))
    raises(ValueError, lambda: to_pa_type("XX"))
    raises(ValueError, lambda: to_pa_type("XX(1,2)"))
    raises(ValueError, lambda: to_pa_type("XX<VARCHAR>"))
