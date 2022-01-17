from fugue_duckdb._utils import to_pa_type, to_duck_type, encode_value_to_expr
import pyarrow as pa
import duckdb
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP
from pytest import raises
import pandas as pd
import numpy as np


def test_encode_value_to_expr():
    assert "1.1" == encode_value_to_expr(1.1)
    assert "1.1" == encode_value_to_expr(np.float64(1.1))
    assert "1" == encode_value_to_expr(1)
    assert "1" == encode_value_to_expr(np.int32(1))
    assert "FALSE" == encode_value_to_expr(False)
    assert "TRUE" == encode_value_to_expr(np.bool(1))
    assert "E'abc'" == encode_value_to_expr("abc")
    assert "E'abc\\n;def'" == encode_value_to_expr("abc\n;def")
    assert "'\\xcaABC'::BLOB" == encode_value_to_expr(b"\xCAABC")
    assert "NULL" == encode_value_to_expr(None)
    assert "NULL" == encode_value_to_expr(float("nan"))
    assert "NULL" == encode_value_to_expr(pd.NA)
    assert "TIMESTAMP '2021-01-02 14:15:16'" == encode_value_to_expr(
        pd.to_datetime("2021-01-02 14:15:16")
    )
    assert "DATE '2021-01-02'" == encode_value_to_expr(
        pd.to_datetime("2021-01-02 14:15:16").date()
    )
    assert "[E'abc', 1, TIMESTAMP '2021-01-02 14:15:16']" == encode_value_to_expr(
        ["abc", 1, pd.to_datetime("2021-01-02 14:15:16")]
    )
    assert "{E'a\\n': E'abc', E'b': 1}" == encode_value_to_expr({"a\n": "abc", "b": 1})

    class D:
        pass

    raises(NotImplementedError, lambda: encode_value_to_expr(D()))


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
