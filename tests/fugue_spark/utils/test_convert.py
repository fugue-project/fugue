import pandas as pd
import pyarrow as pa
import pytest
from pytest import raises
import pyspark
from packaging import version
from triad import Schema

from fugue_spark._utils.convert import (
    _combine_arrow_chunks,
    pandas_udf_can_accept,
    to_cast_expression,
    to_schema,
    to_select_expression,
    to_spark_schema,
)


def test_pandas_udf_can_accept():
    for is_input in [True, False]:
        assert pandas_udf_can_accept(Schema("a:int,b:str"), is_input)
        assert pandas_udf_can_accept(Schema("a:int,b:[str],c:[float]"), is_input)
        if version.parse(pyspark.__version__) < version.parse("3.5"):
            assert not pandas_udf_can_accept(Schema("a:int,b:[datetime]"), is_input)
    assert pandas_udf_can_accept(Schema("a:int,b:{a:int}"), True)
    assert not pandas_udf_can_accept(Schema("a:int,b:{a:int}"), False)


def test_schema_conversion(spark_session):
    def test(expr):
        assert to_schema(to_spark_schema(expr)) == expr

    test("a:int,b:long,c:[int],d:datetime,e:date,f:decimal(3,4),g:str")
    test("a:{a:[int],b:[str]}")
    test("a:[{a:int}]")
    s = to_spark_schema(to_spark_schema("a:int"))
    assert to_spark_schema(s) is s

    df = spark_session.createDataFrame([[1]], schema=to_spark_schema("a:int"))
    assert to_schema(to_spark_schema(df)) == "a:int"
    assert to_schema(df) == "a:int"
    assert to_schema(dict(a=str)) == "a:str"

    from pyspark.sql.types import (
        ArrayType,
        IntegerType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField(
                "name",
                ArrayType(
                    StructType(
                        [
                            StructField("nest_name", StringType(), True),
                            StructField("nest_value", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            )
        ]
    )
    df = spark_session.createDataFrame([[[("a", 1), ("b", 2)]]], schema)
    assert to_schema(df) == "name:[{nest_name:str,nest_value:int}]"
    assert to_spark_schema("name:[{nest_name:str,nest_value:int}]") == schema

    schema = StructType(
        [StructField("a", MapType(StringType(), IntegerType(), True), True)],
    )
    df = spark_session.createDataFrame([[{"x": 1}], [{"y": 2}]], schema)
    assert to_schema(df) == "a:<str,int>"
    assert to_spark_schema("a:<str,int>") == schema


def test_to_cast_expression():
    # length mismatch
    raises(ValueError, lambda: to_cast_expression("a:int,b:int", "a:int", False))
    assert (False, ["a", "b"]) == to_cast_expression(
        "a:int,b:int", "a:int,b:int", False
    )
    assert (False, ["a", "b"]) == to_cast_expression("a:int,b:int", "a:int,b:int", True)
    assert (True, ["aa AS a", "b"]) == to_cast_expression(
        "aa:int,b:int", "a:int,b:int", True
    )
    raises(ValueError, lambda: to_cast_expression("aa:int,b:int", "a:int,b:int", False))
    assert (True, ["CAST(a AS int) a", "b"]) == to_cast_expression(
        "a:long,b:int", "a:int,b:int", True
    )
    assert (True, ["CAST(aa AS int) a", "b"]) == to_cast_expression(
        "aa:long,b:int", "a:int,b:int", True
    )


def test_to_select_expression():
    assert to_select_expression("a:int,b:str", ["b", "a"]) == ["b", "a"]
    raises(KeyError, lambda: to_select_expression("a:int,b:str", ["b", "x"]))
    assert to_select_expression("a:int,b:str", "b:str,a:int") == ["b", "a"]
    assert to_select_expression("a:int,b:str", "b:str,a:long") == [
        "b",
        "CAST(a AS bigint) a",
    ]
    assert to_select_expression("a:int,b:double,c:float", "a:str,b:str,c:long") == [
        "CAST(a AS string) a",
        "CAST(IF(isnan(b) OR b IS NULL, NULL, b) AS string) b",
        "CAST(IF(isnan(c) OR c IS NULL, NULL, c) AS bigint) c",
    ]
    raises(KeyError, lambda: to_select_expression("a:int,b:str", "b:str,x:int"))


def _chunks(s):
    return s.array.__arrow_array__().num_chunks


@pytest.mark.parametrize(
    "arr",
    [
        pd.array(["x", None], dtype="string[pyarrow]"),
        pd.array([1, None], dtype="int64[pyarrow]"),
        pd.array(pd.to_datetime(["2020-01-01", None]), dtype="timestamp[us][pyarrow]"),
        pd.Series(["x", None], dtype="str").array,  # pandas 3 default str
    ],
)
def test_combine_arrow_chunks_merges(arr):
    df = pd.DataFrame({"a": arr, "b": [1, 2]})
    c = pd.concat([df, df])
    if not (
        isinstance(c["a"].dtype, pd.ArrowDtype)
        or getattr(c["a"].dtype, "storage", None) == "pyarrow"
    ):
        pytest.skip("not arrow-backed on this pandas")
    assert _chunks(c["a"]) == 2
    out = _combine_arrow_chunks(c)
    assert _chunks(out["a"]) == 1
    assert out["a"].dtype == c["a"].dtype
    assert out["a"].isna().to_list() == c["a"].isna().to_list()
    assert out["a"].dropna().to_list() == c["a"].dropna().to_list()
    assert out["b"].to_list() == [1, 2, 1, 2]
    assert _chunks(c["a"]) == 2  # input not mutated


def test_combine_arrow_chunks_noop_returns_same_object():
    df = pd.DataFrame(
        {
            "o": pd.Series(["x"], dtype=object),
            "i": pd.array([1], dtype="Int64"),
            "s": pd.array(["x"], dtype="string[pyarrow]"),  # single chunk
            "n": [1.0],
        }
    )
    assert _combine_arrow_chunks(df) is df


def test_combine_arrow_chunks_empty():
    df = pd.DataFrame({"a": pd.array([], dtype="string[pyarrow]")})
    out = _combine_arrow_chunks(pd.concat([df, df]))
    assert len(out) == 0 and out["a"].dtype == df["a"].dtype
