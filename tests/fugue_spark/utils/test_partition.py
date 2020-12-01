from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue_spark._utils.convert import to_schema, to_spark_schema
from fugue_spark._utils.partition import (
    even_repartition,
    hash_repartition,
    rand_repartition,
)
from pyspark.sql import SparkSession


def test_hash_repartition(spark_session):
    df = _df([[0, 1], [0, 2], [0, 3], [0, 4], [1, 1], [1, 2], [1, 3]], "a:int,b:int")
    res = hash_repartition(spark_session, df, 0, []).collect()
    assert 7 == len(res)
    res = hash_repartition(spark_session, df, 1, []).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert 7 == len([x for x in res if x[2] == 7])
    res = hash_repartition(spark_session, df, 1, ["a"]).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert 7 == len([x for x in res if x[2] == 7])
    res = hash_repartition(spark_session, df, 0, ["a"]).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    res = (
        hash_repartition(spark_session, df, 10, ["a"]).rdd.mapPartitions(_pc).collect()
    )
    assert 7 == len(res)
    assert 4 == len([x for x in res if x[2] == 4])
    assert 3 == len([x for x in res if x[2] == 3])


def test_rand_repartition(spark_session):
    df = _df([[0, 1], [0, 2], [0, 3], [0, 4], [1, 1], [1, 2], [1, 3]], "a:int,b:int")
    for i in [0, 1, 2, 3]:
        for p in [[], ["a"]]:
            res = (
                rand_repartition(spark_session, df, i, p)
                .rdd.mapPartitions(_pc)
                .collect()
            )
            assert 7 == len(res)


def test_even_repartition_no_cols(spark_session):
    df = _df([[0, 1], [0, 2], [0, 3], [0, 4], [1, 1], [1, 2], [1, 3]], "a:int,b:int")
    res = even_repartition(spark_session, df, 0, []).collect()
    assert 7 == len(res)
    res = even_repartition(spark_session, df, 1, []).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert 7 == len([x for x in res if x[2] == 7])
    res = even_repartition(spark_session, df, 6, []).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert 5 == len([x for x in res if x[2] == 1])
    assert 2 == len([x for x in res if x[2] == 2])
    res = even_repartition(spark_session, df, 7, []).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert all(x[2] == 1 for x in res)
    res = even_repartition(spark_session, df, 8, []).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert all(x[2] == 1 for x in res)


def test_even_repartition_with_cols(spark_session):
    df = _df([[0, 1], [0, 2], [0, 3], [0, 4], [1, 1], [1, 2], [1, 3]], "a:int,b:int")
    res = even_repartition(spark_session, df, 0, ["a"]).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert 4 == len([x for x in res if x[2] == 4])
    assert 3 == len([x for x in res if x[2] == 3])
    res = (
        even_repartition(spark_session, df, 0, ["a", "b"])
        .rdd.mapPartitions(_pc)
        .collect()
    )
    assert 7 == len(res)
    assert 7 == len([x for x in res if x[2] == 1])
    res = (
        even_repartition(spark_session, df, 1, ["a", "b"])
        .rdd.mapPartitions(_pc)
        .collect()
    )
    assert 7 == len(res)
    assert 7 == len([x for x in res if x[2] == 7])
    res = even_repartition(spark_session, df, 3, ["a"]).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert 4 == len([x for x in res if x[2] == 4])
    assert 3 == len([x for x in res if x[2] == 3])
    res = even_repartition(spark_session, df, 0, ["b"]).rdd.mapPartitions(_pc).collect()
    assert 7 == len(res)
    assert 6 == len([x for x in res if x[2] == 2])
    assert 1 == len([x for x in res if x[2] == 1])

    # test with multiple keys and that are not the first positions
    df = _df(
        [
            [1, "a", 1],
            [2, "b", 2],
            [3, "c", 3],
            [4, "d", 4],
            [5, "e", 1],
            [6, "f", 2],
            [7, "g", 3],
        ],
        "z:int,a:str,b:int",
    )
    res = (
        even_repartition(spark_session, df, 0, ["b", "z"])
        .rdd.mapPartitions(_pc)
        .collect()
    )
    assert sorted(res) == sorted(
        [
            [1, "a", 1, 1],
            [2, "b", 2, 1],
            [3, "c", 3, 1],
            [4, "d", 4, 1],
            [5, "e", 1, 1],
            [6, "f", 2, 1],
            [7, "g", 3, 1],
        ]
    )


def _pc(df):
    data = list(df)
    for row in data:
        yield list(row) + [len(data)]


def _df(data, schema=None, metadata=None):
    session = SparkSession.builder.getOrCreate()
    if schema is not None:
        pdf = PandasDataFrame(data, to_schema(schema), metadata)
        return session.createDataFrame(pdf.native, to_spark_schema(schema))
    else:
        return session.createDataFrame(data)
