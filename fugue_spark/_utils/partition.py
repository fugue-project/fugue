import random
from typing import Any, Iterable, List

import pyspark.sql as ps
from fugue_spark._utils.convert import to_schema, to_spark_schema
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

_PARTITION_DUMMY_KEY = "__partition_dummy_key__"


def hash_repartition(
    session: SparkSession, df: ps.DataFrame, num: int, cols: List[Any]
) -> ps.DataFrame:
    if num <= 0:
        if len(cols) == 0:
            return df
        return df.repartition(*cols)
    if num == 1:
        return _single_repartition(df)
    return df.repartition(num, *cols)


def rand_repartition(
    session: SparkSession, df: ps.DataFrame, num: int, cols: List[Any]
) -> ps.DataFrame:
    if len(cols) > 0 or num <= 1:
        return hash_repartition(session, df, num, cols)

    def _rand(rows: Iterable[Any], n: int) -> Iterable[Any]:  # pragma: no cover
        for row in rows:
            yield (random.randint(0, n - 1), row)

    rdd = (
        df.rdd.mapPartitions(lambda r: _rand(r, num))
        .partitionBy(num, lambda k: k)
        .mapPartitions(_to_rows)
    )
    return session.createDataFrame(rdd, df.schema)


def even_repartition(
    session: SparkSession, df: ps.DataFrame, num: int, cols: List[Any]
) -> ps.DataFrame:
    if num == 1:
        return _single_repartition(df)
    if len(cols) == 0:
        if num == 0:
            return df
        rdd = (
            _zipWithIndex(df.rdd).partitionBy(num, lambda k: k).mapPartitions(_to_rows)
        )
        return session.createDataFrame(rdd, df.schema)
    else:
        keys = df.select(*cols).distinct()
        krdd = _zipWithIndex(keys.rdd, True)
        new_schema = to_spark_schema(
            to_schema(df.schema).extract(cols) + f"{_PARTITION_DUMMY_KEY}:long"
        )
        idx = session.createDataFrame(krdd, new_schema)
        if num <= 0:
            idx = idx.persist()
            num = idx.count()
        idf = (
            df.alias("df")
            .join(idx.alias("idx"), on=cols, how="inner")
            .select(_PARTITION_DUMMY_KEY, *["df." + x for x in df.schema.names])
        )

        def _to_kv(rows: Iterable[Any]) -> Iterable[Any]:  # pragma: no cover
            for row in rows:
                yield (row[0], row[1:])

        rdd = (
            idf.rdd.mapPartitions(_to_kv)
            .partitionBy(num, lambda k: k)
            .mapPartitions(_to_rows)
        )
        return session.createDataFrame(rdd, df.schema)


def _single_repartition(df: ps.DataFrame) -> ps.DataFrame:
    return (
        df.withColumn(_PARTITION_DUMMY_KEY, lit(0))
        .repartition(_PARTITION_DUMMY_KEY)
        .drop(_PARTITION_DUMMY_KEY)
    )


def _to_rows(rdd: Iterable[Any]) -> Iterable[Any]:  # pragma: no cover
    for item in rdd:
        yield item[1]


def _to_rows_with_key(rdd: Iterable[Any]) -> Iterable[Any]:  # pragma: no cover
    for item in rdd:
        yield list(item[1]) + [item[0]]


def _zipWithIndex(rdd: RDD, to_rows: bool = False) -> RDD:
    """
    Modified from
    https://github.com/davies/spark/blob/cebe5bfe263baf3349353f1473f097396821514a/python/pyspark/rdd.py

    """
    starts = [0]
    if rdd.getNumPartitions() > 1:
        nums = rdd.mapPartitions(lambda it: [sum(1 for i in it)]).collect()
        for i in range(len(nums) - 1):
            starts.append(starts[-1] + nums[i])

    def func1(k, it):  # pragma: no cover
        for i, v in enumerate(it, starts[k]):
            yield i, v

    def func2(k, it):  # pragma: no cover
        for i, v in enumerate(it, starts[k]):
            yield list(v) + [i]

    if not to_rows:
        return rdd.mapPartitionsWithIndex(func1)
    else:
        return rdd.mapPartitionsWithIndex(func2)
