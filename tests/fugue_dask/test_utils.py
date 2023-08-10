import json

import dask.dataframe as dd
import pandas as pd
import pytest

from fugue_dask._utils import even_repartition, hash_repartition, rand_repartition


@pytest.fixture(scope="module")
def dask_client():
    from dask.distributed import Client

    with Client(processes=True) as client:
        yield client


def test_even_repartition_num(dask_client):
    def tr(df: pd.DataFrame):
        if len(df) == 0:
            return pd.DataFrame(dict(v=pd.Series(dtype="string")))
        return pd.DataFrame(dict(v=[json.dumps(list(df.aa.sort_values()))]))

    df = make_df([[0, 1], [], [1, 2, 3, 4]])

    for n in [6, 7]:
        rdf = even_repartition(df, n, [])
        res = rdf.map_partitions(tr, meta={"v": str}).compute()
        assert [json.loads(x) for x in sorted(res.v)] == [[0], [1], [1], [2], [3], [4]]

    rdf = even_repartition(df, 5, [])
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 1], [1, 2], [3, 4]]

    rdf = even_repartition(df, 1, [])
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 1, 1, 2, 3, 4]]

    assert df is even_repartition(df, 0, [])

    df = make_df([[], []])

    for n in [1, 2, 3]:
        rdf = even_repartition(df, n, [])
        res = rdf.map_partitions(tr, meta={"v": str}).compute()
        assert [json.loads(x) for x in sorted(res.v)] == []


def test_even_repartition_cols(dask_client):
    def tr(df: pd.DataFrame):
        if len(df) == 0:
            return pd.DataFrame(dict(v=pd.Series(dtype="string")))
        return pd.DataFrame(
            dict(v=[json.dumps(list(df.aa.drop_duplicates().sort_values()))])
        )

    df = make_df([[0, 1], [], [1, 2, 3, 4]])

    for n in [0, 5, 6]:
        rdf = even_repartition(df, n, ["aa", "bb"])
        res = rdf.map_partitions(tr, meta={"v": str}).compute()
        assert [json.loads(x) for x in sorted(res.v)] == [[0], [1], [2], [3], [4]]

    rdf = even_repartition(df, 3, ["aa", "bb"])
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 1], [2, 3], [4]]

    rdf = even_repartition(df, 1, ["aa", "bb"])
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 1, 2, 3, 4]]

    df = make_df([[0, 1], [], [1, 2, 3, 4]], with_emtpy=True)

    for n in [0, 5, 6]:
        rdf = even_repartition(df, n, ["aa", "bb"])
        res = rdf.map_partitions(tr, meta={"v": str}).compute()
        assert [json.loads(x) for x in sorted(res.v)] == [[0], [1], [2], [3], [4]]


def test_hash_repartition(dask_client):
    def tr(df: pd.DataFrame):
        if len(df) == 0:
            return pd.DataFrame(dict(v=pd.Series(dtype="string")))
        return pd.DataFrame(dict(v=[json.dumps(list(df.aa.sort_values()))]))

    df = make_df([[0, 1], [], [1, 2, 3, 4]])

    for n in [105, 107]:
        rdf = hash_repartition(df, n, [])
        res = rdf.map_partitions(tr, meta={"v": str}).compute()
        assert [json.loads(x) for x in sorted(res.v)] == [[0], [1, 1], [2], [3], [4]]

    rdf = hash_repartition(df, 3, [])
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 3], [1, 1, 4], [2]]

    rdf = hash_repartition(df, 3, ["aa"])
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 2], [1, 1, 3, 4]]

    rdf = hash_repartition(df, 1, [])
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 1, 1, 2, 3, 4]]

    assert df is hash_repartition(df, 0, [])
    assert df is hash_repartition(df, 0, ["aa"])

    df = make_df([[], []])

    for n in [1, 2, 3]:
        rdf = hash_repartition(df, n, ["aa"])
        res = rdf.map_partitions(tr, meta={"v": str}).compute()
        assert [json.loads(x) for x in sorted(res.v)] == []


def test_rand_repartition(dask_client):
    def tr(df: pd.DataFrame):
        if len(df) == 0:
            return pd.DataFrame(dict(v=pd.Series(dtype="string")))
        return pd.DataFrame(dict(v=[json.dumps(list(df.aa.sort_values()))]))

    df = make_df([[0, 1], [], [1, 2, 3, 4]])

    rdf = rand_repartition(df, 105, [], seed=0)
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 1], [1, 2], [3], [4]]

    rdf = rand_repartition(df, 2, [], seed=0)
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 1, 4], [1, 2, 3]]

    rdf = rand_repartition(df, 105, ["aa", "bb"], seed=0)
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0], [1, 1], [2], [3], [4]]

    rdf = rand_repartition(df, 3, ["aa", "bb"], seed=0)
    res = rdf.map_partitions(tr, meta={"v": str}).compute()
    assert [json.loads(x) for x in sorted(res.v)] == [[0, 2], [1, 1, 3], [4]]

    assert df is rand_repartition(df, 0, [])
    assert df is rand_repartition(df, 0, ["aa"])

    df = make_df([[], []])

    for n in [1, 2, 3]:
        rdf = rand_repartition(df, n, ["aa"])
        res = rdf.map_partitions(tr, meta={"v": str}).compute()
        assert [json.loads(x) for x in sorted(res.v)] == []


def make_df(data, with_emtpy=False):
    def _make_df(df: pd.DataFrame):
        assert len(df) == 1
        return pd.DataFrame(
            dict(
                aa=pd.Series(data[df.v.iloc[0]], dtype="int64"),
                bb=pd.Series(data[df.v.iloc[0]]).astype("string") + "b"
                if not with_emtpy
                else pd.Series(None, dtype="string"),
            )
        )

    pdf = pd.DataFrame(dict(idx=range(len(data)), v=range(len(data)))).set_index(
        "idx", drop=True
    )
    return dd.from_pandas(pdf, npartitions=len(pdf), sort=False).map_partitions(
        _make_df, meta=[("aa", int), ("bb", str)]
    )
