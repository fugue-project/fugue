from typing import Iterable, Iterator

import polars as pl

import fugue.api as fa


def test_transform_common():
    def tr1(df: pl.DataFrame) -> pl.DataFrame:
        tdf = df.with_column(pl.lit(1, pl.Int32()).alias("b"))
        return tdf

    def tr2(dfs: Iterable[pl.DataFrame]) -> Iterator[pl.DataFrame]:
        for df in dfs:
            tdf = df.with_column(pl.lit(1, pl.Int32()).alias("b"))
            yield tdf

    for tr in [tr1, tr2]:
        df = fa.as_fugue_df([[0], [1]], schema="a:int")
        fdf = fa.transform(df, tr, schema="a:int,b:int", as_fugue=True)
        assert fdf.schema == "a:int,b:int"
        assert fdf.as_array() == [[0, 1], [1, 1]]

        df = fa.as_fugue_df([[0], [1]], schema="a:int")
        fdf = fa.transform(df, tr, schema="b:int,a:str", as_fugue=True)
        assert fdf.schema == "b:int,a:str"
        assert fdf.as_array() == [[1, "0"], [1, "1"]]

        # polars use large_string, so this needs to be handled
        df = fa.as_fugue_df([["0"], ["1"]], schema="a:str")
        fdf = fa.transform(df, tr, schema="b:int,a:str", as_fugue=True)
        assert fdf.schema == "b:int,a:str"
        assert fdf.as_array() == [[1, "0"], [1, "1"]]

        df = fa.as_fugue_df([], schema="a:int")
        fdf = fa.transform(df, tr, schema="a:int,b:int", as_fugue=True)
        assert fdf.schema == "a:int,b:int"
        assert fdf.as_array() == []


def test_transform_empty_result():
    def tr1(df: pl.DataFrame) -> pl.DataFrame:
        tdf = df.with_column(pl.lit(1, pl.Int32()).alias("b"))
        return tdf.head(0)

    def tr2(dfs: Iterable[pl.DataFrame]) -> Iterator[pl.DataFrame]:
        for _ in []:
            yield None

    def tr3(dfs: Iterable[pl.DataFrame]) -> Iterator[pl.DataFrame]:
        return

    for tr in [tr1, tr2, tr3]:
        df = fa.as_fugue_df([[0], [1]], schema="a:int")
        fdf = fa.transform(df, tr, schema="a:int,b:int", as_fugue=True)
        assert fdf.schema == "a:int,b:int"
        assert fdf.as_array() == []
