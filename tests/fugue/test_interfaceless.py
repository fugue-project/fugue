from typing import Any, Dict, Iterable

import pandas as pd

from fugue import DataFrame, PandasDataFrame, out_transform, transform


def test_transform():
    pdf = pd.DataFrame([[1, 10], [0, 0], [1, 1], [0, 20]], columns=["a", "b"])

    def f1(df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values("b").head(1)

    result = transform(pdf, f1, schema="*")
    assert isinstance(result, pd.DataFrame)
    assert result.values.tolist() == [[0, 0]]

    # schema: *
    def f2(df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values("b").head(1)

    result = transform(pdf, f2)
    assert isinstance(result, pd.DataFrame)
    assert result.values.tolist() == [[0, 0]]

    result = transform(pdf, f2, partition=dict(by=["a"]))
    assert isinstance(result, pd.DataFrame)
    assert sorted(result.values.tolist(), key=lambda x: x[0]) == [[0, 0], [1, 1]]

    ppdf = PandasDataFrame(pdf)
    assert isinstance(transform(ppdf, f2), DataFrame)


def test_out_transform():
    pdf = pd.DataFrame([[1, 10], [0, 0], [1, 1], [0, 20]], columns=["a", "b"])

    class T:
        def __init__(self):
            self.n = 0

        def f(self, df: Iterable[Dict[str, Any]]) -> None:
            self.n += 1

    t = T()
    out_transform(pdf, t.f)
    assert 1 == t.n

    t = T()
    out_transform(pdf, t.f, partition=dict(by=["a"]))
    assert 2 == t.n
