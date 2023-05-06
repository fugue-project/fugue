from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from triad import assert_or_throw

from fugue import DataFrames, Outputter
from fugue.exceptions import FugueWorkflowError


class Visualize(Outputter, ABC):
    def __init__(self, func: str) -> None:
        super().__init__()
        self._func = func

    def process(self, dfs: DataFrames) -> None:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        df = dfs[0].as_pandas()
        presort = self.partition_spec.presort
        presort_keys = list(presort.keys())
        presort_asc = list(presort.values())
        if len(presort_keys) > 0:
            df = df.sort_values(presort_keys, ascending=presort_asc).reset_index(
                drop=True
            )
        if len(self.partition_spec.partition_by) == 0:
            self._plot(df)
        else:
            keys: Any = (  # avoid pandas warning
                self.partition_spec.partition_by
                if len(self.partition_spec.partition_by) > 1
                else self.partition_spec.partition_by[0]
            )
            for _, gp in df.groupby(keys, dropna=False):
                self._plot(gp.reset_index(drop=True))

    @abstractmethod
    def _plot(self, df: pd.DataFrame) -> None:  # pragma: no cover
        raise NotImplementedError
