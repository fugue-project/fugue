import json
from typing import Any, Tuple

import pandas as pd

from fugue import Outputter
from fugue.extensions import namespace_candidate, parse_outputter

from ._ext import Visualize


@parse_outputter.candidate(namespace_candidate("viz", lambda x: isinstance(x, str)))
def _parse_pandas_plot(obj: Tuple[str, str]) -> Outputter:
    return _PandasVisualize(obj[1])


class _PandasVisualize(Visualize):
    def __init__(self, func: str) -> None:
        super().__init__(func)
        if func != "plot":
            getattr(pd.DataFrame.plot, func)  # ensure the func exists

    def _plot(self, df: pd.DataFrame) -> None:
        params = dict(self.params)
        if len(self.partition_spec.partition_by) > 0:
            keys = df[self.partition_spec.partition_by].head(1).to_dict("records")[0]
            kt = json.dumps(keys)[1:-1]
            if "title" in params:
                params["title"] = params["title"] + " -- " + kt
            else:
                params["title"] = kt
            df = df.drop(self.partition_spec.partition_by, axis=1)
        func = self._get_func(df)
        func(**params)

    def _get_func(self, df: pd.DataFrame) -> Any:
        if self._func == "plot":
            return df.plot
        return getattr(df.plot, self._func)
