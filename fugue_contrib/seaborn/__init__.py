import json
from functools import partial
from typing import Any, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn

from fugue import Outputter
from fugue.extensions import namespace_candidate, parse_outputter

from ..viz._ext import Visualize


@parse_outputter.candidate(namespace_candidate("sns", lambda x: isinstance(x, str)))
def _parse_seaborn(obj: Tuple[str, str]) -> Outputter:
    return _SeabornVisualize(obj[1])


class _SeabornVisualize(Visualize):
    def __init__(self, func: str) -> None:
        super().__init__(func)
        getattr(seaborn, func)  # ensure the func exists

    def _plot(self, df: pd.DataFrame) -> None:
        params = dict(self.params)
        title: Any = None
        if len(self.partition_spec.partition_by) > 0:
            keys = df[self.partition_spec.partition_by].head(1).to_dict("records")[0]
            kt = json.dumps(keys)[1:-1]
            if "title" in params:
                params["title"] = params["title"] + " -- " + kt
            else:
                params["title"] = kt
            df = df.drop(self.partition_spec.partition_by, axis=1)
        func = self._get_func(df)
        title = params.pop("title", None)
        plt.figure(0)
        func(**params).set(title=title)
        plt.show()

    def _get_func(self, df: pd.DataFrame) -> Any:
        f = getattr(seaborn, self._func)
        return partial(f, df)
