import html
import json
from typing import Any, Callable, Dict, List

import fugue_sql
import pandas as pd
from fugue import (
    ExecutionEngine,
    NativeExecutionEngine,
    Yielded,
    make_execution_engine,
    register_execution_engine,
)
from fugue.extensions._builtins.outputters import Show
from IPython.core.magic import Magics, cell_magic, magics_class
from IPython.display import HTML, display
from triad import ParamDict, Schema
from triad.utils.convert import get_caller_global_local_vars, to_instance


@magics_class
class FugueSQLMagics(Magics):
    """Fugue SQL Magics"""

    def __init__(self, shell, pre_conf, post_conf):
        # You must call the parent constructor
        super().__init__(shell)
        self._pre_conf = pre_conf
        self._post_conf = post_conf
        self._yields: Dict[str, Yielded] = {}

    @cell_magic("fsql")
    def fsql(self, line: str, cell: str) -> None:
        gc, lc = get_caller_global_local_vars(start=-2, end=-6)
        gc.update(lc)
        gc.update(self._yields)
        if "__name__" in gc:
            del gc["__name__"]
        dag = fugue_sql.fsql(cell, gc)
        dag.run(self.get_engine(line, gc))
        self._yields.update(dag.yields)

    def get_engine(self, line: str, lc: Dict[str, Any]) -> ExecutionEngine:
        line = line.strip()
        p = line.find("{")
        if p >= 0:
            engine = line[:p].strip()
            conf = json.loads(line[p:])
        else:
            parts = line.split(" ", 1)
            engine = parts[0]
            conf = ParamDict(None if len(parts) == 1 else lc[parts[1]])
        cf = dict(self._pre_conf)
        cf.update(conf)
        for k, v in self._post_conf.items():
            if k in cf and cf[k] != v:
                raise ValueError(
                    f"{k} must be {v}, but you set to {cf[k]}, you may unset it"
                )
            cf[k] = v
        return make_execution_engine(engine, cf)


def default_pretty_print(
    schema: Schema,
    head_rows: List[List[Any]],
    title: Any,
    rows: int,
    count: int,
):
    components: List[Any] = []
    if title is not None:
        components.append(HTML(f"<h3>{html.escape(title)}</h3>"))
    pdf = pd.DataFrame(head_rows, columns=list(schema.names))
    components.append(pdf)
    if count >= 0:
        components.append(HTML(f"<strong>total count: {count}</strong>"))
    components.append(HTML(f"<small>schema: {schema}</small>"))
    display(*components)


class NotebookSetup(object):
    def get_pre_conf(self) -> Dict[str, Any]:
        return {}

    def get_post_conf(self) -> Dict[str, Any]:
        return {}

    def get_pretty_print(self) -> Callable:
        return default_pretty_print

    def register_execution_engines(self):
        register_execution_engine(
            "native", lambda conf, **kwargs: NativeExecutionEngine(conf=conf)
        )
        try:
            import pyspark  # noqa: F401
            from fugue_spark import SparkExecutionEngine

            register_execution_engine(
                "spark", lambda conf, **kwargs: SparkExecutionEngine(conf=conf)
            )
        except ImportError:
            pass
        try:
            import dask.dataframe  # noqa: F401
            from fugue_dask import DaskExecutionEngine

            register_execution_engine(
                "dask", lambda conf, **kwargs: DaskExecutionEngine(conf=conf)
            )
        except ImportError:
            pass


def setup_fugue_notebook(ipython: Any, setup_obj: Any) -> None:
    s = NotebookSetup() if setup_obj is None else to_instance(setup_obj, NotebookSetup)
    magics = FugueSQLMagics(ipython, dict(s.get_pre_conf()), dict(s.get_post_conf()))
    ipython.register_magics(magics)
    s.register_execution_engines()
    Show.set_hook(s.get_pretty_print())
