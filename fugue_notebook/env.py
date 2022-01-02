# pylint: disable=W0611,W0613
import html
import json
from typing import Any, Callable, Dict, List

import fugue_sql
import pandas as pd
from fugue import (
    ExecutionEngine,
    NativeExecutionEngine,
    make_execution_engine,
    register_execution_engine,
)
from fugue.dataframe import YieldedDataFrame
from fugue.extensions._builtins.outputters import Show
from fugue_sql.exceptions import FugueSQLSyntaxError
from IPython.core.magic import Magics, cell_magic, magics_class, needs_local_scope
from IPython.display import HTML, display
from triad import ParamDict, Schema
from triad.utils.convert import to_instance


class NotebookSetup(object):
    """Jupyter notebook environment customization template."""

    def get_pre_conf(self) -> Dict[str, Any]:
        """The default config for all registered execution engine"""
        return {}

    def get_post_conf(self) -> Dict[str, Any]:
        """The enforced config for all registered execution engine.
        Users should not set these configs manually, if they set, the values
        must match this dict, otherwise, exceptions will be thrown
        """
        return {}

    def get_pretty_print(self) -> Callable:
        """Fugue dataframe pretty print handler"""
        return _default_pretty_print

    def register_execution_engines(self):
        """Register execution engines with names. This will also try to register
        spark and dask engines if the dependent packages are available and they
        are not registered"""
        register_execution_engine(
            "native",
            lambda conf, **kwargs: NativeExecutionEngine(conf=conf),
            on_dup="ignore",
        )

        try:
            import pyspark  # noqa: F401
            import fugue_spark  # noqa: F401
        except ImportError:
            pass

        try:
            import dask.dataframe  # noqa: F401
            import fugue_dask  # noqa: F401
        except ImportError:
            pass


@magics_class
class _FugueSQLMagics(Magics):
    """Fugue SQL Magics"""

    def __init__(
        self,
        shell: Any,
        pre_conf: Dict[str, Any],
        post_conf: Dict[str, Any],
        fsql_ignore_case: bool = False,
    ):
        # You must call the parent constructor
        super().__init__(shell)
        self._pre_conf = pre_conf
        self._post_conf = post_conf
        self._fsql_ignore_case = fsql_ignore_case

    @needs_local_scope
    @cell_magic("fsql")
    def fsql(self, line: str, cell: str, local_ns: Any = None) -> None:
        try:
            dag = fugue_sql.fsql(
                "\n" + cell, local_ns, fsql_ignore_case=self._fsql_ignore_case
            )
        except FugueSQLSyntaxError as ex:
            raise FugueSQLSyntaxError(str(ex)).with_traceback(None) from None
        dag.run(self.get_engine(line, {} if local_ns is None else local_ns))
        for k, v in dag.yields.items():
            if isinstance(v, YieldedDataFrame):
                local_ns[k] = v.result  # type: ignore
            else:
                local_ns[k] = v  # type: ignore

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
        if "+" in engine:
            return make_execution_engine(tuple(engine.split("+", 1)), cf)
        return make_execution_engine(engine, cf)


def _default_pretty_print(
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


def _setup_fugue_notebook(
    ipython: Any, setup_obj: Any, fsql_ignore_case: bool = False
) -> None:
    s = NotebookSetup() if setup_obj is None else to_instance(setup_obj, NotebookSetup)
    magics = _FugueSQLMagics(
        ipython,
        dict(s.get_pre_conf()),
        dict(s.get_post_conf()),
        fsql_ignore_case=fsql_ignore_case,
    )
    ipython.register_magics(magics)
    s.register_execution_engines()
    Show.set_hook(s.get_pretty_print())
