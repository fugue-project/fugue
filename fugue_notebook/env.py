# pylint: disable=W0611,W0613
import html
import json
from typing import Any, Dict, List, Optional

import fugue_sql
from fugue import DataFrame, ExecutionEngine, display_dataset, make_execution_engine
from fugue.dataframe import YieldedDataFrame
from fugue.exceptions import FugueSQLSyntaxError
from IPython.core.magic import Magics, cell_magic, magics_class, needs_local_scope
from IPython.display import HTML, display
from triad import ParamDict
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


@display_dataset.candidate(
    lambda ds, *args, **kwargs: isinstance(ds, DataFrame), priority=3.0
)
def _display_dataframe_in_notebook(
    ds: DataFrame, n: int = 10, with_count: bool = False, title: Optional[str] = None
):
    components: List[Any] = []
    if title is not None:
        components.append(HTML(f"<h3>{html.escape(title)}</h3>"))
    if with_count:
        count = ds.count()
    else:
        count = -1
    pdf = ds.head(n).as_pandas()
    components.append(pdf)
    if count >= 0:
        components.append(HTML(f"<strong>total count: {count}</strong>"))
    components.append(HTML(f"<small>schema: {ds.schema}</small>"))
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
