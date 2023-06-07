from typing import Any, Dict, Tuple

from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_caller_global_local_vars

from fugue._utils.misc import import_fsql_dependency

from ..collections.yielded import Yielded
from ..constants import FUGUE_CONF_SQL_DIALECT, FUGUE_CONF_SQL_IGNORE_CASE
from ..dataframe.api import is_df
from ..dataframe.dataframe import DataFrame
from ..workflow.workflow import FugueWorkflow, WorkflowDataFrame, WorkflowDataFrames
from ._utils import LazyWorkflowDataFrame, fill_sql_template


class FugueSQLWorkflow(FugueWorkflow):
    """Fugue workflow that supports Fugue SQL. Please read |FugueSQLTutorial|."""

    def __init__(self, compile_conf: Any = None):
        super().__init__(compile_conf)
        self._sql_vars: Dict[str, WorkflowDataFrame] = {}

    @property
    def sql_vars(self) -> Dict[str, WorkflowDataFrame]:
        return self._sql_vars

    def __call__(self, code: str, *args: Any, **kwargs: Any) -> None:
        global_vars, local_vars = get_caller_global_local_vars()
        variables = self._sql(
            code, self._sql_vars, global_vars, local_vars, *args, **kwargs
        )
        for k, v in variables.items():
            if isinstance(v, WorkflowDataFrame) and v.workflow is self:
                self._sql_vars[k] = v

    def _sql(
        self, code: str, *args: Any, **kwargs: Any
    ) -> Dict[str, Tuple[WorkflowDataFrame, WorkflowDataFrames, LazyWorkflowDataFrame]]:
        FugueSQLParser = import_fsql_dependency("fugue_sql_antlr").FugueSQLParser

        from ._visitors import FugueSQLHooks, _Extensions

        # TODO: move dict construction to triad
        params: Dict[str, Any] = {}
        for a in args:
            assert_or_throw(
                isinstance(a, Dict), lambda: f"args can only have dict: {a}"
            )
            params.update(a)
        params.update(kwargs)
        params, dfs = self._split_params(params)
        code = fill_sql_template(code, params)
        sql = FugueSQLParser(
            code,
            "fugueLanguage",
            ignore_case=self.conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool),
            parse_mode="auto",
        )
        v = _Extensions(
            sql,
            FugueSQLHooks(),
            self,
            dialect=self.conf.get_or_throw(FUGUE_CONF_SQL_DIALECT, str),
            variables=dfs,  # type: ignore
            local_vars=params,
        )
        v.visit(sql.tree)
        return v.variables

    def _split_params(
        self, params: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, LazyWorkflowDataFrame]]:
        p: Dict[str, Any] = {}
        dfs: Dict[str, LazyWorkflowDataFrame] = {}
        for k, v in params.items():
            if isinstance(v, (int, str, float, bool)):
                p[k] = v
            elif isinstance(v, (DataFrame, Yielded)) or is_df(v):
                dfs[k] = LazyWorkflowDataFrame(k, v, self)
            else:
                p[k] = v
        return p, dfs
