from builtins import isinstance
from typing import Any, Dict, Tuple

from fugue import (
    DataFrame,
    FugueWorkflow,
    WorkflowDataFrame,
    WorkflowDataFrames,
    Yielded,
)
from fugue.workflow import is_acceptable_raw_df
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_caller_global_local_vars

from fugue_sql._constants import (
    FUGUE_SQL_CONF_IGNORE_CASE,
    FUGUE_SQL_CONF_SIMPLE_ASSIGN,
    FUGUE_SQL_DEFAULT_CONF,
)
from fugue_sql._parse import FugueSQL
from fugue_sql._utils import LazyWorkflowDataFrame, fill_sql_template
from fugue_sql._visitors import FugueSQLHooks, _Extensions


class FugueSQLWorkflow(FugueWorkflow):
    """Fugue workflow that supports Fugue SQL. Please read |FugueSQLTutorial|."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._sql_vars: Dict[str, WorkflowDataFrame] = {}
        self._sql_conf = ParamDict({**FUGUE_SQL_DEFAULT_CONF, **super().conf})

    @property
    def conf(self) -> ParamDict:
        return self._sql_conf

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
        # TODO: move dict construction to triad
        params: Dict[str, Any] = {}
        for a in args:
            assert_or_throw(isinstance(a, Dict), f"args can only have dict: {a}")
            params.update(a)
        params.update(kwargs)
        params, dfs = self._split_params(params)
        code = fill_sql_template(code, params)
        sql = FugueSQL(
            code,
            "fugueLanguage",
            ignore_case=self.conf.get_or_throw(FUGUE_SQL_CONF_IGNORE_CASE, bool),
            simple_assign=self.conf.get_or_throw(FUGUE_SQL_CONF_SIMPLE_ASSIGN, bool),
        )
        v = _Extensions(
            sql, FugueSQLHooks(), self, dfs, local_vars=params  # type: ignore
        )
        v.visit(sql.tree)
        return v.variables

    def _split_params(
        self, params: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, LazyWorkflowDataFrame]]:
        p: Dict[str, Any] = {}
        dfs: Dict[str, LazyWorkflowDataFrame] = {}
        for k, v in params.items():
            if isinstance(v, (DataFrame, Yielded)) or is_acceptable_raw_df(v):
                dfs[k] = LazyWorkflowDataFrame(k, v, self)
            else:
                p[k] = v
        return p, dfs


def fsql(sql: str, *args: Any, **kwargs: Any) -> FugueSQLWorkflow:
    global_vars, local_vars = get_caller_global_local_vars()
    dag = FugueSQLWorkflow()
    dag._sql(sql, global_vars, local_vars, *args, **kwargs)
    return dag
