import inspect
from builtins import isinstance
from typing import Any, Dict, Tuple

from fugue import DataFrame, FugueWorkflow, WorkflowDataFrame, WorkflowDataFrames
from fugue.collections.yielded import Yielded
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_caller_global_local_vars

from fugue_sql._constants import (
    FUGUE_SQL_CONF_IGNORE_CASE,
    FUGUE_SQL_CONF_SIMPLE_ASSIGN,
    FUGUE_SQL_DEFAULT_CONF,
)
from fugue_sql._parse import FugueSQL
from fugue_sql._utils import fill_sql_template
from fugue_sql._visitors import FugueSQLHooks, _Extensions
from fugue_sql.exceptions import FugueSQLError


class FugueSQLWorkflow(FugueWorkflow):
    """Fugue workflow that supports Fugue SQL. Please read |FugueSQLTutorial|."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._sql_vars: Dict[str, WorkflowDataFrame] = {}
        self._sql_conf = ParamDict({**FUGUE_SQL_DEFAULT_CONF, **super().conf})

    @property
    def conf(self) -> ParamDict:
        return self._sql_conf

    def __getitem__(self, key: str) -> WorkflowDataFrame:
        assert_or_throw(key in self._sql_vars, FugueSQLError(f"{key} not found"))
        return self._sql_vars[key]

    def __call__(self, code: str, *args: Any, **kwargs: Any):
        cf = inspect.currentframe()
        global_vars, local_vars = get_caller_global_local_vars()
        global_vars = {
            k: v
            for k, v in global_vars.items()
            if not isinstance(v, WorkflowDataFrame) or v.workflow is self
        }
        local_vars = {
            k: v
            for k, v in local_vars.items()
            if not isinstance(v, WorkflowDataFrame) or v.workflow is self
        }
        variables = self._sql(
            code, self._sql_vars, global_vars, local_vars, *args, **kwargs
        )
        if cf is not None:
            for k, v in variables.items():
                if isinstance(v, WorkflowDataFrame) and v.workflow is self:
                    self._sql_vars[k] = v

    def _sql(
        self, code: str, *args: Any, **kwargs: Any
    ) -> Dict[str, Tuple[WorkflowDataFrame, WorkflowDataFrames]]:
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
        v = _Extensions(sql, FugueSQLHooks(), self, dfs, local_vars=params)
        v.visit(sql.tree)
        return v.variables

    def _split_params(
        self, params: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Tuple[WorkflowDataFrame, WorkflowDataFrames]]]:
        p: Dict[str, Any] = {}
        dfs: Dict[str, Tuple[WorkflowDataFrame, WorkflowDataFrames]] = {}
        for k, v in params.items():
            if isinstance(v, (DataFrame, Yielded)):
                dfs[k] = self.df(v)  # type: ignore
            else:
                p[k] = v
        return p, dfs
