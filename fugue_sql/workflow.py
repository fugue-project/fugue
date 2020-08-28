import inspect
from typing import Any, Dict

from fugue.workflow import FugueWorkflow, WorkflowDataFrame
from fugue_sql._utils import fill_sql_template
from fugue_sql._constants import FUGUE_SQL_DEFAULT_CONF
from fugue_sql.exceptions import FugueSQLError
from fugue_sql._parse import FugueSQL
from fugue_sql._visitors import FugueSQLHooks, _Extensions
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_or_throw


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
        local_vars: Dict[str, Any] = {}
        if cf is not None and cf.f_back is not None:
            local_vars = cf.f_back.f_locals
        local_vars = {
            k: v
            for k, v in local_vars.items()
            if not isinstance(v, WorkflowDataFrame) or v.workflow is self
        }
        variables = self._sql(code, self._sql_vars, local_vars, *args, **kwargs)
        if cf is not None:
            for k, v in variables.items():
                if isinstance(v, WorkflowDataFrame) and v.workflow is self:
                    self._sql_vars[k] = v

    def _sql(
        self, code: str, *args: Any, **kwargs: Any
    ) -> Dict[str, WorkflowDataFrame]:
        # TODO: move dict construction to triad
        params: Dict[str, Any] = {}
        for a in args:
            assert_or_throw(isinstance(a, Dict), f"args can only have dict: {a}")
            params.update(a)
        params.update(kwargs)
        code = fill_sql_template(code, params)
        sql = FugueSQL(
            code,
            "fugueLanguage",
            ignore_case=self.conf.get_or_throw("fugue.sql.compile.ignore_case", bool),
            simple_assign=self.conf.get_or_throw(
                "fugue.sql.compile.simple_assign", bool
            ),
        )
        dfs = {k: v for k, v in params.items() if isinstance(v, WorkflowDataFrame)}
        v = _Extensions(sql, FugueSQLHooks(), self, dfs)
        v.visit(sql.tree)
        return v.variables
