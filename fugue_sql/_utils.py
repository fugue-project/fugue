import re
from typing import Any, Dict, Optional

import jinja2
from fugue import FugueWorkflow, WorkflowDataFrame, Yielded
from jinja2 import Template
from triad import assert_or_throw

from fugue_sql.exceptions import FugueSQLError

MATCH_QUOTED_STRING = r"([\"'])(({|%|})*)\1"


def fill_sql_template(sql: str, params: Dict[str, Any]):
    """Prepare string to be executed, inserts params into sql template
    ---
    :param sql: jinja compatible template
    :param params: params to be inserted into template
    """
    try:
        if "self" in params:
            params = {k: v for k, v in params.items() if k != "self"}
        single_quote_pattern = "'{{% raw %}}{}{{% endraw %}}'"
        double_quote_pattern = '"{{% raw %}}{}{{% endraw %}}"'
        new_sql = re.sub(
            MATCH_QUOTED_STRING,
            lambda pattern: double_quote_pattern.format(pattern.group(2))
            if pattern.group(1) == '"'
            else single_quote_pattern.format(pattern.group(2)),
            sql,
        )

        template = Template(new_sql)

    except jinja2.exceptions.TemplateSyntaxError:

        template = Template(sql)

    return template.render(**params)


class LazyWorkflowDataFrame:
    def __init__(self, key: str, df: Any, workflow: FugueWorkflow):
        self._key = key
        self._df = df
        self._workflow = workflow
        self._wdf: Optional[WorkflowDataFrame] = None

    def get_df(self) -> WorkflowDataFrame:
        if self._wdf is None:
            self._wdf = self._get_df()
        return self._wdf

    def _get_df(self) -> WorkflowDataFrame:
        if isinstance(self._df, Yielded):
            return self._workflow.df(self._df)
        if isinstance(self._df, WorkflowDataFrame):
            assert_or_throw(
                self._df.workflow is self._workflow,
                FugueSQLError(f"{self._key}, {self._df} is from another workflow"),
            )
            return self._df
        return self._workflow.df(self._df)
