import re

from jinja2 import Template
from typing import Dict, Any

MATCH_QUOTED_STRING = r"([\"'])(.*?[^\\])\1"


def fill_sql_template(sql: str, params: Dict[str, Any]):
    if "self" in params:
        params = {k: v for k, v in params.items() if k != "self"}
    single_quote_pattern = "'{{% raw %}}{}{{% endraw %}}'"
    double_quote_pattern = '"{{% raw %}}{}{{% endraw %}}"'
    sql = re.sub(
        MATCH_QUOTED_STRING,
        lambda pattern: double_quote_pattern.format(pattern.group(2))
        if pattern.group(1) == '"'
        else single_quote_pattern.format(pattern.group(2)),
        sql,
    )
    template = Template(sql)
    return template.render(**params)
