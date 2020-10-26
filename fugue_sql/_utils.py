import re

from jinja2 import Template
from typing import Dict, Any


def fill_sql_template(sql: str, params: Dict[str, Any]):
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == '"':
            sql = sql[:i] + re.sub(
                r"\"(.+?)\"", r'"{% raw %}\1{% endraw %}"', sql[i:], 1
            )
            i = sql.find('"', i + 1) + 1
        elif ch == "'":
            sql = sql[:i] + re.sub(
                r"\'(.+?)\'", r"'{% raw %}\1{% endraw %}'", sql[i:], 1
            )
            i = sql.find("'", i + 1) + 1
        else:
            i += 1
    template = Template(sql)
    return template.render(**params)
