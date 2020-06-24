from jinja2 import Template
from typing import Dict, Any


def fill_sql_template(sql: str, params: Dict[str, Any]):
    template = Template(sql)
    return template.render(**params)
