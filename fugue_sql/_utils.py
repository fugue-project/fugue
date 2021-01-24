import re
import jinja2

from jinja2 import Template
from typing import Dict, Any

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
