from typing import Dict, Any

FUGUE_SQL_CONF_IGNORE_CASE = "fugue.sql.compile.ignore_case"
FUGUE_SQL_CONF_SIMPLE_ASSIGN = "fugue.sql.compile.simple_assign"

FUGUE_SQL_DEFAULT_CONF: Dict[str, Any] = {
    FUGUE_SQL_CONF_IGNORE_CASE: False,
    FUGUE_SQL_CONF_SIMPLE_ASSIGN: True,
}
