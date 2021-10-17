from typing import Any, Dict, Set

from fugue.constants import FUGUE_SQL_CONF_IGNORE_CASE

FUGUE_SQL_CONF_SIMPLE_ASSIGN = "fugue.sql.compile.simple_assign"

FUGUE_SQL_DEFAULT_CONF: Dict[str, Any] = {
    FUGUE_SQL_CONF_IGNORE_CASE: False,
    FUGUE_SQL_CONF_SIMPLE_ASSIGN: True,
}

FUGUE_SQL_COMPILE_TIME_CONF_KEYS: Set[str] = {
    FUGUE_SQL_CONF_IGNORE_CASE,
    FUGUE_SQL_CONF_SIMPLE_ASSIGN,
}
