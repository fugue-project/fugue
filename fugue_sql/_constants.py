from typing import Dict, Any

FUGUE_SQL_DEFAULT_CONF: Dict[str, Any] = {
    "fugue.sql.compile.ignore_case": False,
    "fugue.sql.compile.simple_assign": True,
}
