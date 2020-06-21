from jinjasql import JinjaSql
from typing import Dict, Any


def fill_sql_template(sql: str, params: Dict[str, Any]):
    sql = sql.replace("%", "%%")
    query, bind = JinjaSql(param_style="pyformat").prepare_query(sql, params)
    return query % bind
