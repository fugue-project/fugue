from typing import Iterable, Tuple
from uuid import uuid4


class TempTableName:
    def __init__(self):
        self.key = "_" + str(uuid4())[:5]

    def __repr__(self) -> str:
        return f"<tmpdf:{self.key}>"


def get_temp_tb_name() -> TempTableName:
    return TempTableName()


def parse_sql(
    sql: str, prefix: str = "<tmpdf:", suffix: str = ">"
) -> Iterable[Tuple[bool, str]]:
    p = 0
    while p < len(sql):
        b = sql.find(prefix, p)
        if b >= 0:
            if b > p:
                yield (False, sql[p:b])
            b += len(prefix)
            e = sql.find(suffix, b)
            yield (True, sql[b:e])
            p = e + len(suffix)
        else:
            yield (False, sql[p:])
            return
