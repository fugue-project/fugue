from logging import Logger
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union
from uuid import uuid4

from triad import to_uuid

from fugue._utils.registry import fugue_plugin
from fugue._utils.misc import import_fsql_dependency

_TEMP_TABLE_EXPR_PREFIX = "<tmpdf:"
_TEMP_TABLE_EXPR_SUFFIX = ">"


class TempTableName:
    """Generating a temporary, random and globaly unique table name"""

    def __init__(self):
        self.key = "_" + str(uuid4())[:5]

    def __repr__(self) -> str:
        return _TEMP_TABLE_EXPR_PREFIX + self.key + _TEMP_TABLE_EXPR_SUFFIX


@fugue_plugin
def transpile_sql(
    raw: str, from_dialect: Optional[str], to_dialect: Optional[str]
) -> str:
    """Transpile SQL between dialects, it should work only when both
    ``from_dialect`` and ``to_dialect`` are not None

    :param raw: the raw SQL
    :param from_dialect: the dialect of the raw SQL
    :param to_dialect: the expected dialect.
    :return: the transpiled SQL
    """
    if (
        from_dialect is not None
        and to_dialect is not None
        and from_dialect != to_dialect
    ):
        sqlglot = import_fsql_dependency("sqlglot")

        return " ".join(sqlglot.transpile(raw, read=from_dialect, write=to_dialect))
    else:
        return raw


class StructuredRawSQL:
    """The Raw SQL object containing table references and dialect information.

    :param statements: In each tuple, the first value indicates whether
        the second value is a dataframe name reference (True), or just a part
        of the statement (False)
    :param dialect: the dialect of the statements, defaults to None

    .. note::

        ``dialect`` None means no transpilation will be done when constructing
        the final sql.
    """

    def __init__(
        self, statements: Iterable[Tuple[bool, str]], dialect: Optional[str] = None
    ):
        self._statements = list(statements)
        self._dialect = dialect

    @property
    def dialect(self) -> Optional[str]:
        """The dialect of this query"""
        return self._dialect

    def __uuid__(self) -> str:
        return to_uuid(self._statements, self._dialect)

    def construct(
        self,
        name_map: Union[None, Callable[[str], str], Dict[str, str]] = None,
        dialect: Optional[str] = None,
        log: Optional[Logger] = None,
    ):
        """Construct the final SQL given the ``dialect``

        :param name_map: the name map from the original statement to
            the expected names, defaults to None. It can be a function or a
            dictionary
        :param dialect: the expected dialect, defaults to None
        :param log: the logger to log information, defaults to None
        :return: the final SQL string
        """
        nm: Any = (
            (lambda x: x)
            if name_map is None
            else name_map
            if not isinstance(name_map, dict)
            else (lambda x: name_map.get(x, x))  # type: ignore
        )
        raw_sql = " ".join(nm(tp[1]) if tp[0] else tp[1] for tp in self._statements)
        if (
            self._dialect is not None
            and dialect is not None
            and self._dialect != dialect
        ):
            tsql = transpile_sql(raw_sql, self._dialect, dialect)
            if log is not None:
                log.debug(
                    "SQL transpiled from %s to %s\n\n"
                    "Original:\n\n%s\n\nTranspiled:\n\n%s\n",
                    self._dialect,
                    dialect,
                    raw_sql,
                    tsql,
                )
            return tsql
        return raw_sql

    @staticmethod
    def from_expr(
        sql: str,
        prefix: str = _TEMP_TABLE_EXPR_PREFIX,
        suffix: str = _TEMP_TABLE_EXPR_SUFFIX,
        dialect: Optional[str] = None,
    ) -> "StructuredRawSQL":
        """Parse the ``StructuredRawSQL`` from the ``sql`` expression.
        The sql should look like ``SELECT * FROM <tmpdf:dfname>``. This
        function can identify the tmpdfs with the given syntax, and construct
        the ``StructuredRawSQL``

        :param sql: the SQL expression with ``<tmpdf:?>``
        :param prefix: the prefix of the temp df
        :param suffix: the suffix of the temp df
        :param dialect: the dialect of the sql expression, defaults to None
        :return: the parsed object
        """

        def _get() -> Iterable[Tuple[bool, str]]:
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

        return StructuredRawSQL(_get(), dialect=dialect)
