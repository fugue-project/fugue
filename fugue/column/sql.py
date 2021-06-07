from typing import Any, Callable, Dict, Iterable, Optional

from fugue.column.expressions import (
    ColumnExpr,
    SelectColumns,
    _BinaryOpExpr,
    _FuncExpr,
    _LiteralColumnExpr,
    _NamedColumnExpr,
    _UnaryOpExpr,
    lit,
    _is_agg,
    _get_column_mentions,
)
from triad import assert_or_throw

_SUPPORTED_OPERATORS: Dict[str, str] = {
    "+": "+",
    "-": "-",
    "*": "*",
    "/": "/",
    "&": " AND ",
    "|": " OR ",
    "<": "<",
    ">": ">",
    "<=": "<=",
    ">=": ">=",
    "==": "=",
    "!=": "!=",
}


class SQLExpressionGenerator:
    def __init__(self):
        self._func_handler: Dict[str, Callable[[_FuncExpr], Iterable[str]]] = {}

    def where(self, condition: ColumnExpr, table: str) -> str:
        assert_or_throw(
            not _is_agg(condition),
            lambda: ValueError(f"{condition} has aggregation functions"),
        )
        cond = self.generate(condition.alias(""))
        return f"SELECT * FROM {table} WHERE {cond}"

    def select(
        self,
        columns: SelectColumns,
        table: str,
        where: Optional[ColumnExpr] = None,
        having: Optional[ColumnExpr] = None,
    ) -> str:
        columns.assert_all_with_names()

        def _where() -> str:
            if where is None:
                return ""
            assert_or_throw(
                not _is_agg(where),
                lambda: ValueError(f"{where} has aggregation functions"),
            )
            return " WHERE " + self.generate(where.alias(""))

        def _having(as_where: bool = False) -> str:
            if having is None:
                return ""
            assert_or_throw(
                not _is_agg(having),
                lambda: ValueError(f"{where} has aggregation functions"),
            )
            names = set(c.output_name for c in columns.all_cols)
            diff = set(_get_column_mentions(having)).difference(names)
            if len(diff) > 0:
                raise ValueError(f"{diff} are in HAVING but not in SELECT")
            pre = " WHERE " if as_where else " HAVING "
            return pre + self.generate(having.alias(""))

        if not columns.has_agg:
            expr = ", ".join(self.generate(x) for x in columns.all_cols)
            return f"SELECT {expr} FROM {table}{_where()}"
        columns.assert_no_wildcard()
        if len(columns.literals) == 0:
            expr = ", ".join(self.generate(x) for x in columns.all_cols)
            if len(columns.group_keys) == 0:
                return f"SELECT {expr} FROM {table}{_where()}{_having()}"
            else:
                keys = ", ".join(self.generate(x) for x in columns.group_keys)
                return (
                    f"SELECT {expr} FROM {table}{_where()} GROUP BY {keys}{_having()}"
                )
        else:
            no_lit = [
                x for x in columns.all_cols if not isinstance(x, _LiteralColumnExpr)
            ]
            sub = self.select(SelectColumns(*no_lit), table, where=where)
            names = [
                self.generate(x) if isinstance(x, _LiteralColumnExpr) else x.output_name
                for x in columns.all_cols
            ]
            expr = ", ".join(names)
            return f"SELECT {expr} FROM ({sub}){_having(as_where=True)}"

    def generate(self, expr: ColumnExpr) -> str:
        return "".join(self._generate(expr)).strip()

    def add_func_handler(
        self, name: str, handler: Callable[[_FuncExpr], Iterable[str]]
    ) -> "SQLExpressionGenerator":
        self._func_handler[name] = handler
        return self

    def _generate(self, expr: ColumnExpr, bracket: bool = False) -> Iterable[str]:
        if expr.is_distinct:
            yield "DISTINCT "
        if isinstance(expr, _LiteralColumnExpr):
            yield from self._on_lit(expr)
        elif isinstance(expr, _NamedColumnExpr):
            yield from self._on_named(expr)
        elif isinstance(expr, _FuncExpr):
            if expr.func in self._func_handler:
                yield from self._func_handler[expr.func](expr)
            elif isinstance(expr, _UnaryOpExpr):
                yield from self._on_common_unary(expr)
            elif isinstance(expr, _BinaryOpExpr):
                yield from self._on_common_binary(expr, bracket)
            else:
                yield from self._on_common_func(expr)
        if expr.as_name != "":
            yield " AS " + expr.as_name

    def _on_named(self, expr: _NamedColumnExpr) -> Iterable[str]:
        yield expr.name

    def _on_lit(self, expr: _LiteralColumnExpr) -> Iterable[str]:
        yield expr.body_str

    def _on_common_unary(self, expr: _UnaryOpExpr) -> Iterable[str]:
        if expr.op == "-":
            yield expr.op
            yield from self._generate(expr.col, bracket=True)
        elif expr.op == "~":
            yield "NOT "
            yield from self._generate(expr.col, bracket=True)
        elif expr.op == "IS_NULL":
            yield from self._generate(expr.col, bracket=True)
            yield " IS NULL"
        elif expr.op == "NOT_NULL":
            yield from self._generate(expr.col, bracket=True)
            yield " IS NOT NULL"
        else:
            raise NotImplementedError(expr)  # pragma: no cover

    def _on_common_binary(self, expr: _BinaryOpExpr, bracket: bool) -> Iterable[str]:
        assert_or_throw(expr.op in _SUPPORTED_OPERATORS, NotImplementedError(expr))
        if bracket:
            yield "("
        yield from self._generate(expr.left, bracket=True)
        yield _SUPPORTED_OPERATORS[expr.op]
        yield from self._generate(expr.right, bracket=True)
        if bracket:
            yield ")"

    def _on_common_func(self, expr: _FuncExpr) -> Iterable[str]:
        def to_str(v: Any) -> Iterable[str]:
            if isinstance(v, ColumnExpr):
                yield from self._generate(v)
            else:
                yield from self._generate(lit(v))

        def get_args() -> Iterable[str]:
            for x in expr.args:
                yield from to_str(x)
                yield ","
            for k, v in expr.kwargs.items():
                yield k
                yield "="
                yield from to_str(v)
                yield ","

        args = list(get_args())
        if len(args) > 0:
            args = args[:-1]
        yield expr.func
        yield "("
        yield from args
        yield ")"
