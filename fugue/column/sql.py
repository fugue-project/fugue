from typing import Any, Callable, Dict, Iterable, List, Optional, Set

import pyarrow as pa
from fugue.column.expressions import (
    ColumnExpr,
    _BinaryOpExpr,
    _FuncExpr,
    _LiteralColumnExpr,
    _NamedColumnExpr,
    _UnaryOpExpr,
    lit,
    col,
)
from fugue.column.functions import is_agg
from triad import Schema, assert_or_throw
from triad.utils.pyarrow import _type_to_expression

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


class SelectColumns:
    def __init__(self, *cols: ColumnExpr):  # noqa: C901
        self._all: List[ColumnExpr] = []
        self._literals: List[ColumnExpr] = []
        self._cols: List[ColumnExpr] = []
        self._non_agg_funcs: List[ColumnExpr] = []
        self._agg_funcs: List[ColumnExpr] = []
        self._group_keys: List[ColumnExpr] = []
        self._has_wildcard = False

        _g_keys: List[ColumnExpr] = []

        for c in cols:
            _is_agg = False
            self._all.append(c)
            if isinstance(c, _LiteralColumnExpr):
                self._literals.append(c)
            else:
                if isinstance(c, _NamedColumnExpr):
                    self._cols.append(c)
                    if c.wildcard:
                        if self._has_wildcard:
                            raise ValueError("'*' can be used at most once")
                        self._has_wildcard = True
                elif isinstance(c, _FuncExpr):
                    if is_agg(c):
                        _is_agg = True
                        self._agg_funcs.append(c)
                    else:
                        self._non_agg_funcs.append(c)
                if not _is_agg:
                    _g_keys.append(c.alias("").cast(None))

        if self.has_agg:
            for c in _g_keys:
                if c.is_distinct:
                    raise ValueError(f"can't use {c} as a group key")
                self._group_keys.append(c)

        if len(self._agg_funcs) > 0 and self._has_wildcard:
            raise ValueError(f"'*' can't be used in aggregation: {self}")

    def __str__(self):
        expr = ", ".join(str(x) for x in self.all_cols)
        return f"[{expr}]"

    def replace_wildcard(self, schema: Schema) -> "SelectColumns":
        def _get_cols() -> Iterable[ColumnExpr]:
            for c in self.all_cols:
                if isinstance(c, _NamedColumnExpr) and c.wildcard:
                    yield from [col(n) for n in schema.names]
                else:
                    yield c

        return SelectColumns(*list(_get_cols()))

    def assert_all_with_names(self) -> "SelectColumns":
        names: Set[str] = set()
        for x in self.all_cols:
            if isinstance(x, _NamedColumnExpr):
                if x.wildcard:
                    continue
                if self._has_wildcard:
                    if x.as_name == "":
                        raise ValueError(
                            f"with '*', all other columns must have an alias: {self}"
                        )
            if x.output_name == "":
                raise ValueError(f"{x} does not have an alias: {self}")
            if x.output_name in names:
                raise ValueError(f"{x} can't be reused in select: {self}")
            names.add(x.output_name)

        return self

    def assert_no_wildcard(self) -> "SelectColumns":
        assert not self._has_wildcard
        return self

    def assert_no_agg(self) -> "SelectColumns":
        assert not self.has_agg
        return self

    @property
    def all_cols(self) -> List[ColumnExpr]:
        return self._all

    @property
    def literals(self) -> List[ColumnExpr]:
        return self._literals

    @property
    def simple_cols(self) -> List[ColumnExpr]:
        return self._cols

    @property
    def non_agg_funcs(self) -> List[ColumnExpr]:
        return self._non_agg_funcs

    @property
    def agg_funcs(self) -> List[ColumnExpr]:
        return self._agg_funcs

    @property
    def group_keys(self) -> List[ColumnExpr]:
        return self._group_keys

    @property
    def has_agg(self) -> bool:
        return len(self.agg_funcs) > 0

    @property
    def has_literals(self) -> bool:
        return len(self.literals) > 0

    @property
    def simple(self) -> bool:
        return len(self.simple_cols) == len(self.all_cols)


class SQLExpressionGenerator:
    def __init__(self, enable_cast: bool = True):
        self._enable_cast = enable_cast
        self._func_handler: Dict[str, Callable[[_FuncExpr], Iterable[str]]] = {}

    def where(self, condition: ColumnExpr, table: str) -> str:
        assert_or_throw(
            not is_agg(condition),
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
                not is_agg(where),
                lambda: ValueError(f"{where} has aggregation functions"),
            )
            return " WHERE " + self.generate(where.alias(""))

        def _having(as_where: bool = False) -> str:
            if having is None:
                return ""
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
            sub = self.select(SelectColumns(*no_lit), table, where=where, having=having)
            names = [
                self.generate(x) if isinstance(x, _LiteralColumnExpr) else x.output_name
                for x in columns.all_cols
            ]
            expr = ", ".join(names)
            return f"SELECT {expr} FROM ({sub})"

    def generate(self, expr: ColumnExpr) -> str:
        return "".join(self._generate(expr)).strip()

    def add_func_handler(
        self, name: str, handler: Callable[[_FuncExpr], Iterable[str]]
    ) -> "SQLExpressionGenerator":
        self._func_handler[name] = handler
        return self

    def correct_select_schema(
        self, input_schema: Schema, select: SelectColumns, output_schema: Schema
    ) -> Optional[Schema]:
        cols = select.replace_wildcard(input_schema).assert_all_with_names()
        fields: List[pa.Field] = []
        for c in cols.all_cols:
            tp = c.infer_schema(input_schema)
            if tp is not None and tp != output_schema[c.output_name].type:
                fields.append(pa.field(c.output_name, tp))
        if len(fields) == 0:
            return None
        return Schema(fields)

    def _generate(  # noqa: C901
        self, expr: ColumnExpr, bracket: bool = False
    ) -> Iterable[str]:
        if expr.is_distinct:
            yield "DISTINCT "
        if self._enable_cast and expr.as_type is not None:
            yield "CAST("
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
        if self._enable_cast and expr.as_type is not None:
            yield " AS "
            yield self.type_to_expr(expr.as_type)
            yield ")"
        if expr.as_name != "":
            yield " AS " + expr.as_name
        elif expr.as_type is not None and expr.name != "":
            yield " AS " + expr.name

    def type_to_expr(self, data_type: pa.DataType):
        return _type_to_expression(data_type)

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
