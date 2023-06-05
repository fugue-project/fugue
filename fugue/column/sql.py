from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import pyarrow as pa
from triad import Schema, assert_or_throw, to_uuid
from triad.utils.pyarrow import _type_to_expression
from triad.utils.schema import quote_name

from fugue.column.expressions import (
    ColumnExpr,
    _BinaryOpExpr,
    _FuncExpr,
    _LiteralColumnExpr,
    _NamedColumnExpr,
    _UnaryOpExpr,
    _WildcardExpr,
    col,
    lit,
)
from fugue.column.functions import is_agg
from fugue.exceptions import FugueBug

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
    """SQL ``SELECT`` columns collection.

    :param cols: collection of :class:`~fugue.column.expressions.ColumnExpr`
    :param arg_distinct: whether this is ``SELECT DISTINCT``, defaults to False

    .. admonition:: New Since
        :class: hint

        **0.6.0**
    """

    def __init__(self, *cols: ColumnExpr, arg_distinct: bool = False):  # noqa: C901
        self._distinct = arg_distinct
        self._all: List[ColumnExpr] = []
        self._literals: List[ColumnExpr] = []
        self._cols: List[ColumnExpr] = []
        self._non_agg_funcs: List[ColumnExpr] = []
        self._agg_funcs: List[ColumnExpr] = []
        self._group_keys: List[ColumnExpr] = []
        self._has_wildcard = False

        _g_keys: List[ColumnExpr] = []

        for c in cols:
            c = c.infer_alias()
            _is_agg = False
            self._all.append(c)
            if isinstance(c, _LiteralColumnExpr):
                self._literals.append(c)
            else:
                if isinstance(c, _NamedColumnExpr):
                    self._cols.append(c)
                elif isinstance(c, _WildcardExpr):
                    if self._has_wildcard:
                        raise ValueError("'*' can be used at most once")
                    self._has_wildcard = True
                    self._cols.append(c)
                elif isinstance(c, _FuncExpr):
                    if is_agg(c):
                        _is_agg = True
                        self._agg_funcs.append(c)
                    else:
                        self._non_agg_funcs.append(c)
                if not _is_agg:
                    if isinstance(c, _WildcardExpr):
                        _g_keys.append(c)
                    else:
                        _g_keys.append(c.alias("").cast(None))

        if self.has_agg:
            self._group_keys += _g_keys

        if len(self._agg_funcs) > 0 and self._has_wildcard:
            raise ValueError(f"'*' can't be used in aggregation: {self}")

    def __str__(self):
        """String representation for debug purpose"""
        expr = ", ".join(str(x) for x in self.all_cols)
        return f"[{expr}]"

    def __uuid__(self):
        """Unique id for this collection"""
        return to_uuid(self._distinct, self.all_cols)

    @property
    def is_distinct(self) -> bool:
        """Whether this is a ``SELECT DISTINCT``"""
        return self._distinct

    def replace_wildcard(self, schema: Schema) -> "SelectColumns":
        """Replace wildcard ``*`` with explicit column names

        :param schema: the schema used to parse the wildcard
        :return: a new instance containing only explicit columns

        .. note::

            It only replaces the top level ``*``. For example
            ``count_distinct(all_cols())`` will not be transformed because
            this ``*`` is not first level.
        """

        def _get_cols() -> Iterable[ColumnExpr]:
            for c in self.all_cols:
                if isinstance(c, _WildcardExpr):
                    yield from [col(n) for n in schema.names]
                else:
                    yield c

        return SelectColumns(*list(_get_cols()))

    def assert_all_with_names(self) -> "SelectColumns":
        """Assert every column have explicit alias or the alias can
        be inferred (non empty value). It will also validate there is
        no duplicated aliases

        :raises ValueError: if there are columns without alias, or there are
          duplicated names.
        :return: the instance itself
        """

        names: Set[str] = set()
        for x in self.all_cols:
            if isinstance(x, _WildcardExpr):
                continue
            if isinstance(x, _NamedColumnExpr):
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
        """Assert there is no ``*`` on first level columns

        :raises AssertionError: if ``all_cols()`` exists
        :return: the instance itself
        """
        assert not self._has_wildcard
        return self

    def assert_no_agg(self) -> "SelectColumns":
        """Assert there is no aggregation operation on any column.

        :raises AssertionError: if there is any aggregation in the
          collection.
        :return: the instance itself

        .. seealso::
            Go to :func:`~fugue.column.functions.is_agg` to see how the
            aggregations are detected.
        """
        assert not self.has_agg
        return self

    @property
    def all_cols(self) -> List[ColumnExpr]:
        """All columns (with inferred aliases)"""
        return self._all

    @property
    def literals(self) -> List[ColumnExpr]:
        """All literal columns"""
        return self._literals

    @property
    def simple_cols(self) -> List[ColumnExpr]:
        """All columns directly representing column names"""
        return self._cols

    @property
    def non_agg_funcs(self) -> List[ColumnExpr]:
        """All columns with non-aggregation operations"""
        return self._non_agg_funcs

    @property
    def agg_funcs(self) -> List[ColumnExpr]:
        """All columns with aggregation operations"""
        return self._agg_funcs

    @property
    def group_keys(self) -> List[ColumnExpr]:
        """Group keys inferred from the columns.

        .. note::

            * if there is no aggregation, the result will be empty
            * it is :meth:`~.simple_cols` plus :meth:`~.non_agg_funcs`
        """
        return self._group_keys

    @property
    def has_agg(self) -> bool:
        """Whether this select is an aggregation"""
        return len(self.agg_funcs) > 0

    @property
    def has_literals(self) -> bool:
        """Whether this select contains literal columns"""
        return len(self.literals) > 0

    @property
    def simple(self) -> bool:
        """Whether this select contains only simple column representations"""
        return len(self.simple_cols) == len(self.all_cols)


class SQLExpressionGenerator:
    """SQL generator for :class:`~.SelectColumns`

    :param enable_cast: whether convert ``cast`` into the statement, defaults to True

    .. admonition:: New Since
        :class: hint

        **0.6.0**
    """

    def __init__(self, enable_cast: bool = True):
        self._enable_cast = enable_cast
        self._func_handler: Dict[str, Callable[[_FuncExpr], Iterable[str]]] = {}

    def where(self, condition: ColumnExpr, table: str) -> Iterable[Tuple[bool, str]]:
        """Generate a ``SELECT *`` statement with the given where clause

        :param condition: column expression for ``WHERE``
        :param table: table name for ``FROM``
        :return: the SQL statement

        :raises ValueError: if ``condition`` contains aggregation

        .. admonition:: Examples

            .. code-block:: python

                gen = SQLExpressionGenerator(enable_cast=False)

                # SELECT * FROM tb WHERE a>1 AND b IS NULL
                gen.where((col("a")>1) & col("b").is_null(), "tb")
        """
        assert_or_throw(
            not is_agg(condition),
            lambda: ValueError(f"{condition} has aggregation functions"),
        )
        cond = self.generate(condition.alias(""))
        yield (False, "SELECT * FROM")
        yield (True, table)
        yield (False, f"WHERE {cond}")

    def select(
        self,
        columns: SelectColumns,
        table: str,
        where: Optional[ColumnExpr] = None,
        having: Optional[ColumnExpr] = None,
    ) -> Iterable[Tuple[bool, str]]:
        """Construct the full ``SELECT`` statement on a single table

        :param columns: columns to select, it may contain aggregations, if
          so, the group keys are inferred.
          See :meth:`~fugue.column.sql.SelectColumns.group_keys`
        :param table: table name to select from
        :param where: ``WHERE`` condition, defaults to None
        :param having: ``HAVING`` condition, defaults to None. It is used
          only when there is aggregation
        :return: the full ``SELECT`` statement
        """
        columns.assert_all_with_names()

        def _where() -> str:
            if where is None:
                return ""
            assert_or_throw(
                not is_agg(where),
                lambda: ValueError(f"{where} has aggregation functions"),
            )
            return "WHERE " + self.generate(where.alias(""))

        def _having(as_where: bool = False) -> str:
            if having is None:
                return ""
            pre = "WHERE " if as_where else "HAVING "
            return pre + self.generate(having.alias(""))

        distinct = "" if not columns.is_distinct else "DISTINCT "

        if not columns.has_agg:
            expr = ", ".join(self.generate(x) for x in columns.all_cols)
            yield (False, f"SELECT {distinct}{expr} FROM")
            yield (True, table)
            yield (False, _where())
            return
        columns.assert_no_wildcard()
        if len(columns.literals) == 0:
            expr = ", ".join(self.generate(x) for x in columns.all_cols)
            if len(columns.group_keys) == 0:
                yield (False, f"SELECT {distinct}{expr} FROM")
                yield (True, table)
                yield (False, _where())
                yield (False, _having())
                return
            else:
                keys = ", ".join(self.generate(x) for x in columns.group_keys)
                yield (False, f"SELECT {distinct}{expr} FROM")
                yield (True, table)
                yield (False, _where())
                yield (False, f"GROUP BY {keys}")
                yield (False, _having())
                return
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
            yield (False, f"SELECT {expr} FROM (")
            yield from sub
            yield (False, ")")

    def generate(self, expr: ColumnExpr) -> str:
        """Convert :class:`~fugue.column.expressions.ColumnExpr` to
        SQL clause

        :param expr: the column expression to convert
        :return: the SQL clause for this expression
        """
        return "".join(self._generate(expr)).strip()

    def add_func_handler(
        self, name: str, handler: Callable[[_FuncExpr], Iterable[str]]
    ) -> "SQLExpressionGenerator":
        """Add special function handler.

        :param name: name of the function
        :param handler: the function to convert the function expression to SQL
          clause
        :return: the instance itself

        .. caution::

            Users should not use this directly
        """
        self._func_handler[name] = handler
        return self

    def correct_select_schema(
        self, input_schema: Schema, select: SelectColumns, output_schema: Schema
    ) -> Optional[Schema]:
        """Do partial schema inference from ``input_schema`` and ``select`` columns,
        then compare with the SQL output dataframe schema, and return the different
        part as a new schema, or None if there is no difference

        :param input_schema: input dataframe schema for the select statement
        :param select: the collection of select columns
        :param output_schema: schema of the output dataframe after executing the SQL
        :return: the difference as a new schema or None if no difference

        .. tip::

            This is particularly useful when the SQL engine messed up the schema of the
            output. For example, ``SELECT *`` should return a dataframe with the same
            schema of the input. However, for example a column ``a:int`` could become
            ``a:long`` in the output dataframe because of information loss. This
            function is designed to make corrections on column types when they can be
            inferred. This may not be perfect but it can solve major discrepancies.
        """
        cols = select.replace_wildcard(input_schema).assert_all_with_names()
        fields: List[pa.Field] = []
        for c in cols.all_cols:
            tp = c.infer_type(input_schema)
            if tp is not None and tp != output_schema[c.output_name].type:
                fields.append(pa.field(c.output_name, tp))
        if len(fields) == 0:
            return None
        return Schema(fields)

    def _generate(  # noqa: C901
        self, expr: ColumnExpr, bracket: bool = False
    ) -> Iterable[str]:
        if self._enable_cast and expr.as_type is not None:
            yield "CAST("
        if isinstance(expr, _LiteralColumnExpr):
            yield from self._on_lit(expr)
        elif isinstance(expr, _NamedColumnExpr):
            yield from self._on_named(expr)
        elif isinstance(expr, _WildcardExpr):
            yield "*"
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
            yield " AS " + quote_name(expr.as_name)
        elif expr.as_type is not None and expr.name != "":
            yield " AS " + quote_name(expr.name)

    def type_to_expr(self, data_type: pa.DataType):
        return _type_to_expression(data_type)

    def _on_named(self, expr: _NamedColumnExpr) -> Iterable[str]:
        yield quote_name(expr.name)

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
        if expr.is_distinct:  # pragma: no cover
            raise FugueBug(f"impossible case {expr}")
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
        if expr.is_distinct:
            yield "DISTINCT "
        yield from args
        yield ")"
