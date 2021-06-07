from typing import Any, Dict, List, Set

from triad import assert_or_throw


class ColumnExpr:
    def __init__(self):
        self._distinct = False
        self._as_name = ""

    @property
    def name(self) -> str:
        return ""

    @property
    def as_name(self) -> str:
        return self._as_name

    @property
    def output_name(self) -> str:
        return self.as_name if self.as_name != "" else self.name

    @property
    def is_distinct(self) -> bool:
        return self._distinct

    def distinct(self) -> "ColumnExpr":  # pragma: no cover
        raise NotImplementedError

    def alias(self, as_name: str) -> "ColumnExpr":  # pragma: no cover
        raise NotImplementedError

    def __str__(self) -> str:
        res = self.body_str
        if self.is_distinct:
            res = f"DISTINCT {res}"
        if self.as_name != "":
            res = res + " AS " + self.as_name
        return res

    @property
    def body_str(self) -> str:  # pragma: no cover
        raise NotImplementedError

    def is_null(self) -> "ColumnExpr":
        return _UnaryOpExpr("IS_NULL", self)

    def not_null(self) -> "ColumnExpr":
        return _UnaryOpExpr("NOT_NULL", self)

    def __neg__(self) -> "ColumnExpr":
        return _UnaryOpExpr("-", self)

    def __pos__(self) -> "ColumnExpr":
        return self

    def __invert__(self) -> "ColumnExpr":
        return _UnaryOpExpr("~", self)

    def __add__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("+", self, other)

    def __radd__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("+", other, self)

    def __sub__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("-", self, other)

    def __rsub__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("-", other, self)

    def __mul__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("*", self, other)

    def __rmul__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("*", other, self)

    def __truediv__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("/", self, other)

    def __rtruediv__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("/", other, self)

    def __and__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("&", self, other)

    def __rand__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("&", other, self)

    def __or__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("|", self, other)

    def __ror__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("|", other, self)

    def __lt__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("<", self, other)

    def __gt__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr(">", self, other)

    def __le__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr("<=", self, other)

    def __ge__(self, other: Any) -> "ColumnExpr":
        return _BinaryOpExpr(">=", self, other)

    def __eq__(self, other: Any) -> "ColumnExpr":  # type: ignore
        return _BinaryOpExpr("==", self, other)

    def __ne__(self, other: Any) -> "ColumnExpr":  # type: ignore
        return _BinaryOpExpr("!=", self, other)


class SelectColumns:
    def __init__(self, *cols: ColumnExpr):
        self._all: List[ColumnExpr] = []
        self._literals: List[ColumnExpr] = []
        self._cols: List[ColumnExpr] = []
        self._non_agg_funcs: List[ColumnExpr] = []
        self._agg_funcs: List[ColumnExpr] = []
        self._group_keys: List[ColumnExpr] = []
        self._has_wildcard = False

        for c in cols:
            is_agg = False
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
                    if _is_agg(c):
                        is_agg = True
                        self._agg_funcs.append(c)
                    else:
                        self._non_agg_funcs.append(c)
                if not is_agg:
                    self._group_keys.append(c.alias(""))

        if len(self._agg_funcs) > 0 and self._has_wildcard:
            raise ValueError(f"'*' can't be used in aggregation: {self}")

    def __str__(self):
        expr = ", ".join(str(x) for x in self.all_cols)
        return f"[{expr}]"

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


def lit(obj: Any, alias: str = "") -> ColumnExpr:
    return (
        _LiteralColumnExpr(obj) if alias == "" else _LiteralColumnExpr(obj).alias(alias)
    )


def null() -> ColumnExpr:
    return lit(None)


def col(obj: Any, alias: str = "") -> ColumnExpr:
    if isinstance(obj, ColumnExpr):
        return obj if alias == "" else obj.alias(alias)
    if isinstance(obj, str):
        return (
            _NamedColumnExpr(obj) if alias == "" else _NamedColumnExpr(obj).alias(alias)
        )
    raise NotImplementedError(obj)


def function(name: str, *args: Any, **kwargs) -> ColumnExpr:
    return _FuncExpr(name, *args, **kwargs)


def agg(name: str, column: ColumnExpr, *args: Any, **kwargs) -> ColumnExpr:
    return _UnaryAggFuncExpr(name, column, *args, **kwargs)


def _is_agg(column: Any) -> bool:
    if isinstance(column, _UnaryAggFuncExpr):
        return True
    if isinstance(column, _FuncExpr):
        return any(_is_agg(x) for x in column.args) or any(
            _is_agg(x) for x in column.kwargs.values()
        )
    return False


def _to_col(obj: Any) -> ColumnExpr:
    if isinstance(obj, ColumnExpr):
        return obj
    return lit(obj)


class _NamedColumnExpr(ColumnExpr):
    def __init__(self, name: Any):
        self._name = name
        super().__init__()

    @property
    def body_str(self) -> str:
        return self.name

    @property
    def name(self) -> str:
        return self._name

    @property
    def wildcard(self) -> bool:
        return self.name == "*"

    def distinct(self) -> ColumnExpr:
        other = _NamedColumnExpr(self.name)
        other._distinct = True
        other._as_name = self.as_name
        return other

    def alias(self, as_name: str) -> ColumnExpr:
        other = _NamedColumnExpr(self.name)
        other._as_name = as_name
        other._distinct = self.is_distinct
        return other


class _LiteralColumnExpr(ColumnExpr):
    _VALID_TYPES = (int, bool, float, str)

    def __init__(self, value: Any):
        assert_or_throw(
            value is None or isinstance(value, _LiteralColumnExpr._VALID_TYPES),
            lambda: NotImplementedError(f"{value}, type: {type(value)}"),
        )
        self._value = value
        super().__init__()

    @property
    def body_str(self) -> str:
        if self.value is None:
            return "NULL"
        elif isinstance(self.value, str):
            body = self.value.translate(
                str.maketrans(
                    {  # type: ignore
                        "\\": r"\\",
                        "'": r"\'",
                    }
                )
            )
            return f"'{body}'"
        elif isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        else:
            return str(self.value)

    @property
    def value(self) -> Any:
        return self._value

    def is_null(self) -> ColumnExpr:
        return _LiteralColumnExpr(self.value is None)

    def not_null(self) -> ColumnExpr:
        return _LiteralColumnExpr(self.value is not None)

    def distinct(self) -> ColumnExpr:
        return self

    def alias(self, as_name: str) -> ColumnExpr:
        other = _LiteralColumnExpr(self.value)
        other._as_name = as_name
        return other


class _FuncExpr(ColumnExpr):
    def __init__(self, func: str, *args: Any, **kwargs: Any):
        self._func = func
        self._args = list(args)
        self._kwargs = dict(kwargs)
        super().__init__()

    @property
    def body_str(self) -> str:
        def to_str(v: Any):
            if isinstance(v, str):
                return f"'{v}'"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            return str(v)

        a1 = [to_str(x) for x in self.args]
        a2 = [k + "=" + to_str(v) for k, v in self.kwargs.items()]
        args = ",".join(a1 + a2)
        return f"{self.func}({args})"

    @property
    def func(self) -> str:
        return self._func

    @property
    def args(self) -> List[Any]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    def distinct(self) -> ColumnExpr:
        other = self._copy()
        other._distinct = True
        other._as_name = self.as_name
        return other

    def alias(self, as_name: str) -> ColumnExpr:
        other = self._copy()
        other._as_name = as_name
        other._distinct = self.is_distinct
        return other

    def _copy(self) -> "_FuncExpr":
        return _FuncExpr(self.func, *self.args, **self.kwargs)


class _UnaryOpExpr(_FuncExpr):
    def __init__(self, op: str, column: ColumnExpr, *args: Any, **kwargs: Any):
        super().__init__(op, column, *args, **kwargs)

    @property
    def col(self) -> ColumnExpr:
        return self.args[0]

    @property
    def op(self) -> str:
        return self.func

    def _copy(self) -> _FuncExpr:
        return _UnaryOpExpr(self.op, self.col)


class _UnaryAggFuncExpr(_FuncExpr):
    def _copy(self) -> _FuncExpr:
        return _UnaryAggFuncExpr(self.func, *self.args, **self.kwargs)


class _BinaryOpExpr(_FuncExpr):
    def __init__(self, op: str, left: Any, right: Any):
        super().__init__(op, _to_col(left), _to_col(right))

    @property
    def left(self) -> ColumnExpr:
        return self.args[0]

    @property
    def right(self) -> ColumnExpr:
        return self.args[1]

    @property
    def op(self) -> str:
        return self.func

    def _copy(self) -> _FuncExpr:
        return _BinaryOpExpr(self.op, self.left, self.right)
