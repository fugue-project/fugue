from typing import Any, Dict, List

from triad import assert_or_throw


class ColumnExpr:
    def __init__(self):
        self._distinct = False
        self._as_name = ""

    @property
    def as_name(self) -> str:
        return self._as_name

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


def lit(obj: Any) -> ColumnExpr:
    return _LiteralColumnExpr(obj)


def null() -> ColumnExpr:
    return lit(None)


def col(obj: Any) -> ColumnExpr:
    if isinstance(obj, ColumnExpr):
        return obj
    if isinstance(obj, str):
        return _NamedColumnExpr(obj)
    raise NotImplementedError(obj)


def function(name: str, *args: Any, **kwargs) -> ColumnExpr:
    return _FuncExpr(name, *args, **kwargs)


def coalesce(*args: Any) -> ColumnExpr:
    return _FuncExpr("COALESCE", *[_to_col(x) for x in args])


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
        if isinstance(self.value, str):
            return f'"{self.value}"'
        if isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
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
                return f'"{v}"'
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
        other = _FuncExpr(self.func, *self.args, **self.kwargs)
        other._distinct = True
        other._as_name = self.as_name
        return other

    def alias(self, as_name: str) -> ColumnExpr:
        other = _FuncExpr(self.func, *self.args, **self.kwargs)
        other._as_name = as_name
        other._distinct = self.is_distinct
        return other


class _UnaryOpExpr(_FuncExpr):
    def __init__(self, op: str, column: ColumnExpr):
        super().__init__(op, column)

    @property
    def col(self) -> ColumnExpr:
        return self.args[0]

    @property
    def op(self) -> str:
        return self.func

    def distinct(self) -> ColumnExpr:
        other = _UnaryOpExpr(self.op, self.col)
        other._distinct = True
        other._as_name = self.as_name
        return other

    def alias(self, as_name: str) -> ColumnExpr:
        other = _UnaryOpExpr(self.op, self.col)
        other._as_name = as_name
        other._distinct = self.is_distinct
        return other


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

    def distinct(self) -> ColumnExpr:
        other = _BinaryOpExpr(self.op, self.left, self.right)
        other._distinct = True
        other._as_name = self.as_name
        return other

    def alias(self, as_name: str) -> ColumnExpr:
        other = _BinaryOpExpr(self.op, self.left, self.right)
        other._as_name = as_name
        other._distinct = self.is_distinct
        return other
