from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa
from triad import Schema, assert_or_throw, to_uuid
from triad.utils.pyarrow import _type_to_expression, to_pa_datatype


class ColumnExpr:
    def __init__(self):
        self._as_name = ""
        self._as_type: Optional[pa.DataType] = None

    @property
    def name(self) -> str:
        return ""

    @property
    def as_name(self) -> str:
        return self._as_name

    @property
    def as_type(self) -> Optional[pa.DataType]:
        return self._as_type

    @property
    def output_name(self) -> str:
        return self.as_name if self.as_name != "" else self.name

    def alias(self, as_name: str) -> "ColumnExpr":  # pragma: no cover
        raise NotImplementedError

    def infer_alias(self) -> "ColumnExpr":
        return self

    def cast(self, data_type: Any) -> "ColumnExpr":  # pragma: no cover
        raise NotImplementedError

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        return self.as_type  # pragma: no cover

    def __str__(self) -> str:
        res = self.body_str
        if self.as_type is not None:
            res = f"CAST({res} AS {_type_to_expression(self.as_type)})"
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
        return _InvertOpExpr("-", self)

    def __pos__(self) -> "ColumnExpr":
        return self

    def __invert__(self) -> "ColumnExpr":
        return _NotOpExpr("~", self)

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
        return _BoolBinaryOpExpr("&", self, other)

    def __rand__(self, other: Any) -> "ColumnExpr":
        return _BoolBinaryOpExpr("&", other, self)

    def __or__(self, other: Any) -> "ColumnExpr":
        return _BoolBinaryOpExpr("|", self, other)

    def __ror__(self, other: Any) -> "ColumnExpr":
        return _BoolBinaryOpExpr("|", other, self)

    def __lt__(self, other: Any) -> "ColumnExpr":
        return _BoolBinaryOpExpr("<", self, other)

    def __gt__(self, other: Any) -> "ColumnExpr":
        return _BoolBinaryOpExpr(">", self, other)

    def __le__(self, other: Any) -> "ColumnExpr":
        return _BoolBinaryOpExpr("<=", self, other)

    def __ge__(self, other: Any) -> "ColumnExpr":
        return _BoolBinaryOpExpr(">=", self, other)

    def __eq__(self, other: Any) -> "ColumnExpr":  # type: ignore
        return _BoolBinaryOpExpr("==", self, other)

    def __ne__(self, other: Any) -> "ColumnExpr":  # type: ignore
        return _BoolBinaryOpExpr("!=", self, other)

    def __uuid__(self) -> str:
        return to_uuid(
            str(type(self)),
            self.as_name,
            self.as_type,
            *self._uuid_keys(),
        )

    def _uuid_keys(self) -> List[Any]:  # pragma: no cover
        raise NotImplementedError


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


def function(name: str, *args: Any, arg_distinct: bool = False, **kwargs) -> ColumnExpr:
    return _FuncExpr(name, *args, arg_distinct=arg_distinct, **kwargs)


def _get_column_mentions(column: ColumnExpr) -> Iterable[str]:
    if isinstance(column, _NamedColumnExpr):
        yield column.name
    elif isinstance(column, _FuncExpr):
        for a in column.args:
            yield from _get_column_mentions(a)
        for a in column.kwargs.values():
            yield from _get_column_mentions(a)


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

    def alias(self, as_name: str) -> ColumnExpr:
        if self.wildcard and as_name != "":
            raise ValueError("'*' can't have alias")
        other = _NamedColumnExpr(self.name)
        other._as_name = as_name
        other._as_type = self.as_type
        return other

    def cast(self, data_type: Any) -> "ColumnExpr":
        if self.wildcard and data_type is not None:
            raise ValueError("'*' can't cast")
        other = _NamedColumnExpr(self.name)
        other._as_name = self.as_name
        other._as_type = None if data_type is None else to_pa_datatype(data_type)
        return other

    def infer_alias(self) -> ColumnExpr:
        if self.as_name == "" and self.as_type is not None:
            return self.alias(self.output_name)
        return self

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        if self.name not in schema:
            return self.as_type
        return self.as_type or schema[self.name].type

    def _uuid_keys(self) -> List[Any]:
        return [self.name]


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

    def alias(self, as_name: str) -> ColumnExpr:
        other = _LiteralColumnExpr(self.value)
        other._as_name = as_name
        other._as_type = self.as_type
        return other

    def cast(self, data_type: Any) -> ColumnExpr:
        other = _LiteralColumnExpr(self.value)
        other._as_name = self.as_name
        other._as_type = None if data_type is None else to_pa_datatype(data_type)
        return other

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        if self.value is None:
            return self.as_type
        return self.as_type or to_pa_datatype(type(self.value))

    def _uuid_keys(self) -> List[Any]:
        return [self.value]


class _FuncExpr(ColumnExpr):
    def __init__(
        self,
        func: str,
        *args: Any,
        arg_distinct: bool = False,
        **kwargs: Any,
    ):
        self._distinct = arg_distinct
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
        distinct = "DISTINCT " if self.is_distinct else ""
        return f"{self.func}({distinct}{args})"

    @property
    def func(self) -> str:
        return self._func

    @property
    def is_distinct(self) -> bool:
        return self._distinct

    @property
    def args(self) -> List[Any]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    def alias(self, as_name: str) -> ColumnExpr:
        other = self._copy()
        other._as_name = as_name
        other._distinct = self.is_distinct
        other._as_type = self.as_type
        return other

    def cast(self, data_type: Any) -> ColumnExpr:
        other = self._copy()
        other._as_name = self.as_name
        other._distinct = self.is_distinct
        other._as_type = None if data_type is None else to_pa_datatype(data_type)
        return other

    def _copy(self) -> "_FuncExpr":
        return _FuncExpr(self.func, *self.args, **self.kwargs)

    def _uuid_keys(self) -> List[Any]:
        return [self.func, self.is_distinct, self.args, self.kwargs]


class _UnaryOpExpr(_FuncExpr):
    def __init__(self, op: str, column: ColumnExpr, arg_distinct: bool = False):
        super().__init__(op, column, arg_distinct=arg_distinct)

    @property
    def col(self) -> ColumnExpr:
        return self.args[0]

    @property
    def op(self) -> str:
        return self.func

    def infer_alias(self) -> ColumnExpr:
        return self if self.output_name != "" else self.alias(self.col.output_name)

    def _copy(self) -> _FuncExpr:
        return _UnaryOpExpr(self.op, self.col)


class _InvertOpExpr(_UnaryOpExpr):
    def _copy(self) -> _FuncExpr:
        return _InvertOpExpr(self.op, self.col)

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        if self.as_type is not None:
            return self.as_type
        tp = self.col.infer_type(schema)
        if pa.types.is_signed_integer(tp) or pa.types.is_floating(tp):
            return tp
        return None


class _NotOpExpr(_UnaryOpExpr):
    def _copy(self) -> _FuncExpr:
        return _NotOpExpr(self.op, self.col)

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        if self.as_type is not None:
            return self.as_type
        tp = self.col.infer_type(schema)
        if pa.types.is_boolean(tp):
            return tp
        return None


class _BinaryOpExpr(_FuncExpr):
    def __init__(self, op: str, left: Any, right: Any, arg_distinct: bool = False):
        super().__init__(op, _to_col(left), _to_col(right), arg_distinct=arg_distinct)

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


class _BoolBinaryOpExpr(_BinaryOpExpr):
    def _copy(self) -> _FuncExpr:
        return _BoolBinaryOpExpr(self.op, self.left, self.right)

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        return self.as_type or pa.bool_()
