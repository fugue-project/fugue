from typing import Any, Optional

import pyarrow as pa
from fugue.column.expressions import (
    ColumnExpr,
    _FuncExpr,
    _to_col,
    function,
)
from triad import Schema


def coalesce(*args: Any) -> ColumnExpr:
    return function("COALESCE", *[_to_col(x) for x in args])


def min(col: ColumnExpr) -> ColumnExpr:  # pylint: disable=redefined-builtin
    assert isinstance(col, ColumnExpr)
    return _SameTypeUnaryAggFuncExpr("MIN", col)


def max(col: ColumnExpr) -> ColumnExpr:  # pylint: disable=redefined-builtin
    assert isinstance(col, ColumnExpr)
    return _SameTypeUnaryAggFuncExpr("MAX", col)


def count(col: ColumnExpr) -> ColumnExpr:
    assert isinstance(col, ColumnExpr)
    return _UnaryAggFuncExpr("COUNT", col)


def count_distinct(col: ColumnExpr) -> ColumnExpr:
    assert isinstance(col, ColumnExpr)
    return _UnaryAggFuncExpr("COUNT", col, arg_distinct=True)


def avg(col: ColumnExpr) -> ColumnExpr:
    assert isinstance(col, ColumnExpr)
    return _UnaryAggFuncExpr("AVG", col)


def sum(col: ColumnExpr) -> ColumnExpr:  # pylint: disable=redefined-builtin
    assert isinstance(col, ColumnExpr)
    return _UnaryAggFuncExpr("SUM", col)


def first(col: ColumnExpr) -> ColumnExpr:
    assert isinstance(col, ColumnExpr)
    return _SameTypeUnaryAggFuncExpr("FIRST", col)


def last(col: ColumnExpr) -> ColumnExpr:
    assert isinstance(col, ColumnExpr)
    return _SameTypeUnaryAggFuncExpr("LAST", col)


def is_agg(column: Any) -> bool:
    if isinstance(column, _UnaryAggFuncExpr):
        return True
    if isinstance(column, _FuncExpr):
        return any(is_agg(x) for x in column.args) or any(
            is_agg(x) for x in column.kwargs.values()
        )
    return False


class _UnaryAggFuncExpr(_FuncExpr):
    def __init__(self, func: str, col: ColumnExpr, arg_distinct: bool = False):
        super().__init__(func, col, arg_distinct=arg_distinct)

    def infer_alias(self) -> ColumnExpr:
        return (
            self
            if self.output_name != ""
            else self.alias(self.args[0].infer_alias().output_name)
        )

    def _copy(self) -> _FuncExpr:
        return _UnaryAggFuncExpr(self.func, *self.args, **self.kwargs)


class _SameTypeUnaryAggFuncExpr(_UnaryAggFuncExpr):
    def _copy(self) -> _FuncExpr:
        return _SameTypeUnaryAggFuncExpr(self.func, *self.args, **self.kwargs)

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        return self.as_type or self.args[0].infer_type(schema)
