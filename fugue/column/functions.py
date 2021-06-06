from typing import Any

from fugue.column.expressions import ColumnExpr, _to_col, function, agg


def coalesce(*args: Any) -> ColumnExpr:
    return function("COALESCE", *[_to_col(x) for x in args])


def min(col: ColumnExpr):  # pylint: disable=redefined-builtin
    assert isinstance(col, ColumnExpr)
    return agg("MIN", col)


def max(col: ColumnExpr):  # pylint: disable=redefined-builtin
    assert isinstance(col, ColumnExpr)
    return agg("MAX", col)


def avg(col: ColumnExpr):
    assert isinstance(col, ColumnExpr)
    return agg("AVG", col)


def first(col: ColumnExpr):
    assert isinstance(col, ColumnExpr)
    return agg("FIRST", col)


def last(col: ColumnExpr):
    return agg("LAST", col)
