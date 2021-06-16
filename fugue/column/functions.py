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
    """SQL ``COALESCE`` function

    :param args: If a value is not :class:`~fugue.column.expressions.ColumnExpr`
      then it's converted to a literal column by
      :func:`~fugue.column.expressions.col`

    .. note::

        this function can infer neither type nor alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            f.coalesce(col("a"), col("b")+col("c"), 1)
    """
    return function("COALESCE", *[_to_col(x) for x in args])


def min(col: ColumnExpr) -> ColumnExpr:  # pylint: disable=redefined-builtin
    """SQL ``MIN`` function (aggregation)

    :param col: the column to find min

    .. note::

        * this function can infer type from ``col`` type
        * this function can infer alias from ``col``'s inferred alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            # assume col a has type double
            f.min(col("a"))  # CAST(MIN(a) AS double) AS a
            f.min(-col("a"))  # CAST(MIN(-a) AS double) AS a

            # neither type nor alias can be inferred in the following cases
            f.min(col("a")+1)
            f.min(col("a")+col("b"))

            # you can specify explicitly
            # CAST(MIN(a+b) AS int) AS x
            f.min(col("a")+col("b")).cast(int).alias("x")
    """
    assert isinstance(col, ColumnExpr)
    return _SameTypeUnaryAggFuncExpr("MIN", col)


def max(col: ColumnExpr) -> ColumnExpr:  # pylint: disable=redefined-builtin
    """SQL ``MAX`` function (aggregation)

    :param col: the column to find max

    .. note::

        * this function can infer type from ``col`` type
        * this function can infer alias from ``col``'s inferred alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            # assume col a has type double
            f.max(col("a"))  # CAST(MAX(a) AS double) AS a
            f.max(-col("a"))  # CAST(MAX(-a) AS double) AS a

            # neither type nor alias can be inferred in the following cases
            f.max(col("a")+1)
            f.max(col("a")+col("b"))

            # you can specify explicitly
            # CAST(MAX(a+b) AS int) AS x
            f.max(col("a")+col("b")).cast(int).alias("x")
    """
    assert isinstance(col, ColumnExpr)
    return _SameTypeUnaryAggFuncExpr("MAX", col)


def count(col: ColumnExpr) -> ColumnExpr:
    """SQL ``COUNT`` function (aggregation)

    :param col: the column to find count

    .. note::

        * this function cannot infer type from ``col`` type
        * this function can infer alias from ``col``'s inferred alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            f.count(col("*"))  # COUNT(*)
            f.count(col("a"))  # COUNT(a) AS a

            # you can specify explicitly
            # CAST(COUNT(a) AS double) AS a
            f.count(col("a")).cast(float)
    """
    assert isinstance(col, ColumnExpr)
    return _UnaryAggFuncExpr("COUNT", col)


def count_distinct(col: ColumnExpr) -> ColumnExpr:
    """SQL ``COUNT DISTINCT`` function (aggregation)

    :param col: the column to find distinct element count

    .. note::

        * this function cannot infer type from ``col`` type
        * this function can infer alias from ``col``'s inferred alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            f.count_distinct(col("*"))  # COUNT(DISTINCT *)
            f.count_distinct(col("a"))  # COUNT(DISTINCT a) AS a

            # you can specify explicitly
            # CAST(COUNT(DISTINCT a) AS double) AS a
            f.count_distinct(col("a")).cast(float)
    """
    assert isinstance(col, ColumnExpr)
    return _UnaryAggFuncExpr("COUNT", col, arg_distinct=True)


def avg(col: ColumnExpr) -> ColumnExpr:
    """SQL ``AVG`` function (aggregation)

    :param col: the column to find average

    .. note::

        * this function cannot infer type from ``col`` type
        * this function can infer alias from ``col``'s inferred alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            f.avg(col("a"))  # AVG(a) AS a

            # you can specify explicitly
            # CAST(AVG(a) AS double) AS a
            f.avg(col("a")).cast(float)
    """
    assert isinstance(col, ColumnExpr)
    return _UnaryAggFuncExpr("AVG", col)


def sum(col: ColumnExpr) -> ColumnExpr:  # pylint: disable=redefined-builtin
    """SQL ``SUM`` function (aggregation)

    :param col: the column to find sum

    .. note::

        * this function cannot infer type from ``col`` type
        * this function can infer alias from ``col``'s inferred alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            f.sum(col("a"))  # SUM(a) AS a

            # you can specify explicitly
            # CAST(SUM(a) AS double) AS a
            f.sum(col("a")).cast(float)
    """
    assert isinstance(col, ColumnExpr)
    return _UnaryAggFuncExpr("SUM", col)


def first(col: ColumnExpr) -> ColumnExpr:
    """SQL ``FIRST`` function (aggregation)

    :param col: the column to find first

    .. note::

        * this function can infer type from ``col`` type
        * this function can infer alias from ``col``'s inferred alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            # assume col a has type double
            f.first(col("a"))  # CAST(FIRST(a) AS double) AS a
            f.first(-col("a"))  # CAST(FIRST(-a) AS double) AS a

            # neither type nor alias can be inferred in the following cases
            f.first(col("a")+1)
            f.first(col("a")+col("b"))

            # you can specify explicitly
            # CAST(FIRST(a+b) AS int) AS x
            f.first(col("a")+col("b")).cast(int).alias("x")
    """
    assert isinstance(col, ColumnExpr)
    return _SameTypeUnaryAggFuncExpr("FIRST", col)


def last(col: ColumnExpr) -> ColumnExpr:
    """SQL ``LAST`` function (aggregation)

    :param col: the column to find last

    .. note::

        * this function can infer type from ``col`` type
        * this function can infer alias from ``col``'s inferred alias

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            # assume col a has type double
            f.last(col("a"))  # CAST(LAST(a) AS double) AS a
            f.last(-col("a"))  # CAST(LAST(-a) AS double) AS a

            # neither type nor alias can be inferred in the following cases
            f.last(col("a")+1)
            f.last(col("a")+col("b"))

            # you can specify explicitly
            # CAST(LAST(a+b) AS int) AS x
            f.last(col("a")+col("b")).cast(int).alias("x")
    """
    assert isinstance(col, ColumnExpr)
    return _SameTypeUnaryAggFuncExpr("LAST", col)


def is_agg(column: Any) -> bool:
    """Check if a column contains aggregation operation

    :param col: the column to check
    :return: whether the column is :class:`~fugue.column.expressions.ColumnExpr`
      and contains aggregation operations

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column.functions as f

            assert not f.is_agg(1)
            assert not f.is_agg(col("a"))
            assert not f.is_agg(col("a")+lit(1))

            assert f.is_agg(f.max(col("a")))
            assert f.is_agg(-f.max(col("a")))
            assert f.is_agg(f.max(col("a")+1))
            assert f.is_agg(f.max(col("a"))+f.min(col("a"))))
    """
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
