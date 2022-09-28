# flake8: noqa
# pylint: disable-all

try:
    from ibis.expr.types import Table as IbisTable
except Exception:
    from ibis.expr.types import TableExpr as IbisTable
