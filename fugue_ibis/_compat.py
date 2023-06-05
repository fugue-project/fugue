# flake8: noqa
# pylint: disable-all

try:  # pragma: no cover
    from ibis.expr.types import Table as IbisTable
except Exception:  # pragma: no cover
    from ibis.expr.types import TableExpr as IbisTable

from ibis import Schema as IbisSchema
