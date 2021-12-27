from typing import Any, Callable, Dict

import ibis
import ibis.expr.types as ir
from fugue import DataFrame, DataFrames, Processor, WorkflowDataFrame
from fugue.exceptions import FugueWorkflowCompileError
from fugue.workflow.workflow import WorkflowDataFrames
from triad.utils.assertion import assert_or_throw

from fugue_ibis._utils import LazyIbisObject, _materialize
from fugue_ibis.execution.ibis_engine import to_ibis_engine


def run_ibis(
    ibis_func: Callable[[ibis.BaseBackend], ir.TableExpr],
    ibis_engine: Any = None,
    **dfs: WorkflowDataFrame,
) -> WorkflowDataFrame:
    """Run an ibis workflow wrapped in ``ibis_func``

    :param ibis_func: the function taking in an ibis backend, and returning
        a :ref:`TableExpr <ibis:/api.rst#table-methods>`
    :param ibis_engine: an object that together with |ExecutionEngine|
        can determine :class:`~fugue_ibis.execution.ibis_engine.IbisEngine`
        , defaults to None
    :param dfs: dataframes in the same workflow
    :return: the output workflow dataframe
    """
    wdfs = WorkflowDataFrames(**dfs)
    return wdfs.workflow.process(
        wdfs,
        using=_IbisProcessor,
        params=dict(ibis_func=ibis_func, ibis_engine=ibis_engine),
    )


def as_ibis(df: WorkflowDataFrame) -> ir.TableExpr:
    return LazyIbisObject(df)  # type: ignore


def as_fugue(
    expr: ir.TableExpr,
    ibis_engine: Any = None,
) -> WorkflowDataFrame:
    """Convert to lazy ibis object to Fugue workflow dataframe

    :param expr: the actual instance should be LazyIbisObject
    :return: the Fugue workflow dataframe
    """

    def _func(
        be: ibis.BaseBackend,
        lazy_expr: LazyIbisObject,
        ctx: Dict[int, Any],
    ) -> ir.TableExpr:
        return _materialize(
            lazy_expr, {k: be.table(f"_{id(v)}") for k, v in ctx.items()}
        )

    assert_or_throw(
        isinstance(expr, LazyIbisObject),
        FugueWorkflowCompileError("expr must be a LazyIbisObject"),
    )
    _lazy_expr: LazyIbisObject = expr  # type: ignore
    _ctx = _lazy_expr._super_lazy_internal_ctx
    _dfs = {f"_{id(v)}": v for _, v in _ctx.items()}
    return run_ibis(
        lambda be: _func(be, _lazy_expr, _ctx), ibis_engine=ibis_engine, **_dfs
    )


def setup():
    setattr(WorkflowDataFrame, "as_ibis", as_ibis)  # noqa: B010
    setattr(LazyIbisObject, "as_fugue", as_fugue)  # noqa: B010


class _IbisProcessor(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        ibis_func = self.params.get_or_throw("ibis_func", Callable)
        ibis_engine = self.params.get_or_none("ibis_engine", object)
        ie = to_ibis_engine(self.execution_engine, ibis_engine)
        return ie.select(dfs, ibis_func)
