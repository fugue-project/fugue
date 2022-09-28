# flake8: noqa
from triad import run_at_def

from ._compat import IbisTable
from .dataframe import IbisDataFrame
from .execution.ibis_engine import IbisEngine, register_ibis_engine
from .execution.pandas_backend import _to_pandas_ibis_engine
from .execution_engine import IbisExecutionEngine
from .extensions import as_fugue, as_ibis, run_ibis


@run_at_def
def register():
    register_ibis_engine(1, _to_pandas_ibis_engine)
