# flake8: noqa
from fugue_ibis.execution.ibis_engine import IbisEngine, register_ibis_engine
from fugue_ibis.execution.pandas_backend import _to_pandas_ibis_engine
from fugue_ibis.extensions import as_fugue, as_ibis, run_ibis
from triad import run_at_def


@run_at_def
def register():
    register_ibis_engine(1, _to_pandas_ibis_engine)
