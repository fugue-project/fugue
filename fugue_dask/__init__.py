# flake8: noqa
from fugue_version import __version__

from fugue_dask.dataframe import DaskDataFrame
from fugue_dask.execution_engine import DaskExecutionEngine
from fugue_dask.registry import register

try:
    from fugue_dask.ibis_engine import DaskIbisEngine
except Exception:  # pragma: no cover
    pass

register()
