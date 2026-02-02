# flake8: noqa
from importlib.metadata import version

__version__ = version("fugue")

from fugue_dask.dataframe import DaskDataFrame
from fugue_dask.execution_engine import DaskExecutionEngine
