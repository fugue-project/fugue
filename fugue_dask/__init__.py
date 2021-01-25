# flake8: noqa
import dask.dataframe as dd
from fugue.workflow import register_raw_df_type
from fugue_version import __version__

from fugue_dask.dataframe import DaskDataFrame
from fugue_dask.execution_engine import DaskExecutionEngine

register_raw_df_type(dd.DataFrame)
