# flake8: noqa
# TODO: This folder is to be deprecated
from fugue_version import __version__

import warnings
from fugue import FugueSQLWorkflow, fsql

warnings.warn(
    "fsql and FugueSQLWorkflow now should be imported directly from fugue, "
    "fugue_sql will be removed in 0.9.0"
)
