# pylint: disable-all
# flake8: noqa
# TODO: This folder is to be deprecated
import warnings
from fugue.exceptions import *

warnings.warn(
    "fsql and FugueSQLWorkflow now should be imported directly from fugue, "
    "fugue_sql will be removed in 0.9.0"
)
