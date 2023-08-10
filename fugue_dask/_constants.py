from typing import Any, Dict
import pandas as pd
import dask

FUGUE_DASK_CONF_DEFAULT_PARTITIONS = "fugue.dask.default.partitions"
FUGUE_DASK_DEFAULT_CONF: Dict[str, Any] = {FUGUE_DASK_CONF_DEFAULT_PARTITIONS: -1}
FUGUE_DASK_USE_ARROW = hasattr(pd, "ArrowDtype") and dask.__version__ >= "2023.7.1"
