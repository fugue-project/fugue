from typing import Any, Dict

import dask
import pandas as pd
import pyarrow as pa
from packaging import version

FUGUE_DASK_CONF_DEFAULT_PARTITIONS = "fugue.dask.default.partitions"
FUGUE_DASK_DEFAULT_CONF: Dict[str, Any] = {FUGUE_DASK_CONF_DEFAULT_PARTITIONS: -1}
FUGUE_DASK_USE_ARROW = (
    hasattr(pd, "ArrowDtype")
    and version.parse(dask.__version__) >= version.parse("2023.2")
    and version.parse(pa.__version__) >= version.parse("7")
    and version.parse(pd.__version__) >= version.parse("2")
)
