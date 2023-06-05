from typing import Any, Dict

FUGUE_DASK_CONF_DEFAULT_PARTITIONS = "fugue.dask.default.partitions"
FUGUE_DASK_DEFAULT_CONF: Dict[str, Any] = {FUGUE_DASK_CONF_DEFAULT_PARTITIONS: -1}
