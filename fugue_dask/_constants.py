from typing import Dict, Any

FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS = "fugue.dask.dataframe.default.partitions"
FUGUE_DASK_DEFAULT_CONF: Dict[str, Any] = {
    FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS: 16
}
