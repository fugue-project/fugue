from typing import Dict, Any

try:
    import psutil

    _CPU_COUNT = psutil.cpu_count(logical=False)
except Exception:  # pragma: no cover
    try:
        import multiprocessing

        _CPU_COUNT = multiprocessing.cpu_count()
        if _CPU_COUNT is None or _CPU_COUNT <= 0:
            _CPU_COUNT = 8
    except Exception:
        _CPU_COUNT = 8


FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS = "fugue.dask.dataframe.default.partitions"
FUGUE_DASK_DEFAULT_CONF: Dict[str, Any] = {
    FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS: _CPU_COUNT * 2
}
