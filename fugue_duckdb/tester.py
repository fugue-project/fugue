from contextlib import contextmanager
from typing import Any, Dict, Iterator

import duckdb

import fugue.test as ft

try:
    import dask.distributed as dd
    import dask

    _HAS_DASK = True
except ImportError:  # pragma: no cover
    _HAS_DASK = False


@ft.fugue_test_backend
class DuckDBTestBackend(ft.FugueTestBackend):
    name = "duckdb"

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        with duckdb.connect(config=session_conf) as conn:
            yield conn


if _HAS_DASK:

    @ft.fugue_test_backend
    class DuckDaskTestBackend(ft.FugueTestBackend):
        name = "duckdask"

        @classmethod
        def transform_session_conf(cls, conf: Dict[str, Any]) -> Dict[str, Any]:
            res = ft.extract_conf(conf, "duck.", remove_prefix=False)
            res.update(ft.extract_conf(conf, "dask.", remove_prefix=False))
            return res

        @classmethod
        @contextmanager
        def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
            duck_conf = ft.extract_conf(session_conf, "duck.", remove_prefix=True)
            dask_conf = ft.extract_conf(session_conf, "dask.", remove_prefix=True)
            with dd.Client(**dask_conf) as client:
                dask.config.set({"dataframe.shuffle.method": "tasks"})
                dask.config.set({"dataframe.convert-string": False})
                with duckdb.connect(config=duck_conf) as conn:
                    yield [conn, client]
