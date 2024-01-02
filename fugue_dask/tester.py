from contextlib import contextmanager
from typing import Any, Dict, Iterator

import dask
from dask.distributed import Client

import fugue.test as ft


@ft.fugue_test_backend
class DaskTestBackend(ft.FugueTestBackend):
    name = "dask"

    @classmethod
    def transform_session_conf(cls, conf: Dict[str, Any]) -> Dict[str, Any]:
        return ft.extract_conf(conf, "dask.", remove_prefix=True)

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        with Client(**session_conf) as client:
            dask.config.set({"dataframe.shuffle.method": "tasks"})
            dask.config.set({"dataframe.convert-string": False})
            yield client
