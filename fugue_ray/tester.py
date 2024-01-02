from contextlib import contextmanager
from typing import Any, Dict, Iterator

import ray

import fugue.test as ft


@ft.fugue_test_backend
class RayTestBackend(ft.FugueTestBackend):
    name = "ray"
    default_session_conf = {"num_cpus": 2}
    default_fugue_conf = {
        "fugue.ray.zero_copy": True,
        "fugue.ray.default.batch_size": 10000,
    }

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        with ray.init(**session_conf):
            yield "ray"
