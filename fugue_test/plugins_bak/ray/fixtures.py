from contextlib import contextmanager
from typing import Any, Dict, Iterator

import pytest

from .. import FugueTester, register_fugue_tester


@register_fugue_tester("ray")
class RayTester(FugueTester):
    default_conf = {"num_cpus": 2}

    @classmethod
    def transform_conf(cls, conf: Dict[str, Any]) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        for k, v in conf.items():
            if k.startswith("ray."):
                res[k[4:]] = v
        return res

    @classmethod
    @contextmanager
    def context(cls) -> Iterator[Any]:
        import ray

        with ray.init(**cls.conf):
            yield "ray"


@pytest.fixture(scope="session")
def fugue_ray_session():
    with RayTester.context() as engine:
        yield engine
