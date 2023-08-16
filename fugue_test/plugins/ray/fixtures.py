import pytest


@pytest.fixture(scope="session")
def fugue_ray_session():
    import ray

    with ray.init(num_cpus=2):
        yield "ray"
