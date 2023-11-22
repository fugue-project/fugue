import fugue.api as fa
from fugue_test.plugins import with_fugue_engine_context


@with_fugue_engine_context("ray")
def test_all():
    print(fa.get_context_engine())
