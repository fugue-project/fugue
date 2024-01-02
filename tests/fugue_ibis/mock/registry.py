from fugue.plugins import parse_execution_engine
from typing import Any
from .execution_engine import MockDuckExecutionEngine


@parse_execution_engine.candidate(
    lambda engine, conf, **kwargs: isinstance(engine, str) and engine == "mockibisduck"
)
def _parse_mockibisduck(
    engine: str, conf: Any, **kwargs: Any
) -> MockDuckExecutionEngine:
    return MockDuckExecutionEngine(conf=conf)
