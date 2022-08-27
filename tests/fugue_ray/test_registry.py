import pandas as pd
import ray
import ray.data as rd
from fugue import FugueWorkflow

from fugue_ray import RayExecutionEngine


def test_registry():
    def creator() -> rd.Dataset:
        return rd.from_pandas(pd.DataFrame(dict(a=[1, 2], b=["a", "b"])))

    def processor1(ctx: RayExecutionEngine, df: rd.Dataset) -> pd.DataFrame:
        assert isinstance(ctx, RayExecutionEngine)
        return df.to_pandas()

    def processor2(df: pd.DataFrame) -> rd.Dataset:
        return rd.from_pandas(df)

    def outputter(df: rd.Dataset) -> None:
        assert [[1, "a"], [2, "b"]] == df.to_pandas().values.tolist()

    dag = FugueWorkflow()
    dag.create(creator).process(processor1).process(processor2).output(outputter)

    with ray.init():
        dag.run("ray")
