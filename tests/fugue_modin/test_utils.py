from fugue_modin.utils import get_schema
import modin.pandas as pd
import os
os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask


def test_get_schema():
    df = pd.DataFrame([[0, "a"]], columns=["b", "a"])
    assert get_schema(df) == "b:long,a:str"
