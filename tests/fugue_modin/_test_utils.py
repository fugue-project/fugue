import os
import random

import modin.pandas as pd
import numpy as np
from fugue_modin.utils import get_schema

os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask


def test_get_schema():
    df = pd.DataFrame([[0, "a"]], columns=["b", "a"])
    assert get_schema(df) == "b:long,a:str"
