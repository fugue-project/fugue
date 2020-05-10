from typing import List

import modin.pandas as pd
import numpy as np
import pyarrow as pa
from triad.collections import Schema


def get_schema(df: pd.DataFrame) -> Schema:
    fields: List[pa.Field] = []
    for col in df.columns:
        if df[col].dtype == np.object:
            fields.append(pa.field(col, pa.string()))
        else:
            fields.append(pa.field(col, pa.from_numpy_dtype(df[col].dtype)))
    return Schema(fields)
