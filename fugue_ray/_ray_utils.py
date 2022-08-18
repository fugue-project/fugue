import ray.data as rd
from typing import Optional
from triad import Schema
from fugue.dataframe.arrow_dataframe import _build_empty_arrow


def get_dataset_format(df: rd.Dataset) -> Optional[str]:
    try:
        return df._dataset_format()
    except Exception:
        return None


def build_empty(schema: Schema) -> rd.Dataset:
    return rd.from_arrow(_build_empty_arrow(schema))
