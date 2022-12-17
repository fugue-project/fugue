import pickle
from typing import List, Optional, Tuple, Dict, Any

import pyarrow as pa
import ray.data as rd
from fugue.dataframe.arrow_dataframe import _build_empty_arrow
from triad import Schema

_RAY_NULL_REPR = "__RAY_NULL__"


def get_dataset_format(df: rd.Dataset) -> Optional[str]:
    try:  # pragma: no cover
        if hasattr(df, "_dataset_format"):  # ray<2.2
            return df._dataset_format()
        return df.dataset_format()  # ray>=2.2
    except Exception:
        return None


def build_empty(schema: Schema) -> rd.Dataset:
    return rd.from_arrow(_build_empty_arrow(schema))


def add_partition_key(
    df: rd.Dataset, input_schema: Schema, keys: List[str], output_key: str
) -> Tuple[rd.Dataset, Schema]:
    def is_valid_type(tp: pa.DataType) -> bool:
        return (
            pa.types.is_string(tp)
            or pa.types.is_integer(tp)
            or pa.types.is_floating(tp)
            or pa.types.is_date(tp)
            or pa.types.is_time(tp)
            or pa.types.is_timestamp(tp)
            or pa.types.is_boolean(tp)
            or pa.types.is_binary(tp)
        )

    ray_remote_args: Dict[str, Any] = {"num_cpus": 1}

    if len(keys) == 1 and is_valid_type(input_schema[keys[0]].type):

        def add_simple_key(arrow_df: pa.Table) -> pa.Table:  # pragma: no cover
            return arrow_df.append_column(
                output_key,
                arrow_df.column(input_schema.index_of_key(keys[0]))
                .cast(pa.string())
                .fill_null(_RAY_NULL_REPR),
            )

        return df.map_batches(
            add_simple_key, batch_format="pyarrow", **ray_remote_args
        ), input_schema + (
            output_key,
            str,
        )
    else:
        key_cols = [input_schema.index_of_key(k) for k in keys]

        def add_key(arrow_df: pa.Table) -> pa.Table:  # pragma: no cover
            fdf = arrow_df.combine_chunks()
            sarr = pa.StructArray.from_arrays(
                [fdf.column(i).combine_chunks() for i in key_cols], keys
            ).tolist()
            sarr = pa.array([pickle.dumps(x) for x in sarr])
            return fdf.append_column(output_key, sarr)

        return df.map_batches(
            add_key, batch_format="pyarrow", **ray_remote_args
        ), input_schema + (
            output_key,
            pa.binary(),
        )
