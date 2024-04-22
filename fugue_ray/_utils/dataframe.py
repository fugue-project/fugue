import pickle
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import ray.data as rd
from triad import Schema

from .._constants import _ZERO_COPY

_RAY_NULL_REPR = "__RAY_NULL__"


def is_materialized(df: rd.Dataset) -> bool:
    if hasattr(rd.dataset, "MaterializedDataset"):
        return isinstance(df, rd.dataset.MaterializedDataset)
    return df.is_fully_executed()  # pragma: no cover


def materialize(df: rd.Dataset) -> rd.Dataset:
    if not is_materialized(df):
        if hasattr(df, "materialize"):
            df = df.materialize()
        else:  # pragma: no cover
            df = df.fully_executed()
    return df


def get_dataset_format(df: rd.Dataset) -> Tuple[Optional[str], rd.Dataset]:
    df = materialize(df)
    if df.count() == 0:
        return None, df
    schema = df.schema(fetch_if_missing=True)
    if schema is None:  # pragma: no cover
        return None, df
    if isinstance(schema.base_schema, pa.Schema):
        return "arrow", df
    return "pandas", df


def to_schema(schema: Any) -> Schema:  # pragma: no cover
    if isinstance(schema, pa.Schema):
        return Schema(schema)
    if isinstance(schema, rd.Schema):
        if hasattr(schema, "base_schema") and isinstance(schema.base_schema, pa.Schema):
            return Schema(schema.base_schema)
        return Schema(list(zip(schema.names, schema.types)))
    raise ValueError(f"{schema} is not supported")


def build_empty(schema: Schema) -> rd.Dataset:
    return rd.from_arrow(schema.create_empty_arrow_table())


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
            add_simple_key, batch_format="pyarrow", **_ZERO_COPY, **ray_remote_args
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
            add_key, batch_format="pyarrow", **_ZERO_COPY, **ray_remote_args
        ), input_schema + (
            output_key,
            pa.binary(),
        )


def add_coarse_partition_key(
    df: rd.Dataset,
    keys: List[str],
    output_key: str,
    bucket: int,
) -> rd.Dataset:
    ray_remote_args: Dict[str, Any] = {"num_cpus": 1}

    def add_coarse_key(arrow_df: pa.Table) -> pa.Table:  # pragma: no cover
        hdf = arrow_df.select(keys).to_pandas()
        _hash = pd.util.hash_pandas_object(hdf, index=False).mod(bucket)
        return arrow_df.append_column(output_key, pa.Array.from_pandas(_hash))

    return df.map_batches(
        add_coarse_key,
        batch_format="pyarrow",
        **_ZERO_COPY,
        **ray_remote_args,
    )
