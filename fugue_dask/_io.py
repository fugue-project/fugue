from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from dask import dataframe as dd
from fsspec import AbstractFileSystem
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.io import isfile, join, makedirs, url_to_fs

from fugue._utils.io import FileParser, _get_single_files
from fugue_dask.dataframe import DaskDataFrame

from ._utils import DASK_UTILS


def load_df(
    uri: Union[str, List[str]],
    format_hint: Optional[str] = None,
    columns: Any = None,
    fs: Optional[AbstractFileSystem] = None,
    **kwargs: Any,
) -> DaskDataFrame:
    if isinstance(uri, str):
        fp = [FileParser(uri, format_hint)]
    else:
        fp = [FileParser(u, format_hint) for u in uri]
    dfs: List[dd.DataFrame] = []
    schema: Any = None
    for f in _get_single_files(fp, fs):
        df, schema = _FORMAT_LOAD[f.file_format](f, columns, **kwargs)
        dfs.append(df)
    return DaskDataFrame(dd.concat(dfs), schema)


def save_df(
    df: DaskDataFrame,
    uri: str,
    format_hint: Optional[str] = None,
    mode: str = "overwrite",
    fs: Optional[AbstractFileSystem] = None,
    **kwargs: Any,
) -> None:
    assert_or_throw(
        mode in ["overwrite", "error"],
        lambda: NotImplementedError(f"{mode} is not supported"),
    )
    p = FileParser(uri, format_hint).assert_no_glob()
    if fs is None:
        fs, _ = url_to_fs(uri)
    if fs.exists(uri):
        assert_or_throw(mode == "overwrite", FileExistsError(uri))
        try:
            fs.rm(uri, recursive=True)
        except Exception:  # pragma: no cover
            pass
    _FORMAT_SAVE[p.file_format](df, p, **kwargs)


def _save_parquet(df: DaskDataFrame, p: FileParser, **kwargs: Any) -> None:
    params = {
        "schema": df.schema.pa_schema,
        "write_index": False,
        **kwargs,
    }
    DASK_UTILS.to_parquet_friendly(df.native).to_parquet(p.path, **params)


def _load_parquet(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[dd.DataFrame, Any]:
    params = dict(kwargs)
    dtype_backend: Any = params.pop("dtype_backend", None)
    params["engine"] = "pyarrow"
    params["arrow_to_pandas"] = {"ignore_metadata": True}
    if pd.__version__ >= "1.5":
        dtype_backend = "pyarrow"
    if columns is None:
        pdf = dd.read_parquet(p.path, dtype_backend=dtype_backend, **params)
        schema = Schema(pdf.head(1))
        return pdf, schema
    if isinstance(columns, list):  # column names
        pdf = dd.read_parquet(
            p.path, columns=columns, dtype_backend=dtype_backend, **params
        )
        schema = Schema(pdf.head(1))
        return pdf, schema
    schema = Schema(columns)
    pdf = dd.read_parquet(
        p.path, columns=schema.names, dtype_backend=dtype_backend, **params
    )
    return pdf, schema


def _save_csv(df: DaskDataFrame, p: FileParser, **kwargs: Any) -> None:
    makedirs(p.path, exist_ok=True)
    df.native.to_csv(
        p.join("*.csv").path, **{"index": False, "header": False, **kwargs}
    )


def _safe_load_csv(path: str, **kwargs: Any) -> dd.DataFrame:
    if not isfile(path):
        return dd.read_csv(join(path, "*.csv"), **kwargs)
    try:
        return dd.read_csv(path, **kwargs)
    except (IsADirectoryError, PermissionError):  # pragma: no cover
        return dd.read_csv(join(path, "*.csv"), **kwargs)


def _load_csv(  # noqa: C901
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[dd.DataFrame, Any]:
    kw = ParamDict(kwargs)
    infer_schema = kw.get("infer_schema", False)
    if infer_schema and columns is not None and not isinstance(columns, list):
        raise ValueError("can't set columns as a schema when infer schema is true")
    if not infer_schema:
        kw["dtype"] = object
    if "infer_schema" in kw:
        del kw["infer_schema"]
    header: Any = False
    if "header" in kw:
        header = kw["header"]
        del kw["header"]
    if str(header) in ["True", "0"]:
        pdf = _safe_load_csv(p.path, **{"header": 0, **kw})
        if columns is None:
            return pdf, None
        if isinstance(columns, list):  # column names
            return pdf[columns], None
        schema = Schema(columns)
        return pdf[schema.names], schema
    if header is None or str(header) == "False":
        if columns is None:
            raise ValueError("columns must be set if without header")
        if isinstance(columns, list):  # column names
            pdf = _safe_load_csv(p.path, **{"header": None, "names": columns, **kw})
            return pdf, None
        schema = Schema(columns)
        pdf = _safe_load_csv(p.path, **{"header": None, "names": schema.names, **kw})
        return pdf, schema
    else:
        raise NotImplementedError(f"{header} is not supported")


def _save_json(df: DaskDataFrame, p: FileParser, **kwargs: Any) -> None:
    makedirs(p.path, exist_ok=True)
    df.native.to_json(p.join("*.json").path, **kwargs)


def _safe_load_json(path: str, **kwargs: Any) -> dd.DataFrame:
    if not isfile(path):
        return dd.read_json(join(path, "*.json"), **kwargs)
    try:
        return dd.read_json(path, **kwargs)
    except (IsADirectoryError, PermissionError):  # pragma: no cover
        return dd.read_json(join(path, "*.json"), **kwargs)


def _load_json(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[dd.DataFrame, Any]:
    pdf = _safe_load_json(p.path, **kwargs).reset_index(drop=True)
    if columns is None:
        return pdf, None
    if isinstance(columns, list):  # column names
        return pdf[columns], None
    schema = Schema(columns)
    return pdf[schema.names], schema


_FORMAT_LOAD: Dict[str, Callable[..., Tuple[dd.DataFrame, Any]]] = {
    "csv": _load_csv,
    "parquet": _load_parquet,
    "json": _load_json,
}

_FORMAT_SAVE: Dict[str, Callable] = {
    "csv": _save_csv,
    "parquet": _save_parquet,
    "json": _save_json,
}
