import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from dask import dataframe as dd
from fugue._utils.io import FileParser
from fugue._utils.io import _load_avro as _pd_load_avro
from fugue._utils.io import _save_avro
from triad.collections.dict import ParamDict
from triad.collections.fs import FileSystem
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw

from fugue_dask.dataframe import DaskDataFrame


def load_df(
    uri: Union[str, List[str]],
    format_hint: Optional[str] = None,
    columns: Any = None,
    fs: Optional[FileSystem] = None,
    **kwargs: Any,
) -> DaskDataFrame:
    if isinstance(uri, str):
        fp = [FileParser(uri, format_hint)]
    else:
        fp = [FileParser(u, format_hint) for u in uri]
    dfs: List[dd.DataFrame] = []
    schema: Any = None
    for f in _dask_get_single_files(fp, fs):
        df, schema = _FORMAT_LOAD[f.file_format](f, columns, **kwargs)
        dfs.append(df)
    return DaskDataFrame(dd.concat(dfs), schema)


def save_df(
    df: DaskDataFrame,
    uri: str,
    format_hint: Optional[str] = None,
    mode: str = "overwrite",
    fs: Optional[FileSystem] = None,
    **kwargs: Any,
) -> None:
    assert_or_throw(
        mode in ["overwrite", "error"], NotImplementedError(f"{mode} is not supported")
    )
    p = FileParser(uri, format_hint).assert_no_glob()
    if fs is None:
        fs = FileSystem()
    if fs.exists(uri):
        assert_or_throw(mode == "overwrite", FileExistsError(uri))
        try:
            fs.remove(uri)
        except Exception:
            try:
                fs.removetree(uri)
            except Exception:  # pragma: no cover
                pass
    _FORMAT_SAVE[p.file_format](df, p, **kwargs)


def _save_parquet(df: DaskDataFrame, p: FileParser, **kwargs: Any) -> None:
    df.native.to_parquet(p.uri, **{"schema": df.schema.pa_schema, **kwargs})


def _load_parquet(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[dd.DataFrame, Any]:
    if columns is None:
        pdf = dd.read_parquet(p.uri, **kwargs)
        schema = Schema(pdf.head(1))
        return pdf, schema
    if isinstance(columns, list):  # column names
        pdf = dd.read_parquet(p.uri, columns=columns, **kwargs)
        schema = Schema(pdf.head(1))
        return pdf, schema
    schema = Schema(columns)
    pdf = dd.read_parquet(p.uri, columns=schema.names, **kwargs)
    return pdf, schema


def _save_csv(df: DaskDataFrame, p: FileParser, **kwargs: Any) -> None:
    df.native.to_csv(p.uri, **{"index": False, "header": False, **kwargs})


def _load_csv(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[dd.DataFrame, Any]:
    kw = ParamDict(kwargs)
    infer_schema = kw.get("infer_schema", False)
    if not infer_schema:
        kw["dtype"] = object
    if "infer_schema" in kw:
        del kw["infer_schema"]
    header: Any = False
    if "header" in kw:
        header = kw["header"]
        del kw["header"]
    if str(header) in ["True", "0"]:
        pdf = dd.read_csv(p.uri, **{"header": 0, **kw})
        if columns is None:
            return pdf, None
        if isinstance(columns, list):  # column names
            return pdf[columns], None
        schema = Schema(columns)
        return pdf[schema.names], schema
    if header is None or str(header) == "False":
        if columns is None:
            raise InvalidOperationError("columns must be set if without header")
        if isinstance(columns, list):  # column names
            pdf = dd.read_csv(p.uri, **{"header": None, "names": columns, **kw})
            return pdf, None
        schema = Schema(columns)
        pdf = dd.read_csv(p.uri, **{"header": None, "names": schema.names, **kw})
        return pdf, schema
    else:
        raise NotImplementedError(f"{header} is not supported")


def _save_json(df: DaskDataFrame, p: FileParser, **kwargs: Any) -> None:
    df.native.to_json(p.uri, **kwargs)


def _load_json(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[dd.DataFrame, Any]:
    pdf = dd.read_json(p.uri, **kwargs).reset_index(drop=True)
    if columns is None:
        return pdf, None
    if isinstance(columns, list):  # column names
        return pdf[columns], None
    schema = Schema(columns)
    return pdf[schema.names], schema


def _load_avro(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[dd.DataFrame, Any]:
    pdf, schema = _pd_load_avro(p, columns, **kwargs)

    return dd.from_pandas(pdf), schema


def _dask_get_single_files(
    fp: Iterable[FileParser], fs: Optional[FileSystem]
) -> Iterable[FileParser]:
    if fs is None:
        fs = FileSystem()
    for f in fp:
        if f.glob_pattern != "":
            files = [
                FileParser(os.path.join(f.uri, os.path.basename(x.path)))
                for x in fs.opendir(f.uri).glob(f.glob_pattern)
            ]
            yield from _dask_get_single_files(files, fs)
        elif fs.isdir(f.uri):
            if f.uri.endswith(f.file_format):
                if f.file_format in ["csv", "json"]:
                    yield FileParser(f.uri + "/[!_]*", format_hint=f.file_format)
                else:
                    yield f
            else:
                for x in fs.filterdir(f.uri, files=["*." + f.file_format]):
                    yield FileParser(
                        os.path.join(f.uri, x.name), format_hint=f.file_format
                    )
        else:
            yield f


_FORMAT_LOAD: Dict[str, Callable[..., Tuple[dd.DataFrame, Any]]] = {
    "csv": _load_csv,
    "parquet": _load_parquet,
    "json": _load_json,
    "avro": _load_avro,
}

_FORMAT_SAVE: Dict[str, Callable] = {
    "csv": _save_csv,
    "parquet": _save_parquet,
    "json": _save_json,
    "avro": _save_avro,
}
