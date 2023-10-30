import os
import pathlib
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urlparse

import pandas as pd
from fsspec import AbstractFileSystem
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.io import join, url_to_fs
from triad.utils.pandas_like import PD_UTILS

from fugue.dataframe import LocalBoundedDataFrame, LocalDataFrame, PandasDataFrame


class FileParser(object):
    def __init__(self, path: str, format_hint: Optional[str] = None):
        last = len(path)
        has_glob = False
        self._orig_format_hint = format_hint
        for i in range(len(path)):
            if path[i] in ["/", "\\"]:
                last = i
            if path[i] in ["*", "?"]:
                has_glob = True
                break
        if not has_glob:
            self._uri = urlparse(path)
            self._glob_pattern = ""
            self._path = self._uri.path
        else:
            self._uri = urlparse(path[:last])
            self._glob_pattern = path[last + 1 :]
            self._path = join(self._uri.path, self._glob_pattern)

        if format_hint is None or format_hint == "":
            for k, v in _FORMAT_MAP.items():
                if self.suffix.endswith(k):
                    self._format = v
                    return
            raise NotImplementedError(f"{self.suffix} is not supported")
        else:
            assert_or_throw(
                format_hint in _FORMAT_MAP.values(),
                NotImplementedError(f"{format_hint} is not supported"),
            )
            self._format = format_hint

    def assert_no_glob(self) -> "FileParser":
        assert_or_throw(self.glob_pattern == "", f"{self.path} has glob pattern")
        return self

    def with_glob(self, glob: str, format_hint: Optional[str] = None) -> "FileParser":
        uri = self.uri
        if glob != "":
            uri = join(uri, glob)
        return FileParser(uri, format_hint or self._orig_format_hint)

    def find_all(self, fs: Optional[AbstractFileSystem] = None) -> List[str]:
        if self.glob_pattern == "":
            return [self.uri]
        else:
            if fs is None:
                return self.fs.glob(self._path)
            else:
                return fs.glob(self._path)

    @property
    def fs(self) -> AbstractFileSystem:
        _fs, _ = url_to_fs(self.uri)
        return _fs

    @property
    def glob_pattern(self) -> str:
        return self._glob_pattern

    @property
    def uri(self) -> str:
        return self._uri.geturl()

    @property
    def uri_with_glob(self) -> str:
        if self.glob_pattern == "":
            return self.uri
        return join(self.uri, self.glob_pattern)

    @property
    def parent(self) -> str:
        dn = os.path.dirname(self.uri)
        return dn if dn != "" else "."

    @property
    def scheme(self) -> str:
        return self._uri.scheme

    @property
    def path(self) -> str:
        return self._path

    @property
    def suffix(self) -> str:
        return "".join(pathlib.Path(self.path.lower()).suffixes)

    @property
    def file_format(self) -> str:
        return self._format


def load_df(
    uri: Union[str, List[str]],
    format_hint: Optional[str] = None,
    columns: Any = None,
    fs: Optional[AbstractFileSystem] = None,
    **kwargs: Any,
) -> LocalBoundedDataFrame:
    if isinstance(uri, str):
        fp = [FileParser(uri, format_hint)]
    else:
        fp = [FileParser(u, format_hint) for u in uri]
    dfs: List[pd.DataFrame] = []
    schema: Any = None
    for f in _get_single_files(fp, fs):
        df, schema = _FORMAT_LOAD[f.file_format](f.assert_no_glob(), columns, **kwargs)
        dfs.append(df)
    return PandasDataFrame(pd.concat(dfs), schema)


def save_df(
    df: LocalDataFrame,
    uri: str,
    format_hint: Optional[str] = None,
    mode: str = "overwrite",
    fs: Optional[AbstractFileSystem] = None,
    **kwargs: Any,
) -> None:
    assert_or_throw(
        mode in ["overwrite", "error"], NotImplementedError(f"{mode} is not supported")
    )
    p = FileParser(uri, format_hint).assert_no_glob()
    if fs is None:
        fs, _ = url_to_fs(uri)
    if fs.exists(uri):
        assert_or_throw(mode == "overwrite", FileExistsError(uri))
        try:
            fs.rm(uri, recursive=True)
        except Exception:
            pass
    _FORMAT_SAVE[p.file_format](df, p, **kwargs)


def _get_single_files(
    fp: Iterable[FileParser], fs: Optional[AbstractFileSystem]
) -> Iterable[FileParser]:
    for f in fp:
        if f.glob_pattern != "":
            files = [FileParser(x) for x in f.find_all(fs)]
            yield from _get_single_files(files, fs)
        else:
            yield f


def _save_parquet(df: LocalDataFrame, p: FileParser, **kwargs: Any) -> None:
    PD_UTILS.to_parquet_friendly(
        df.as_pandas(), partition_cols=kwargs.get("partition_cols", [])
    ).to_parquet(
        p.uri,
        **{
            "engine": "pyarrow",
            "schema": df.schema.pa_schema,
            **kwargs,
        },
    )


def _load_parquet(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[pd.DataFrame, Any]:
    if columns is None:
        pdf = pd.read_parquet(p.uri, **{"engine": "pyarrow", **kwargs})
        return pdf, None
    if isinstance(columns, list):  # column names
        pdf = pd.read_parquet(p.uri, columns=columns, **{"engine": "pyarrow", **kwargs})
        return pdf, None
    schema = Schema(columns)
    pdf = pd.read_parquet(
        p.uri, columns=schema.names, **{"engine": "pyarrow", **kwargs}
    )
    return pdf, schema


def _save_csv(df: LocalDataFrame, p: FileParser, **kwargs: Any) -> None:
    df.as_pandas().to_csv(p.uri, **{"index": False, "header": False, **kwargs})


def _safe_load_csv(path: str, **kwargs: Any) -> pd.DataFrame:
    def load_dir() -> pd.DataFrame:
        fs, _ = url_to_fs(path)
        return pd.concat(
            [pd.read_csv(x, **kwargs) for x in fs.glob(join(path, "*.csv"))]
        )

    try:
        return pd.read_csv(path, **kwargs)
    except IsADirectoryError:
        return load_dir()
    except pd.errors.ParserError:  # pragma: no cover
        return load_dir()
    except PermissionError:  # pragma: no cover
        # for windows
        return load_dir()


def _load_csv(  # noqa: C901
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[pd.DataFrame, Any]:
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
        pdf = _safe_load_csv(p.uri, **{"index_col": False, "header": 0, **kw})
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
            pdf = _safe_load_csv(
                p.uri, **{"index_col": False, "header": None, "names": columns, **kw}
            )
            return pdf, None
        schema = Schema(columns)
        pdf = _safe_load_csv(
            p.uri, **{"index_col": False, "header": None, "names": schema.names, **kw}
        )
        return pdf, schema
    else:
        raise NotImplementedError(f"{header} is not supported")


def _save_json(df: LocalDataFrame, p: FileParser, **kwargs: Any) -> None:
    df.as_pandas().to_json(p.uri, **{"orient": "records", "lines": True, **kwargs})


def _safe_load_json(path: str, **kwargs: Any) -> pd.DataFrame:
    kw = {"orient": "records", "lines": True, **kwargs}
    try:
        return pd.read_json(path, **kw)
    except (IsADirectoryError, PermissionError):
        fs, _ = url_to_fs(path)
        return pd.concat([pd.read_json(x, **kw) for x in fs.glob(join(path, "*.json"))])


def _load_json(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[pd.DataFrame, Any]:
    pdf = _safe_load_json(p.uri, **kwargs).reset_index(drop=True)
    if columns is None:
        return pdf, None
    if isinstance(columns, list):  # column names
        return pdf[columns], None
    schema = Schema(columns)
    return pdf[schema.names], schema


_FORMAT_MAP: Dict[str, str] = {
    ".csv": "csv",
    ".csv.gz": "csv",
    ".parquet": "parquet",
    ".json": "json",
    ".json.gz": "json",
}

_FORMAT_LOAD: Dict[str, Callable[..., Tuple[pd.DataFrame, Any]]] = {
    "csv": _load_csv,
    "parquet": _load_parquet,
    "json": _load_json,
}

_FORMAT_SAVE: Dict[str, Callable] = {
    "csv": _save_csv,
    "parquet": _save_parquet,
    "json": _save_json,
}
