import os
import pathlib
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Iterable
from urllib.parse import urlparse

import pandas as pd
from fugue.dataframe import LocalBoundedDataFrame, LocalDataFrame, PandasDataFrame
from triad.collections.fs import FileSystem
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.collections.dict import ParamDict


class FileParser(object):
    def __init__(self, path: str, format_hint: Optional[str] = None):
        last = len(path)
        has_glob = False
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
            self._path = os.path.join(self._uri.path, self._glob_pattern)

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

    @property
    def glob_pattern(self) -> str:
        return self._glob_pattern

    @property
    def uri(self) -> str:
        return self._uri.geturl()

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
    fs: Optional[FileSystem] = None,
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


def _get_single_files(
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
            yield from _get_single_files(files, fs)
        elif fs.isdir(f.uri):
            for x in fs.filterdir(f.uri, files=["*." + f.file_format]):
                yield FileParser(os.path.join(f.uri, x.name))
        else:
            yield f


def _save_parquet(df: LocalDataFrame, p: FileParser, **kwargs: Any) -> None:
    df.as_pandas().to_parquet(
        p.uri, **{"engine": "pyarrow", "schema": df.schema.pa_schema, **kwargs}
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


def _load_csv(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[pd.DataFrame, Any]:
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
        pdf = pd.read_csv(p.uri, **{"index_col": False, "header": 0, **kw})
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
            pdf = pd.read_csv(
                p.uri, **{"index_col": False, "header": None, "names": columns, **kw}
            )
            return pdf, None
        schema = Schema(columns)
        pdf = pd.read_csv(
            p.uri, **{"index_col": False, "header": None, "names": schema.names, **kw}
        )
        return pdf, schema
    else:
        raise NotImplementedError(f"{header} is not supported")


def _save_json(df: LocalDataFrame, p: FileParser, **kwargs: Any) -> None:
    df.as_pandas().to_json(p.uri, **kwargs)


def _load_json(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[pd.DataFrame, Any]:
    pdf = pd.read_json(p.uri, **kwargs).reset_index(drop=True)
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
