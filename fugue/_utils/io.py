import os
import pathlib
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from triad.collections.dict import ParamDict
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.io import join, url_to_fs
from triad.utils.pandas_like import PD_UTILS

from fugue.dataframe import LocalBoundedDataFrame, LocalDataFrame, PandasDataFrame


class FileParser(object):
    def __init__(self, path: str, format_hint: Optional[str] = None):
        self._orig_format_hint = format_hint
        self._has_glob = "*" in path or "?" in path
        self._raw_path = path
        self._fs, self._fs_path = url_to_fs(path)
        if not self._has_glob and self._fs.isdir(self._fs_path):
            self._is_dir = True
        else:
            self._is_dir = False
        if not self.is_local:
            self._path = self._fs.unstrip_protocol(self._fs_path)
        else:
            self._path = os.path.abspath(self._fs._strip_protocol(path))

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
        assert_or_throw(not self.has_glob, f"{self.raw_path} has glob pattern")
        return self

    @property
    def is_dir(self) -> bool:
        return self._is_dir

    @property
    def has_glob(self) -> bool:
        return self._has_glob

    @property
    def is_local(self) -> bool:
        return isinstance(self._fs, LocalFileSystem)

    def join(self, path: str, format_hint: Optional[str] = None) -> "FileParser":
        if not self.has_glob:
            _path = join(self.path, path)
        else:
            _path = join(self.parent, path)
        return FileParser(_path, format_hint or self._orig_format_hint)

    @property
    def parent(self) -> str:
        return self._fs.unstrip_protocol(self._fs._parent(self._fs_path))

    @property
    def path(self) -> str:
        return self._path

    def as_dir_path(self) -> str:
        assert_or_throw(self.is_dir, f"{self.raw_path} is not a directory")
        return self.path + self._fs.sep

    @property
    def raw_path(self) -> str:
        return self._raw_path

    @property
    def suffix(self) -> str:
        return "".join(pathlib.Path(self.raw_path.lower()).suffixes)

    @property
    def file_format(self) -> str:
        return self._format

    def make_parent_dirs(self) -> None:
        self._fs.makedirs(self._fs._parent(self._fs_path), exist_ok=True)

    def find_all(self) -> Iterable["FileParser"]:
        if self.has_glob:
            for x in self._fs.glob(self._fs_path):
                yield FileParser(self._fs.unstrip_protocol(x))
        else:
            yield self

    def open(self, *args: Any, **kwargs: Any) -> Any:
        self.assert_no_glob()
        return self._fs.open(self._fs_path, *args, **kwargs)


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
        except Exception:  # pragma: no cover
            pass
    _FORMAT_SAVE[p.file_format](df, p, **kwargs)


def _get_single_files(
    fp: Iterable[FileParser], fs: Optional[AbstractFileSystem]
) -> Iterable[FileParser]:
    for f in fp:
        yield from f.find_all()


def _save_parquet(df: LocalDataFrame, p: FileParser, **kwargs: Any) -> None:
    PD_UTILS.to_parquet_friendly(
        df.as_pandas(), partition_cols=kwargs.get("partition_cols", [])
    ).to_parquet(
        p.assert_no_glob().path,
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
        pdf = pd.read_parquet(p.path, **{"engine": "pyarrow", **kwargs})
        return pdf, None
    if isinstance(columns, list):  # column names
        pdf = pd.read_parquet(
            p.path, columns=columns, **{"engine": "pyarrow", **kwargs}
        )
        return pdf, None
    schema = Schema(columns)
    pdf = pd.read_parquet(
        p.path, columns=schema.names, **{"engine": "pyarrow", **kwargs}
    )
    return pdf, schema


def _save_csv(df: LocalDataFrame, p: FileParser, **kwargs: Any) -> None:
    with p.open("w") as f:
        df.as_pandas().to_csv(f, **{"index": False, "header": False, **kwargs})


def _safe_load_csv(p: FileParser, **kwargs: Any) -> pd.DataFrame:
    def load_dir() -> pd.DataFrame:
        dfs: List[pd.DataFrame] = []
        for _p in p.join("*.csv").find_all():  # type: ignore
            with _p.open("r") as f:
                dfs.append(pd.read_csv(f, **kwargs))
        return pd.concat(dfs)

    try:
        with p.open("r") as f:
            return pd.read_csv(f, **kwargs)
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
        pdf = _safe_load_csv(p, **{"index_col": False, "header": 0, **kw})
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
                p,
                **{"index_col": False, "header": None, "names": columns, **kw},
            )
            return pdf, None
        schema = Schema(columns)
        pdf = _safe_load_csv(
            p,
            **{"index_col": False, "header": None, "names": schema.names, **kw},
        )
        return pdf, schema
    else:
        raise NotImplementedError(f"{header} is not supported")


def _save_json(df: LocalDataFrame, p: FileParser, **kwargs: Any) -> None:
    with p.open("w") as f:
        df.as_pandas().to_json(f, **{"orient": "records", "lines": True, **kwargs})


def _safe_load_json(p: FileParser, **kwargs: Any) -> pd.DataFrame:
    kw = {"orient": "records", "lines": True, **kwargs}

    def load_dir() -> pd.DataFrame:
        dfs: List[pd.DataFrame] = []
        for _p in p.join("*.json").find_all():  # type: ignore
            with _p.open("r") as f:
                dfs.append(pd.read_json(f, **kw))
        return pd.concat(dfs)

    try:
        with p.open("r") as f:
            return pd.read_json(f, **kw)
    except (IsADirectoryError, PermissionError):
        return load_dir()


def _load_json(
    p: FileParser, columns: Any = None, **kwargs: Any
) -> Tuple[pd.DataFrame, Any]:
    pdf = _safe_load_json(p, **kwargs).reset_index(drop=True)
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
