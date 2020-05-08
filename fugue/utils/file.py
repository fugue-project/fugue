import pathlib
from typing import Dict, Optional, Callable, Any
from urllib.parse import urlparse

from triad.utils.assertion import assert_or_throw
import pandas as pd

_FORMAT_MAP: Dict[str, str] = {
    ".csv": "csv",
    ".csv.gz": "csv",
    ".parquet": "parquet",
    ".json": "json",
    ".json.gz": "json",
}

_FORMAT_LOAD: Dict[str, Callable[..., pd.DataFrame]] = {
    "csv": lambda p, **kwargs: pd.from_csv(p.uri, **kwargs),
    "parquet": lambda p, **kwargs: pd.from_parquet(p.uri, **kwargs),
    "json": lambda p, **kwargs: pd.from_json(p.uri, **kwargs),
}

_FORMAT_SAVE: Dict[str, Callable] = {
    "csv": lambda df, p, **kwargs: df.to_csv(p.uri, **kwargs),
    "parquet": lambda df, p, **kwargs: df.to_parquet(p.uri, **kwargs),
    "json": lambda df, p, **kwargs: df.to_json(p.uri, **kwargs),
}


class FileParser(object):
    def __init__(self, uri: str, format_hint: Optional[str] = None):
        self._uri = urlparse(uri)
        if format_hint is None or format_hint == "":
            assert_or_throw(
                self.suffix in _FORMAT_MAP,
                NotImplementedError(f"{self.suffix} is not supported"),
            )
            self._format = _FORMAT_MAP[self.suffix]
        else:
            assert_or_throw(
                format_hint in _FORMAT_MAP.values(),
                NotImplementedError(f"{format_hint} is not supported"),
            )
            self._format = format_hint

    @property
    def uri(self) -> str:
        return self._uri.geturl()

    @property
    def scheme(self) -> str:
        return self._uri.scheme

    @property
    def path(self) -> str:
        return self._uri.path

    @property
    def suffix(self) -> str:
        return "".join(pathlib.Path(self.path.lower()).suffixes)

    @property
    def file_format(self) -> str:
        return self._format


def load_pandas(
    uri: str, format_hint: Optional[str] = None, **kwargs: Any
) -> pd.DataFrame:
    p = FileParser(uri, format_hint)
    return _FORMAT_LOAD[p.file_format](p, **kwargs)


def save_pandas(
    df: pd.DataFrame, uri: str, format_hint: Optional[str] = None, **kwargs: Any
) -> None:
    p = FileParser(uri, format_hint)
    _FORMAT_SAVE[p.file_format](df, p, **kwargs)
