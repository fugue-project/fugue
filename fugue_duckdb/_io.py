import os
from typing import Any, Iterable, List, Optional, Union

from duckdb import DuckDBPyConnection
from fugue._utils.io import FileParser, load_df, save_df
from fugue.dataframe import ArrowDataFrame, LocalBoundedDataFrame
from fugue_duckdb._utils import encode_value_to_expr, get_temp_df_name
from fugue_duckdb.dataframe import DuckDataFrame
from triad import ParamDict
from triad.collections.fs import FileSystem
from triad.utils.assertion import assert_or_throw


def _get_single_files(
    fp: Iterable[FileParser], fs: FileSystem, fmt: str
) -> Iterable[FileParser]:
    for f in fp:
        if fs.isdir(f.uri):
            yield f.with_glob("*." + fmt, fmt)
        else:
            yield f


class DuckDBIO:
    def __init__(self, fs: FileSystem, con: DuckDBPyConnection) -> None:
        self._con = con
        self._fs = fs
        self._format_load = {"csv": self._load_csv, "parquet": self._load_parquet}
        self._format_save = {"csv": self._save_csv, "parquet": self._save_parquet}

    def load_df(
        self,
        uri: Union[str, List[str]],
        format_hint: Optional[str] = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> LocalBoundedDataFrame:
        for k in kwargs.keys():
            assert_or_throw(k.isidentifier(), ValueError(f"{k} is invalid"))
        if isinstance(uri, str):
            fp = [FileParser(uri, format_hint)]
        else:
            fp = [FileParser(u, format_hint) for u in uri]
        if fp[0].file_format not in self._format_load:
            return load_df(
                uri, format_hint=format_hint, columns=columns, fs=self._fs, **kwargs
            )
        dfs: List[DuckDataFrame] = []
        for f in _get_single_files(fp, self._fs, fp[0].file_format):
            df = self._format_load[f.file_format](f, columns, **kwargs)
            dfs.append(df)
        rel = dfs[0].native
        for i in range(1, len(dfs)):
            rel = rel.union(dfs[i].native)
        return DuckDataFrame(rel)

    def save_df(
        self,
        df: DuckDataFrame,
        uri: str,
        format_hint: Optional[str] = None,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> None:
        for k in kwargs.keys():
            assert_or_throw(k.isidentifier(), ValueError(f"{k} is invalid"))
        assert_or_throw(
            mode in ["overwrite", "error"],
            NotImplementedError(f"{mode} is not supported"),
        )
        p = FileParser(uri, format_hint).assert_no_glob()
        if p.file_format not in self._format_save:
            self._fs.makedirs(os.path.dirname(uri), recreate=True)
            ldf = ArrowDataFrame(df.native.arrow())
            return save_df(
                ldf, uri=uri, format_hint=format_hint, mode=mode, fs=self._fs, **kwargs
            )
        fs = self._fs
        if fs.exists(uri):
            assert_or_throw(mode == "overwrite", FileExistsError(uri))
            try:
                fs.remove(uri)
            except Exception:
                try:
                    fs.removetree(uri)
                except Exception:  # pragma: no cover
                    pass
        if not fs.exists(p.parent):
            fs.makedirs(p.parent, recreate=True)
        self._format_save[p.file_format](df, p, **kwargs)

    def _save_csv(self, df: DuckDataFrame, p: FileParser, **kwargs: Any):
        dn = get_temp_df_name()
        df.native.create_view(dn)
        kw = ParamDict({k.lower(): v for k, v in kwargs.items()})
        kw["header"] = 1 if kw.pop("header", False) else 0
        params: List[str] = []
        for k, v in kw.items():
            params.append(f"{k.upper()} " + encode_value_to_expr(v))
        pm = ", ".join(params)
        query = f"COPY {dn} TO {encode_value_to_expr(p.uri)} WITH ({pm})"
        self._con.execute(query)

    def _load_csv(
        self, p: FileParser, columns: Any = None, **kwargs: Any
    ) -> DuckDataFrame:
        kw = ParamDict({k.lower(): v for k, v in kwargs.items()})
        infer_schema = kw.pop("infer_schema", False)
        header = kw.pop("header", False)
        kw.pop("auto_detect", None)
        params: List[str] = [encode_value_to_expr(p.uri_with_glob)]
        if infer_schema:
            if columns is None:
                cols = "*"
            elif isinstance(columns, list):
                cols = ", ".join(columns)
            else:
                raise ValueError(columns)
            kw["header"] = 1 if header else 0
            kw["auto_detect"] = 1
            for k, v in kw.items():
                params.append(f"{k}=" + encode_value_to_expr(v))
            pm = ", ".join(params)
            query = f"SELECT {cols} FROM read_csv_auto({pm})"
            return DuckDataFrame(self._con.from_query(query))
        else:
            kw["ALL_VARCHAR"] = 1
            kw["header"] = 1 if header else 0
            kw["auto_detect"] = 0
            if columns is None:
                cols = "*"
            else:
                cols = ", ".join(columns) if isinstance(columns, list) else "*"
            for k, v in kw.items():
                params.append(f"{k}=" + encode_value_to_expr(v))
            pm = ", ".join(params)
            query = f"SELECT {cols} FROM read_csv_auto({pm})"
            res = DuckDataFrame(self._con.from_query(query))
            return (
                res  # type: ignore
                if isinstance(columns, list) or columns is None
                else res.alter_columns(columns)
            )

    def _save_parquet(self, df: DuckDataFrame, p: FileParser, **kwargs: Any):
        dn = get_temp_df_name()
        df.native.create_view(dn)
        kw = ParamDict({k.lower(): v for k, v in kwargs.items()})
        kw["format"] = "parquet"
        params: List[str] = []
        for k, v in kw.items():
            params.append(f"{k.upper()} " + encode_value_to_expr(v))
        pm = ", ".join(params)
        query = f"COPY {dn} TO {encode_value_to_expr(p.uri)}"
        if len(params) > 0:
            query += f" WITH ({pm})"
        self._con.execute(query)

    def _load_parquet(
        self, p: FileParser, columns: Any = None, **kwargs: Any
    ) -> DuckDataFrame:
        kw = ParamDict({k.lower(): v for k, v in kwargs.items()})
        params: List[str] = [encode_value_to_expr(p.uri_with_glob)]
        if isinstance(columns, list):
            cols = ", ".join(columns)
        else:
            cols = "*"
        assert_or_throw(
            len(kw) == 0,
            NotImplementedError("can't take extra parameters for loading parquet"),
        )
        # for k, v in kw.items():
        #    params.append(f"{k}=" + encode_value_to_expr(v))
        pm = ", ".join(params)
        query = f"SELECT {cols} FROM parquet_scan({pm})"
        res = DuckDataFrame(self._con.from_query(query))
        return (
            res  # type: ignore
            if isinstance(columns, list) or columns is None
            else res.alter_columns(columns)
        )
