import os
from typing import Any, Iterable, List, Optional, Union

from duckdb import DuckDBPyConnection
from triad import ParamDict, Schema

from triad.utils.assertion import assert_or_throw
from triad.utils.io import isdir, makedirs, rm, exists
from fugue._utils.io import FileParser, load_df, save_df
from fugue.collections.sql import TempTableName
from fugue.dataframe import ArrowDataFrame, LocalBoundedDataFrame
from fugue_duckdb._utils import (
    encode_value_to_expr,
    to_duck_type,
    encode_column_name,
    encode_column_names,
)
from fugue_duckdb.dataframe import DuckDataFrame


def _get_files(fp: Iterable[FileParser], fmt: str) -> Iterable[FileParser]:
    for f in fp:
        if not f.has_glob and isdir(f.path):
            yield from f.join("*." + fmt, fmt).find_all()
        else:
            yield f


class DuckDBIO:
    def __init__(self, con: DuckDBPyConnection) -> None:
        self._con = con
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
            return load_df(uri, format_hint=format_hint, columns=columns, **kwargs)
        dfs: List[DuckDataFrame] = []
        for f in _get_files(fp, fp[0].file_format):
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
        if (p.file_format not in self._format_save) or ("partition_cols" in kwargs):
            makedirs(os.path.dirname(uri), exist_ok=True)
            ldf = ArrowDataFrame(df.as_arrow())
            return save_df(ldf, uri=uri, format_hint=format_hint, mode=mode, **kwargs)
        if exists(uri):
            assert_or_throw(mode == "overwrite", FileExistsError(uri))
            try:
                rm(uri, recursive=True)
            except Exception:  # pragma: no cover
                pass
        p.make_parent_dirs()
        self._format_save[p.file_format](df, p, **kwargs)

    def _save_csv(self, df: DuckDataFrame, p: FileParser, **kwargs: Any):
        p.assert_no_glob()
        dn = TempTableName()
        df.native.create_view(dn.key)
        kw = ParamDict({k.lower(): v for k, v in kwargs.items()})
        kw["header"] = 1 if kw.pop("header", False) else 0
        params: List[str] = []
        for k, v in kw.items():
            params.append(f"{k.upper()} " + encode_value_to_expr(v))
        pm = ", ".join(params)
        query = f"COPY {dn.key} TO {encode_value_to_expr(p.path)} WITH ({pm})"
        self._con.execute(query)

    def _load_csv(  # noqa: C901
        self, p: FileParser, columns: Any = None, **kwargs: Any
    ) -> DuckDataFrame:
        kw = ParamDict({k.lower(): v for k, v in kwargs.items()})
        infer_schema = kw.pop("infer_schema", False)
        header = kw.pop("header", False)
        assert_or_throw(
            not (columns is None and not header),
            ValueError("when csv has no header, columns must be specified"),
        )
        kw.pop("auto_detect", None)
        params: List[str] = [encode_value_to_expr(p.path)]
        kw["header"] = 1 if header else 0
        kw["auto_detect"] = 1 if infer_schema else 0
        if infer_schema:
            for k, v in kw.items():
                params.append(f"{k}=" + encode_value_to_expr(v))
            pm = ", ".join(params)
            if header:
                if columns is None:
                    cols = "*"
                elif isinstance(columns, list):
                    cols = ", ".join(encode_column_names(columns))
                else:
                    raise ValueError(
                        "columns can't be schema when infer_schema is true"
                    )
                query = f"SELECT {cols} FROM read_csv_auto({pm})"
                return DuckDataFrame(self._con.from_query(query))
            else:
                if isinstance(columns, list):
                    pass
                else:
                    raise ValueError(
                        "columns can't be schema when infer_schema is true"
                    )
                query = f"SELECT * FROM read_csv_auto({pm})"
                tdf = DuckDataFrame(self._con.from_query(query))
                rn = dict(zip(tdf.columns, columns))
                return tdf.rename(rn)  # type: ignore
        else:
            if header:
                kw["ALL_VARCHAR"] = 1
                kw["auto_detect"] = 1
                if columns is None:
                    cols = "*"
                elif isinstance(columns, list):
                    cols = ", ".join(encode_column_names(columns))
                else:
                    cols = "*"
                for k, v in kw.items():
                    params.append(f"{k}=" + encode_value_to_expr(v))
                pm = ", ".join(params)
                query = f"SELECT {cols} FROM read_csv_auto({pm})"
                res = DuckDataFrame(self._con.from_query(query))
                if isinstance(columns, list):
                    res = res[columns]  # type: ignore
                elif columns is not None:
                    res = res[Schema(columns).names].alter_columns(  # type: ignore
                        columns
                    )
                return res
            else:
                if isinstance(columns, list):
                    schema = Schema([(x, str) for x in columns])
                else:
                    schema = Schema(columns)
                kw["columns"] = {f.name: to_duck_type(f.type) for f in schema.fields}
                for k, v in kw.items():
                    params.append(encode_column_name(k) + "=" + encode_value_to_expr(v))
                pm = ", ".join(params)
                query = f"SELECT * FROM read_csv({pm})"
                return DuckDataFrame(self._con.from_query(query))

    def _save_parquet(self, df: DuckDataFrame, p: FileParser, **kwargs: Any):
        p.assert_no_glob()
        dn = TempTableName()
        df.native.create_view(dn.key)
        kw = ParamDict({k.lower(): v for k, v in kwargs.items()})
        kw["format"] = "parquet"
        params: List[str] = []
        for k, v in kw.items():
            params.append(f"{k.upper()} " + encode_value_to_expr(v))
        pm = ", ".join(params)
        query = f"COPY {dn.key} TO {encode_value_to_expr(p.path)}"
        if len(params) > 0:
            query += f" WITH ({pm})"
        self._con.execute(query)

    def _load_parquet(
        self, p: FileParser, columns: Any = None, **kwargs: Any
    ) -> DuckDataFrame:
        kw = ParamDict({k.lower(): v for k, v in kwargs.items()})
        params: List[str] = [encode_value_to_expr(p.path)]
        if isinstance(columns, list):
            cols = ", ".join(encode_column_names(columns))
        else:
            cols = "*"
        assert_or_throw(
            len(kw) == 0,
            NotImplementedError("can't take extra parameters for loading parquet"),
        )
        # for k, v in kw.items():
        #    params.append(f"{k}=" + encode_value_to_expr(v))
        pm = ", ".join(params)
        query = f"SELECT {cols} FROM parquet_scan([{pm}])"
        res = DuckDataFrame(self._con.from_query(query))
        return (
            res  # type: ignore
            if isinstance(columns, list) or columns is None
            else res.alter_columns(columns)
        )
