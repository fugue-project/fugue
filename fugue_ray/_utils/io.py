import os
import pathlib
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import pyarrow as pa
import ray.data as rd
from packaging import version
from pyarrow import csv as pacsv
from pyarrow import json as pajson

from triad.collections import Schema
from triad.collections.dict import ParamDict
from triad.utils.assertion import assert_or_throw
from triad.utils.io import exists, makedirs, rm, isfile

from fugue import ExecutionEngine
from fugue._utils.io import FileParser, save_df
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame
from fugue_ray.dataframe import RayDataFrame

from .._constants import RAY_VERSION

try:
    from ray.data.datasource import FileExtensionFilter

    class _FileFiler(FileExtensionFilter):  # pragma: no cover
        def __init__(
            self, file_extensions: Union[str, List[str]], exclude: Iterable[str]
        ):
            super().__init__(file_extensions, allow_if_no_extension=True)
            self._exclude = set(exclude)

        def _is_valid(self, path: str) -> bool:
            return pathlib.Path(
                path
            ).name not in self._exclude and self._file_has_extension(path)

        def __call__(self, paths: List[str]) -> List[str]:
            return [path for path in paths if self._is_valid(path)]

except ImportError:  # pragma: no cover
    pass  # ray >=2.10


class RayIO(object):
    def __init__(self, engine: ExecutionEngine):
        self._engine = engine
        self._logger = engine.log
        self._loads: Dict[str, Callable[..., DataFrame]] = {
            "csv": self._load_csv,
            "parquet": self._load_parquet,
            "json": self._load_json,
        }
        self._saves: Dict[str, Callable[..., None]] = {
            "csv": self._save_csv,
            "parquet": self._save_parquet,
            "json": self._save_json,
        }

    def load_df(
        self,
        uri: Union[str, List[str]],
        format_hint: Optional[str] = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:
        if isinstance(uri, str):
            fp = [FileParser(uri, format_hint)]
        else:
            fp = [FileParser(u, format_hint) for u in uri]
        fmts = list(set(f.file_format for f in fp))  # noqa: C401
        assert_or_throw(
            len(fmts) == 1, NotImplementedError("can't support multiple formats")
        )
        fmt = fmts[0]
        files = [f.as_dir_path() if f.is_dir else f.path for f in fp]
        return self._loads[fmt](files, columns, **kwargs)

    def save_df(
        self,
        df: RayDataFrame,
        uri: str,
        format_hint: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        mode: str = "overwrite",
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:
        partition_spec = partition_spec or PartitionSpec()
        if exists(uri):
            assert_or_throw(mode == "overwrite", FileExistsError(uri))
            try:
                rm(uri, recursive=True)
            except Exception:  # pragma: no cover
                pass
        p = FileParser(uri, format_hint)
        if not force_single:
            df = self._prepartition(df, partition_spec=partition_spec)

            self._saves[p.file_format](df=df, uri=p.path, **kwargs)
        else:
            ldf = df.as_local()
            makedirs(os.path.dirname(uri), exist_ok=True)
            save_df(ldf, uri, format_hint=format_hint, mode=mode, **kwargs)

    def _save_parquet(
        self,
        df: RayDataFrame,
        uri: str,
        **kwargs: Any,
    ) -> None:
        df.native.write_parquet(uri, ray_remote_args=self._remote_args(), **kwargs)

    def _save_csv(
        self,
        df: RayDataFrame,
        uri: str,
        **kwargs: Any,
    ) -> None:
        kw = dict(kwargs)
        if "header" in kw:
            kw["include_header"] = kw.pop("header")

        def _fn() -> Dict[str, Any]:  # pragma: no cover
            return dict(write_options=pacsv.WriteOptions(**kw))

        df.native.write_csv(
            uri, ray_remote_args=self._remote_args(), arrow_csv_args_fn=_fn
        )

    def _save_json(
        self,
        df: RayDataFrame,
        uri: str,
        **kwargs: Any,
    ) -> None:
        df.native.write_json(uri, ray_remote_args=self._remote_args(), **kwargs)

    def _prepartition(
        self, rdf: RayDataFrame, partition_spec: PartitionSpec
    ) -> RayDataFrame:
        # will not use bucket by because here we don't save as table
        by = partition_spec.partition_by
        if len(by) > 0:  # pragma: no cover
            self._logger.warning(
                f"prepartitioning by keys {by} is not supported by ray, will ignore"
            )
        return self._engine.repartition(
            rdf, partition_spec=partition_spec  # type: ignore
        )

    def _load_parquet(
        self, p: List[str], columns: Any = None, **kwargs: Any
    ) -> DataFrame:
        # in 2.52.0 the default changes to ["parquet"]
        if "file_extensions" not in kwargs:
            kwargs = kwargs.copy()
            kwargs["file_extensions"] = None
        sdf = rd.read_parquet(p, ray_remote_args=self._remote_args(), **kwargs)
        if columns is None:
            return RayDataFrame(sdf)
        if isinstance(columns, list):  # column names
            return RayDataFrame(sdf)[columns]
        schema = Schema(columns)
        return RayDataFrame(sdf)[schema.names].alter_columns(schema)

    def _load_csv(  # noqa: C901
        self, p: List[str], columns: Any = None, **kwargs: Any
    ) -> DataFrame:
        kw = ParamDict(kwargs)
        infer_schema = kw.get("infer_schema", False)
        read_options: Dict[str, Any] = {"use_threads": False}
        parse_options: Dict[str, Any] = {}
        convert_options: Dict[str, Any] = {}
        if infer_schema and columns is not None and not isinstance(columns, list):
            raise ValueError("can't set columns as a schema when infer schema is true")

        if RAY_VERSION >= version.parse("2.10"):
            if len(p) == 1 and isfile(p[0]):  # TODO: very hacky
                params: Dict[str, Any] = {}
            else:
                params = {"file_extensions": ["csv"]}
        else:  # pragma: no cover
            params = {
                "partition_filter": _FileFiler(
                    file_extensions=["csv"], exclude=["_SUCCESS"]
                ),
            }

        def _read_csv(to_str: bool) -> RayDataFrame:
            res = rd.read_csv(
                p,
                ray_remote_args=self._remote_args(),
                read_options=pacsv.ReadOptions(**read_options),
                parse_options=pacsv.ParseOptions(**parse_options),
                convert_options=pacsv.ConvertOptions(**convert_options),
                **params,
            )
            if to_str:
                _schema = res.schema(fetch_if_missing=True)
                str_schema = Schema([(x, pa.string()) for x in _schema.names]).pa_schema
                return RayDataFrame(res, schema=str_schema)
            else:
                return RayDataFrame(res)

        header = str(kw.get_or_none("header", bool)).lower()
        if header == "true":
            read_options["autogenerate_column_names"] = False
            df = _read_csv(not infer_schema)
            if columns is None:
                return df
            if isinstance(columns, list):
                return df[columns]
            schema = Schema(columns)
            return df[schema.names].alter_columns(schema)
        elif header in ["false", "none"]:
            read_options["autogenerate_column_names"] = False
            if columns is None:
                raise ValueError("columns must be set if without header")
            if isinstance(columns, list):  # column names
                read_options["column_names"] = columns
                return _read_csv(not infer_schema)
            else:
                schema = Schema(columns)
                read_options["column_names"] = schema.names
                convert_options["column_types"] = schema.pa_schema
                return _read_csv(False)
        else:  # pragma: no cover
            raise NotImplementedError(f"{header} is not supported")

    def _load_json(self, p: List[str], columns: Any = None, **kwargs: Any) -> DataFrame:
        read_options: Dict[str, Any] = {"use_threads": False}
        parse_options: Dict[str, Any] = {}

        def _read_json() -> RayDataFrame:  # pragma: no cover
            if RAY_VERSION >= version.parse("2.10"):
                if len(p) == 1 and isfile(p[0]):  # TODO: very hacky
                    params: Dict[str, Any] = {"file_extensions": None}
                else:
                    params = {"file_extensions": ["json"]}
            elif RAY_VERSION >= version.parse("2.9"):  # pragma: no cover
                params = {
                    "file_extensions": None,
                    "partition_filter": _FileFiler(
                        file_extensions=["json"], exclude=["_SUCCESS"]
                    ),
                }
            else:  # pragma: no cover
                params = {
                    "partition_filter": _FileFiler(
                        file_extensions=["json"], exclude=["_SUCCESS"]
                    ),
                }
            return RayDataFrame(
                rd.read_json(
                    p,
                    ray_remote_args=self._remote_args(),
                    read_options=pajson.ReadOptions(**read_options),
                    parse_options=pajson.ParseOptions(**parse_options),
                    **params,
                )
            )

        if columns is None or isinstance(columns, list):
            rdf = _read_json()
            if isinstance(columns, list):  # column names
                return rdf[columns]
            return rdf
        else:
            schema = Schema(columns)
            return _read_json()[schema.names].alter_columns(schema)

    def _remote_args(self) -> Dict[str, Any]:
        return {"num_cpus": 1}
