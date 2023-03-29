from typing import Any

import fs as pfs
from fugue.collections.partition import PartitionSpec
from fugue.collections.yielded import PhysicalYielded
from fugue.constants import FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH
from fugue.dataframe import DataFrame
from fugue.exceptions import FugueWorkflowCompileError, FugueWorkflowRuntimeError
from fugue.execution.execution_engine import ExecutionEngine
from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid


class Checkpoint(object):
    def __init__(
        self,
        deterministic: bool = False,
        permanent: bool = False,
        lazy: bool = False,
        **kwargs: Any,
    ):
        if deterministic:
            assert_or_throw(permanent, "deterministic checkpoint must be permanent")
        self.deterministic = deterministic
        self.permanent = permanent
        self.lazy = lazy
        self.kwargs = dict(kwargs)

    def run(self, df: DataFrame, path: "CheckpointPath") -> DataFrame:
        return df

    @property
    def is_null(self) -> bool:
        return True


class StrongCheckpoint(Checkpoint):
    def __init__(
        self,
        storage_type: str,
        obj_id: str,
        deterministic: bool,
        permanent: bool,
        lazy: bool = False,
        partition: Any = None,
        single: bool = False,
        namespace: Any = None,
        **save_kwargs: Any,
    ):
        super().__init__(
            deterministic=deterministic,
            permanent=permanent,
            lazy=lazy,
            fmt="",
            partition=PartitionSpec(partition),
            single=single,
            namespace=namespace,
            save_kwargs=dict(save_kwargs),
        )
        self._yield_func: Any = None
        self._obj_id = to_uuid(obj_id, namespace)
        self._yielded = PhysicalYielded(self._obj_id, storage_type=storage_type)

    def run(self, df: DataFrame, path: "CheckpointPath") -> DataFrame:
        if self._yielded.storage_type == "file":
            fpath = path.get_temp_file(self._obj_id, self.permanent)
            if not self.deterministic or not path.temp_file_exists(fpath):
                path.execution_engine.save_df(
                    df=df,
                    path=fpath,
                    format_hint=self.kwargs["fmt"],
                    mode="overwrite",
                    partition_spec=self.kwargs["partition"],
                    force_single=self.kwargs["single"],
                    **self.kwargs["save_kwargs"],
                )
            result = path.execution_engine.load_df(
                path=fpath, format_hint=self.kwargs["fmt"]
            )
            self._yielded.set_value(fpath)
        else:
            tb = path.get_table_name(self._obj_id, self.permanent)
            if (
                not self.deterministic
                or not path.execution_engine.sql_engine.table_exists(tb)
            ):
                path.execution_engine.sql_engine.save_table(
                    df=df,
                    table=tb,
                    partition_spec=self.kwargs["partition"],
                    **self.kwargs["save_kwargs"],
                )
            result = path.execution_engine.sql_engine.load_table(tb)
            self._yielded.set_value(tb)
        return result

    @property
    def yielded(self) -> PhysicalYielded:
        assert_or_throw(
            self.permanent,
            lambda: FugueWorkflowCompileError(f"yield is not allowed for {self}"),
        )
        return self._yielded

    @property
    def is_null(self) -> bool:
        return False


class WeakCheckpoint(Checkpoint):
    def __init__(
        self,
        lazy: bool = False,
        **kwargs: Any,
    ):
        super().__init__(
            deterministic=False,
            lazy=lazy,
            **kwargs,
        )

    def run(self, df: DataFrame, path: "CheckpointPath") -> DataFrame:
        return path.execution_engine.persist(df, lazy=self.lazy, **self.kwargs)

    @property
    def is_null(self) -> bool:
        return False


class CheckpointPath(object):
    def __init__(self, engine: ExecutionEngine):
        self._engine = engine
        self._fs = engine.fs
        self._log = engine.log
        self._path = engine.conf.get(FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH, "").strip()
        self._temp_path = ""

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._engine

    def init_temp_path(self, execution_id: str) -> str:
        if self._path == "":
            self._temp_path = ""
            return ""
        self._temp_path = pfs.path.combine(self._path, execution_id)
        self._fs.makedirs(self._temp_path, recreate=True)
        return self._temp_path

    def remove_temp_path(self):
        if self._temp_path != "":
            try:
                self._fs.removetree(self._temp_path)
            except Exception as e:  # pragma: no cover
                self._log.info("Unable to remove " + self._temp_path, e)

    def get_temp_file(self, obj_id: str, permanent: bool) -> str:
        path = self._path if permanent else self._temp_path
        assert_or_throw(
            path != "",
            FugueWorkflowRuntimeError(
                f"{FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH} is not set"
            ),
        )
        return pfs.path.combine(path, obj_id + ".parquet")

    def get_table_name(self, obj_id: str, permanent: bool) -> str:
        path = self._path if permanent else self._temp_path
        return "temp_" + to_uuid(path, obj_id)[:5]

    def temp_file_exists(self, path: str) -> bool:
        try:
            return self._fs.exists(path)
        except Exception:  # pragma: no cover
            return False
