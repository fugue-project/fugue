import os
from typing import Any, Optional
from uuid import uuid4

from fugue.collections.partition import PartitionSpec
from fugue.constants import FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH
from fugue.dataframe import DataFrame
from fugue.exceptions import FugueWorkflowCompileError, FugueWorkflowRuntimeError
from fugue.execution.execution_engine import ExecutionEngine
from fugue.workflow.yielded import Yielded
from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid


class Checkpoint(object):
    def __init__(
        self,
        to_file: bool = False,
        deterministic: bool = False,
        permanent: bool = False,
        lazy: bool = False,
        **kwargs: Any,
    ):
        if deterministic:
            assert_or_throw(permanent, "deterministic checkpoint must be permanent")
        self.to_file = to_file
        self.deterministic = deterministic
        self.permanent = permanent
        self.lazy = lazy
        self.kwargs = dict(kwargs)
        self._yield = False
        self._yielded: Optional[Yielded] = None

    def run(self, file_id: str, df: DataFrame, path: "CheckpointPath") -> DataFrame:
        return df

    def set_yield(self) -> None:
        raise FugueWorkflowCompileError(f"yield is not allowed for {self}")

    @property
    def yielded(self) -> Optional[Yielded]:
        return self._yielded

    @property
    def is_null(self) -> bool:
        return True

    def __uuid__(self) -> str:
        # _yield is not a part for determinism
        return to_uuid(
            self.to_file,
            self.deterministic,
            self.lazy,
            self.kwargs,
        )


class FileCheckpoint(Checkpoint):
    def __init__(
        self,
        deterministic: bool,
        permanent: bool,
        lazy: bool = False,
        partition: Any = None,
        single: bool = False,
        namespace: Any = None,
        **save_kwargs: Any,
    ):
        super().__init__(
            to_file=True,
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

    def run(self, file_id: str, df: DataFrame, path: "CheckpointPath") -> DataFrame:
        fpath = path.get_temp_file(file_id, self.deterministic, self.permanent)
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
        if self._yield:
            self._yielded = Yielded(fpath)
        return result

    def set_yield(self) -> None:
        assert_or_throw(
            self.permanent,
            FugueWorkflowCompileError(f"yield is not allowed for {self}"),
        )
        self._yield = True

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
            to_file=False,
            deterministic=False,
            lazy=lazy,
            **kwargs,
        )

    def run(self, file_id: str, df: DataFrame, path: "CheckpointPath") -> DataFrame:
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
        self._temp_path = os.path.join(self._path, execution_id)
        self._fs.makedirs(self._temp_path, recreate=True)
        return self._temp_path

    def remove_temp_path(self):
        if self._temp_path != "":
            try:
                self._fs.removetree(self._temp_path)
            except Exception as e:  # pragma: no cover
                self._log.warn("Unable to remove " + self._temp_path, e)

    def get_temp_file(self, file_id: str, deterministic: bool, permanent: bool) -> str:
        path = self._path if permanent else self._temp_path
        if not deterministic:
            file_id = str(uuid4())
        assert_or_throw(
            path != "",
            FugueWorkflowRuntimeError(
                f"{FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH} is not set"
            ),
        )
        return os.path.join(path, file_id + ".parquet")

    def temp_file_exists(self, path: str) -> bool:
        try:
            return self._fs.exists(path)
        except Exception:  # pragma: no cover
            return False
