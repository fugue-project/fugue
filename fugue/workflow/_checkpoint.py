import os
from typing import Any

from fugue.dataframe import DataFrame
from fugue.constants import FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH
from fugue.exceptions import FugueWorkflowError
from fugue.execution.execution_engine import ExecutionEngine
from triad.utils.assertion import assert_or_throw
from triad.utils.hash import to_uuid
from fugue.collections.partition import PartitionSpec


class Checkpoint(object):
    def __init__(
        self,
        to_file: bool = False,
        deterministic: bool = False,
        lazy: bool = False,
        **kwargs: Any,
    ):
        self.to_file = to_file
        self.deterministic = deterministic
        self.lazy = lazy
        self.kwargs = dict(kwargs)

    def run(self, file_id: str, df: DataFrame, path: "CheckpointPath") -> DataFrame:
        return df

    def __uuid__(self) -> str:
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
        lazy: bool = False,
        partition: Any = None,
        single: bool = False,
        namespace: Any = None,
        **save_kwargs: Any,
    ):
        super().__init__(
            to_file=True,
            deterministic=deterministic,
            lazy=lazy,
            fmt="",
            partition=PartitionSpec(partition),
            single=single,
            namespace=namespace,
            save_kwargs=dict(save_kwargs),
        )

    def run(self, file_id: str, df: DataFrame, path: "CheckpointPath") -> DataFrame:
        fpath = path.get_temp_file(file_id, self.deterministic)
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
        return path.execution_engine.load_df(path=fpath, format_hint=self.kwargs["fmt"])


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

    def get_temp_file(self, file_id: str, deterministic: bool) -> str:
        path = self._path if deterministic else self._temp_path
        assert_or_throw(
            path != "",
            FugueWorkflowError(f"{FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH} is not set"),
        )
        return os.path.join(path, file_id + ".parquet")

    def temp_file_exists(self, path: str) -> bool:
        try:
            return self._fs.exists(path)
        except Exception:  # pragma: no cover
            return False
