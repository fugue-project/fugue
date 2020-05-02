from typing import Any
from unittest import TestCase

import pandas as pd
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import ArrayDataFrame, DataFrame, LocalDataFrame, PandasDataFrame
from fugue.execution.execution_engine import ExecutionEngine
from fugue.transformer import Transformer, transformer
from fugue_test.utils import df_eq


class ExecutionEngineTests(object):
    class Tests(TestCase):
        @classmethod
        def setUpClass(cls):
            cls._engine = cls.make_engine(cls)

        @property
        def engine(self) -> ExecutionEngine:
            return self._engine  # type: ignore

        @classmethod
        def tearDownClass(cls):
            cls._engine.stop()

        def make_engine(self) -> ExecutionEngine:  # pragma: no cover
            raise NotImplementedError

        def test_map_partitions(self):
            def select_top(no, data):
                for x in data:
                    yield x
                    break

            e = self.engine
            a = e.to_df(
                ArrayDataFrame([[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int")
            )
            c = e.map_partitions(a, select_top, a.schema, PartitionSpec())
            df_eq(c, [[1, 2]], "a:double,b:int", throw=True)
            c = e.map_partitions(
                a, select_top, a.schema, PartitionSpec(partition_by=["a"])
            )
            df_eq(c, [[None, 1], [1, 2], [3, 4]], "a:double,b:int", throw=True)
            c = e.map_partitions(
                a,
                select_top,
                a.schema,
                PartitionSpec(partition_by=["a"], presort="b DESC"),
            )
            df_eq(c, [[None, 4], [1, 2], [3, 4]], "a:double,b:int", throw=True)
            c = e.map_partitions(
                a,
                select_top,
                a.schema,
                PartitionSpec(partition_by=["a"], presort="b DESC", num_partitions=3),
            )
            df_eq(c, [[None, 4], [1, 2], [3, 4]], "a:double,b:int", throw=True)

        def test_transform(self):
            e = self.engine
            a = e.to_df(
                ArrayDataFrame(
                    [[1, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int", dict(x=1)
                )
            )
            c = e.transform(
                a,
                MockTransform1,
                dict(p="10"),
                partition_spec=PartitionSpec(),
                ignore_errors=[],
            )
            df_eq(
                c,
                [[1, 2, 4, 10], [None, 1, 4, 10], [3, 4, 4, 10], [None, 4, 4, 10]],
                "a:double,b:int,ct:int,p:int",
                throw=True,
            )
            c = e.transform(
                a,
                MockTransform1,
                dict(p="10"),
                partition_spec=PartitionSpec(partition_by=["a"]),
                ignore_errors=[],
            )
            df_eq(
                c,
                [[None, 1, 2, 10], [None, 4, 2, 10], [1, 2, 1, 10], [3, 4, 1, 10]],
                "a:double,b:int,ct:int,p:int",
                throw=True,
            )
            c = e.transform(
                a,
                MockTransform1,
                dict(p="10"),
                partition_spec=PartitionSpec(partition_by=["a"], presort="b DESC"),
                ignore_errors=[],
            )
            df_eq(
                c,
                [[None, 4, 2, 10], [None, 1, 2, 10], [1, 2, 1, 10], [3, 4, 1, 10]],
                "a:double,b:int,ct:int,p:int",
                throw=True,
            )

            c = e.transform(
                a,
                mock_tf1,
                dict(p=10),
                partition_spec=PartitionSpec(partition_by=["a"], presort="b DESC"),
                ignore_errors=[],
            )
            df_eq(
                c,
                [[None, 4, 2, 10], [None, 1, 2, 10], [1, 2, 1, 10], [3, 4, 1, 10]],
                "a:double,b:int,ct:int,p:int",
                throw=True,
            )


class MockTransform1(Transformer):
    def get_output_schema(self, df: DataFrame) -> Any:
        assert "x" in df.metadata
        return [df.schema, "ct:int,p:int"]

    def init_physical_partition(self, df: LocalDataFrame) -> None:
        assert "x" in df.metadata
        self.pn = self.cursor.physical_partition_no
        self.ks = self.key_schema

    def init_logical_partition(self, df: LocalDataFrame) -> None:
        assert "x" in df.metadata
        self.ln = self.cursor.partition_no

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        assert "x" in df.metadata
        pdf = df.as_pandas()
        pdf["p"] = self.params.get_or_throw("p", int)
        pdf["ct"] = pdf.shape[0]
        return PandasDataFrame(pdf, self.output_schema)


@transformer("*,ct:int,p:int")
def mock_tf1(df: pd.DataFrame, p=1) -> pd.DataFrame:
    df["ct"] = df.shape[0]
    df["p"] = p
    return df
