import os

from fugue.collections.partition import PartitionSpec
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueDataFrameInitError
from fugue_spark.dataframe import SparkDataFrame
from fugue_spark._utils.convert import to_schema, to_spark_schema
from fugue_spark._utils.io import SparkIO
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pytest import raises
from triad.collections.fs import FileSystem
from triad.exceptions import InvalidOperationError


def test_parquet_io(tmpdir, spark_session):
    si = SparkIO(spark_session, FileSystem())
    df1 = _df([["1", 2, 3]], "a:str,b:int,c:long")
    df2 = _df([[[1, 2]]], "a:[int]")
    # {a:int} will become {a:long} because pyarrow lib has issue
    df3 = _df([[dict(a=1)]], "a:{a:long}")
    for df in [df1, df2, df3]:
        path = os.path.join(tmpdir, "a.parquet")
        si.save_df(df, path)
        actual = si.load_df(path)
        df_eq(df, actual, throw=True)

    si.save_df(df1, path)
    actual = si.load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    actual = si.load_df(path, columns="b:str,a:int")
    df_eq(actual, [["2", 1]], "b:str,a:int")
    raises(Exception, lambda: si.load_df(path, columns="bb:str,a:int"))

    # load directory
    fs = FileSystem()
    folder = os.path.join(tmpdir, "folder")
    fs.makedirs(folder)
    f0 = os.path.join(folder, "_SUCCESS")
    f1 = os.path.join(folder, "1.parquet")
    f2 = os.path.join(folder, "3.parquet")
    fs.touch(f0)
    si.save_df(df1, f1, force_single=True)
    si.save_df(df1, f2, force_single=True)
    assert fs.isfile(f1)
    actual = si.load_df(folder, "parquet")
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # load multiple paths
    actual = si.load_df([f1, f2], "parquet")
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")
    actual = si.load_df([f1, f2], "parquet", columns="b:str,a:str")
    df_eq(actual, [["2", "1"], ["2", "1"]], "a:str,b:int,c:long")

    # overwrite = False
    raises(
        (FileExistsError, AnalysisException), lambda: si.save_df(df1, f1, mode="error")
    )
    # wrong mode
    raises(Exception, lambda: si.save_df(df1, f1, mode="dummy"))


def test_csv_io(tmpdir, spark_session):
    fs = FileSystem()
    si = SparkIO(spark_session, fs)
    df1 = _df([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.csv")
    # without header
    si.save_df(df1, path)
    raises(InvalidOperationError, lambda: si.load_df(path, header=False))
    actual = si.load_df(path, columns=["a", "b", "c"], header=False)
    assert [["1", "2", "3"]] == actual.as_array()
    assert actual.schema == "a:str,b:str,c:str"
    actual = si.load_df(path, columns="a:double,b:str,c:str", header=False)
    assert [[1.0, "2", "3"]] == actual.as_array()
    assert actual.schema == "a:double,b:str,c:str"
    # with header
    si.save_df(df1, path, header=True)
    actual = si.load_df(path, header=True)
    assert [["1", "2", "3"]] == actual.as_array()
    actual = si.load_df(path, columns=["b", "a"], header=True)
    assert [["2", "1"]] == actual.as_array()
    actual = si.load_df(path, columns="b:str,a:double", header=True)
    assert [["2", 1.0]] == actual.as_array()
    raises(Exception, lambda: si.load_df(path, columns="b:str,x:double", header=True))

    raises(
        NotImplementedError,
        lambda: si.load_df(path, columns="b:str,x:double", header=2),
    )


def test_json_io(tmpdir, spark_session):
    fs = FileSystem()
    si = SparkIO(spark_session, fs)
    df1 = _df([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.json")
    si.save_df(df1, path)
    actual = si.load_df(path)
    df_eq(actual, [[1, 2, 3]], "a:long,b:long,c:long")
    actual = si.load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    actual = si.load_df(path, columns="b:str,a:int")
    df_eq(actual, [["2", 1]], "b:str,a:int")
    raises(Exception, lambda: si.load_df(path, columns="bb:str,a:int"))


def test_avro_io(tmpdir, spark_session):
    if spark_session.version < "3.0.0":
        return
    fs = FileSystem()
    si = SparkIO(spark_session, fs)
    df1 = _df([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.avro")
    si.save_df(df1, path)
    actual = si.load_df(path)
    df_eq(actual, [["1", 2, 3]], "a:str,b:int,c:long")
    actual = si.load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    actual = si.load_df(path, columns="b:str,a:int")
    df_eq(actual, [["2", 1]], "b:str,a:int")
    raises(Exception, lambda: si.load_df(path, columns="bb:str,a:int"))


def test_save_with_partition(tmpdir, spark_session):
    si = SparkIO(spark_session, FileSystem())
    df1 = _df([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.parquet")
    si.save_df(df1, path, partition_spec=PartitionSpec(num=2))
    actual = si.load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    si.save_df(df1, path, partition_spec=PartitionSpec(by=["a"]))
    actual = si.load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    si.save_df(df1, path, partition_spec=PartitionSpec(by=["a"], num=2))
    actual = si.load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")


def _df(data, schema=None, metadata=None):
    session = SparkSession.builder.getOrCreate()
    if schema is not None:
        pdf = PandasDataFrame(data, to_schema(schema), metadata)
        df = session.createDataFrame(pdf.native, to_spark_schema(schema))
    else:
        df = session.createDataFrame(data)
    return SparkDataFrame(df, schema, metadata)
