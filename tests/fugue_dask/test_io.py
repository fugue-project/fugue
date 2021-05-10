import os

from fugue._utils.io import save_df as pd_save_df
from fugue.dataframe.utils import _df_eq as df_eq
from pytest import mark, raises
from triad.collections.fs import FileSystem
from triad.exceptions import InvalidOperationError

from fugue_dask._io import load_df, save_df
from fugue_dask.dataframe import DaskDataFrame


def test_parquet_io(tmpdir):
    df1 = DaskDataFrame([["1", 2, 3]], "a:str,b:int,c:long")
    df2 = DaskDataFrame([[[1, 2]]], "a:[int]")
    # {a:int} will become {a:long} because pyarrow lib has issue
    df3 = DaskDataFrame([[dict(a=1)]], "a:{a:long}")
    for df in [df1, df2, df3]:
        path = os.path.join(tmpdir, "a.parquet")
        save_df(df, path)
        actual = load_df(path)
        df_eq(df, actual, throw=True)

    save_df(df1, path)
    actual = load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    actual = load_df(path, columns="b:str,a:int")
    df_eq(actual, [["2", 1]], "b:str,a:int")
    # can't specify wrong columns
    raises(Exception, lambda: load_df(path, columns="bb:str,a:int"))

    # load directory
    fs = FileSystem()
    for name in ["folder.parquet", "folder"]:
        folder = os.path.join(tmpdir, name)
        fs.makedirs(folder)
        f0 = os.path.join(folder, "_SUCCESS")
        f1 = os.path.join(folder, "1.parquet")
        f2 = os.path.join(folder, "3.parquet")
        fs.touch(f0)
        pd_save_df(df1, f1)
        pd_save_df(df1, f2)

    actual = load_df(folder, "parquet")
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # load multiple paths
    actual = load_df([f1, f2], "parquet")
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # load folder
    actual = load_df(folder, "parquet")
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    actual = load_df(os.path.join(tmpdir, "folder.parquet"))
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # load pattern
    actual = load_df(os.path.join(tmpdir, "folder", "*.parquet"))
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # overwrite folder with single file
    save_df(actual, os.path.join(tmpdir, "folder.parquet"), mode="overwrite")
    actual = load_df(os.path.join(tmpdir, "folder.parquet"))
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # overwrite = False
    raises(FileExistsError, lambda: save_df(df1, f1, mode="error"))
    raises(
        FileExistsError,
        lambda: save_df(df1, os.path.join(tmpdir, "folder.parquet"), mode="error"),
    )

    # wrong mode
    raises(NotImplementedError, lambda: save_df(df1, f1, mode="dummy"))


def test_csv_io(tmpdir):
    fs = FileSystem()
    df1 = DaskDataFrame([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.csv")
    # without header
    save_df(df1, path)
    # assert fs.readtext(path).startswith("1,2,3")
    raises(InvalidOperationError, lambda: load_df(path, header=False))
    actual = load_df(path, columns=["a", "b", "c"], header=False, infer_schema=True)
    assert [[1, 2, 3]] == actual.as_array()
    assert actual.schema == "a:long,b:long,c:long"
    actual = load_df(path, columns="a:double,b:str,c:str", header=False)
    assert [[1.0, "2", "3"]] == actual.as_array()
    assert actual.schema == "a:double,b:str,c:str"
    # with header
    save_df(df1, path, header=True)
    # assert fs.readtext(path).startswith("a,b,c")
    actual = load_df(path, header=True)
    assert [["1", "2", "3"]] == actual.as_array()
    actual = load_df(path, header=True, infer_schema=True)
    assert [[1, 2, 3]] == actual.as_array()
    actual = load_df(path, columns=["b", "a"], header=True, infer_schema=True)
    assert [[2, 1]] == actual.as_array()
    actual = load_df(path, columns="b:str,a:double", header=True)
    assert [["2", 1.0]] == actual.as_array()
    raises(KeyError, lambda: load_df(path, columns="b:str,x:double", header=True))

    raises(
        NotImplementedError, lambda: load_df(path, columns="b:str,x:double", header=2)
    )


def test_json(tmpdir):
    df1 = DaskDataFrame([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.json")
    save_df(df1, path)
    actual = load_df(path)
    df_eq(actual, [[1, 2, 3]], "a:long,b:long,c:long")
    actual = load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    actual = load_df(path, columns="b:str,a:int")
    df_eq(actual, [["2", 1]], "b:str,a:int")
    raises(KeyError, lambda: load_df(path, columns="bb:str,a:int"))


@mark.skip(reason="Unable to test due to spark jars not being downloaded properly")
def test_avro_io(tmpdir):
    df1 = DaskDataFrame([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.avro")
    save_df(df1, path)
    actual = load_df(path)

    df_eq(actual, [["1", 2, 3]], "a:str,b:long,c:long")
    actual = load_df(path, columns=["a", "b"])
    df_eq(actual, [["1", 3]], "a:str,b:long")

    actual = load_df(path, columns="a:str,b:int,c:long")
    df_eq(actual, [["1", 2, 3]], "a:str,b:int,c:long")

    actual = load_df(path, columns=["b", "c"], infer_schema=True)
    df_eq(actual, [[2, 3]], "b:long,c:long")

    # provide schema and columns -> throw error
    raises(
        Exception,
        lambda: save_df(
            path,
            columns="a:str,b:int,c:long",
            schema={
                "type": "record",
                "name": "Root",
                "fields": [
                    {"name": "station", "type": "string"},
                    {"name": "time", "type": "long"},
                    {"name": "temp", "type": "int"},
                ],
            },
        ),
    )

    # provide schema and infer_schema is True -> throw error
    raises(
        Exception,
        lambda: save_df(
            path,
            columns=None,
            schema={
                "type": "record",
                "name": "Root",
                "fields": [
                    {"name": "station", "type": "string"},
                    {"name": "time", "type": "long"},
                    {"name": "temp", "type": "int"},
                ],
            },
            infer_schema=True,
        ),
    )
