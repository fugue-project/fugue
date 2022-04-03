# <img src="./images/logo.svg" width="200">

[![PyPI version](https://badge.fury.io/py/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI license](https://img.shields.io/pypi/l/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![codecov](https://codecov.io/gh/fugue-project/fugue/branch/master/graph/badge.svg?token=ZO9YD5N3IA)](https://codecov.io/gh/fugue-project/fugue)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/4fa5f2f53e6f48aaa1218a89f4808b91)](https://www.codacy.com/gh/fugue-project/fugue/dashboard?utm_source=github.com&utm_medium=referral&utm_content=fugue-project/fugue&utm_campaign=Badge_Grade)
[![Downloads](https://pepy.tech/badge/fugue)](https://pepy.tech/project/fugue)

| Documentation | Tutorials | Chat with us on slack! |
| --- | --- | --- |
| [![Doc](https://readthedocs.org/projects/fugue/badge)](https://fugue.readthedocs.org) | [![Jupyter Book Badge](https://jupyterbook.org/badge.svg)](https://fugue-tutorials.readthedocs.io/) | [![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ) |


**Fugue is a unified interface for distributed computing that lets users execute Python, pandas, and SQL code on Spark and Dask without rewrites**. It is meant for:

*   Data scientists/analysts who want to **focus on defining logic rather than worrying about execution**
*   SQL-lovers wanting to use **SQL to define end-to-end workflows** in pandas, Spark, and Dask
*   Data scientists using pandas wanting to take advantage of **Spark or Dask** with minimal effort
*   Big data practitioners finding **testing code** to be costly and slow
*   Data teams with big data projects that **struggle maintaining code**

For a more comprehensive overview of Fugue, read [this](https://towardsdatascience.com/introducing-fugue-reducing-pyspark-developer-friction-a702230455de) article.

## Select Features

*   **Cross-framework code**: Write code once in native Python, SQL, or pandas then execute it on Dask or Spark with no rewrites. Logic and execution are decoupled through Fugue, enabling users to leverage the Spark and Dask engines without learning the specific framework syntax.
*   **Rapid iterations for big data projects**: Test code on smaller data, then reliably scale to Dask or Spark when ready. This accelerates project iteration time and reduces expensive mistakes.
*   **Friendlier interface for Spark**: Users can get Python/pandas code running on Spark with significantly less effort compared to PySpark. FugueSQL extends SparkSQL to be a more complete programming language.
*   **Highly testable code**: Fugue makes logic more testable because all code is written in native Python. Unit tests scale seamlessly from local workflows to distributed computing workflows.

## Fugue Transform

The simplest way to use Fugue is the [`transform()` function](https://fugue-tutorials.readthedocs.io/tutorials/beginner/introduction.html#fugue-transform). This lets users parallelize the execution of a single function by bringing it to Spark or Dask. In the example below, the `map_letter_to_food()` function takes in a mapping and applies it on a column. This is just pandas and Python so far (without Fugue).

```python
import pandas as pd
from typing import Dict

input_df = pd.DataFrame({"id":[0,1,2], "value": (["A", "B", "C"])})
map_dict = {"A": "Apple", "B": "Banana", "C": "Carrot"}

def map_letter_to_food(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df["value"] = df["value"].map(mapping)
    return df
```

Now, the `map_letter_to_food()` function is brought to the Spark execution engine by invoking the `transform` function of Fugue. The output `schema`, `params` and `engine` are passed to the `transform()` call. The `schema` is needed because it's a requirement on Spark. A schema of `"*"` below means all input columns are in the output.

```python
from fugue import transform
import fugue_spark

df = transform(input_df,
               map_letter_to_food,
               schema="*",
               params=dict(mapping=map_dict),
               engine="spark"
            )
df.show()
```
```rst
+---+------+
| id| value|
+---+------+
|  0| Apple|
|  1|Banana|
|  2|Carrot|
+---+------+
```

<details>
  <summary>PySpark equivalent of Fugue transform</summary>

  ```python
from typing import Iterator, Union
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, SparkSession

spark_session = SparkSession.builder.getOrCreate()

def mapping_wrapper(dfs: Iterator[pd.DataFrame], mapping):
    for df in dfs:
        yield map_letter_to_food(df, mapping)

def run_map_letter_to_food(input_df: Union[DataFrame, pd.DataFrame], mapping):
    # conversion
    if isinstance(input_df, pd.DataFrame):
        sdf = spark_session.createDataFrame(input_df.copy())
    else:
        sdf = input_df.copy()

    schema = StructType(list(sdf.schema.fields))
    return sdf.mapInPandas(lambda dfs: mapping_wrapper(dfs, mapping),
                            schema=schema)

result = run_map_letter_to_food(input_df, map_dict)
result.show()
  ```
</details>

This syntax is simpler, cleaner, and more maintainable than the PySpark equivalent. At the same time, no edits were made to the original pandas-based function to bring it to Spark. It is still usable on pandas DataFrames. Because the Spark execution engine was used, the returned `df` is now a Spark DataFrame. Fugue `transform()` also supports `DaskExecutionEngine` and the pandas-based `NativeExecutionEngine`.

## [FugueSQL](https://fugue-tutorials.readthedocs.io/tutorials/fugue_sql/index.html)

A SQL-based language capable of expressing end-to-end workflows. The `map_letter_to_food()` function above is used in the SQL expression below. This is how to use a Python-defined transformer along with the standard SQL `SELECT` statement.

```python
from fugue_sql import fsql
import json

query = """
    SELECT id, value FROM input_df
    TRANSFORM USING map_letter_to_food(mapping={{mapping}}) SCHEMA *
    PRINT
    """
map_dict_str = json.dumps(map_dict)

fsql(query,mapping=map_dict_str).run()
```

For FugueSQL, we can change the engine by passing it to the `run()` method: `fsql(query,mapping=map_dict_str).run("spark")`.

## Jupyter Notebook Extension

There is an accompanying notebook extension for FugueSQL that lets users use the `%%fsql` cell magic. The extension also provides syntax highlighting for FugueSQL cells. (Syntax highlighting is not available yet for JupyterLab).

![FugueSQL gif](https://miro.medium.com/max/700/1*6091-RcrOPyifJTLjo0anA.gif)

The notebook environment can be setup by using the `setup()` function as follows in the first cell of a notebook:

```python
from fugue_notebook import setup
setup()
```

Note that you can automatically load `fugue_notebook` iPython extension at startup,
read [this](https://ipython.readthedocs.io/en/stable/config/extensions/#using-extensions) to configure your Jupyter environment.


## Installation

Fugue can be installed through pip by using:

```bash
pip install fugue
```

It also has the following extras:

*   **sql**: to support [FugueSQL](https://fugue-tutorials.readthedocs.io/tutorials/fugue_sql/index.html)
*   **spark**: to support Spark as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/tutorials/advanced/execution_engine.html)
*   **dask**: to support Dask as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/tutorials/advanced/execution_engine.html)
*   **all**: install everything above

For example a common use case is:

```bash
pip install fugue[sql,spark]
```

To install the notebook extension (after installing Fugue):

```bash
jupyter nbextension install --py fugue_notebook
jupyter nbextension enable fugue_notebook --py
```

## [Getting Started](https://fugue-tutorials.readthedocs.io/)

The best way to get started with Fugue is to work through the [tutorials](https://fugue-tutorials.readthedocs.io/).

The tutorials can also be run in an interactive notebook environment through binder or Docker:

### Using binder

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

**Note it runs slow on binder** because the machine on binder isn't powerful enough for a distributed framework such as Spark. Parallel executions can become sequential, so some of the performance comparison examples will not give you the correct numbers.

### Using Docker

Alternatively, you should get decent performance by running this Docker image on your own machine:

```bash
docker run -p 8888:8888 fugueproject/tutorials:latest
```

For the API docs, [click here](https://fugue.readthedocs.org)

## Ecosystem

By being an abstraction layer, Fugue can be used with a lot of other open-source projects seamlessly.

Fugue can use the following projects as backends:

*   [Spark](https://github.com/apache/spark)
*   [Dask](https://github.com/dask/dask)
*   [Duckdb](https://github.com/duckdb/duckdb) - in-process SQL OLAP database management
*   [Ibis](https://github.com/ibis-project/ibis/) - pandas-like interface for SQL engines
*   [blazing-sql](https://github.com/BlazingDB/blazingsql) - GPU accelerated SQL engine based on cuDF
*   [dask-sql](https://github.com/dask-contrib/dask-sql) - SQL interface for Dask

Fugue is available as a backend or can integrate with the following projects:

*   [PyCaret](https://github.com/pycaret/pycaret) - low code machine learning
*   [Pandera](https://github.com/pandera-dev/pandera) - data validation


## Further Resources

View some of our latest conferences presentations and content. For a more complete list, check the [Resources](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/resources.html) page in the tutorials.

### Case Studies

*   [How LyftLearn Democratizes Distributed Compute through Kubernetes Spark and Fugue](https://eng.lyft.com/how-lyftlearn-democratizes-distributed-compute-through-kubernetes-spark-and-fugue-c0875b97c3d9)

### Blogs

*   [Introducing Fugue - Reducing PySpark Developer Friction](https://towardsdatascience.com/introducing-fugue-reducing-pyspark-developer-friction-a702230455de)
*   [Introducing FugueSQL — SQL for Pandas, Spark, and Dask DataFrames (Towards Data Science by Khuyen Tran)](https://towardsdatascience.com/introducing-fuguesql-sql-for-pandas-spark-and-dask-dataframes-63d461a16b27)
*   [Interoperable Python and SQL in Jupyter Notebooks (Towards Data Science)](https://towardsdatascience.com/interoperable-python-and-sql-in-jupyter-notebooks-86245e711352)
*   [Using Pandera on Spark for Data Validation through Fugue (Towards Data Science)](https://towardsdatascience.com/using-pandera-on-spark-for-data-validation-through-fugue-72956f274793)

### Conferences

*   [Large Scale Data Validation with Spark and Dask (PyCon US)](https://www.youtube.com/watch?v=2AdvBgjO_3Q)
*   [FugueSQL - The Enhanced SQL Interface for Pandas, Spark, and Dask DataFrames (PyData Global)](https://www.youtube.com/watch?v=OBpnGYjNBBI)
*   [Scaling Machine Learning Workflows to Big Data with Fugue (KubeCon)](https://www.youtube.com/watch?v=fDIRMiwc0aA)

## Community and Contributing

Feel free to message us on [Slack](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ). We also have [contributing instructions](CONTRIBUTING.md).
