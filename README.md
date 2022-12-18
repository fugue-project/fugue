# <img src="./images/logo.svg" width="200">

[![PyPI version](https://badge.fury.io/py/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI license](https://img.shields.io/pypi/l/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![codecov](https://codecov.io/gh/fugue-project/fugue/branch/master/graph/badge.svg?token=ZO9YD5N3IA)](https://codecov.io/gh/fugue-project/fugue)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/4fa5f2f53e6f48aaa1218a89f4808b91)](https://www.codacy.com/gh/fugue-project/fugue/dashboard?utm_source=github.com&utm_medium=referral&utm_content=fugue-project/fugue&utm_campaign=Badge_Grade)
[![Downloads](https://pepy.tech/badge/fugue)](https://pepy.tech/project/fugue)

| Tutorials | API Documentation | Chat with us on slack! |
| --- | --- | --- |
| [![Jupyter Book Badge](https://jupyterbook.org/badge.svg)](https://fugue-tutorials.readthedocs.io/) | [![Doc](https://readthedocs.org/projects/fugue/badge)](https://fugue.readthedocs.org)  | [![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai) |


**Fugue is a unified interface for distributed computing that lets users execute Python, pandas, and SQL code on Spark, Dask, and Ray with minimal rewrites**.

The most common use cases are:

*   **Parallelizing or scaling existing Python and pandas code** by bringing it to Spark, Dask, or Ray with minimal rewrites.
*   Using [FugueSQL](https://fugue-tutorials.readthedocs.io/tutorials/quick_look/ten_minutes_sql.html) to **define end-to-end workflows** on top of pandas, Spark, and Dask DataFrames. FugueSQL is an enhanced SQL interface that can invoke Python code with added keywords.
*   Maintaining one codebase for pandas, Spark, Dask, and Ray projects. **Logic and execution are decoupled through Fugue**, enabling users to be focused on their business logic rather than writing framework-specific code.
*   Improving iteration speed of big data projects. Fugue seamlessly scales execution to big data after local development and testing. By removing PySpark code, unit tests can be written in Python or pandas and ran quickly without spinning up a cluster.

For a more comprehensive overview of Fugue, read [this](https://towardsdatascience.com/introducing-fugue-reducing-pyspark-developer-friction-a702230455de) article.

## Fugue transform()

The simplest way to use Fugue is the [`transform()` function](https://fugue-tutorials.readthedocs.io/tutorials/beginner/transform.html). This lets users parallelize the execution of a single function by bringing it to Spark, Dask, or Ray. In the example below, the `map_letter_to_food()` function takes in a mapping and applies it on a column. This is just pandas and Python so far (without Fugue).

```python
import pandas as pd
from typing import Dict

input_df = pd.DataFrame({"id":[0,1,2], "value": (["A", "B", "C"])})
map_dict = {"A": "Apple", "B": "Banana", "C": "Carrot"}

def map_letter_to_food(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df["value"] = df["value"].map(mapping)
    return df
```

Now, the `map_letter_to_food()` function is brought to the Spark execution engine by invoking the `transform()` function of Fugue. The output `schema`, `params` and `engine` are passed to the `transform()` call. The `schema` is needed because it's a requirement for distributed frameworks. A schema of `"*"` below means all input columns are in the output.

```python
from pyspark.sql import SparkSession
from fugue import transform

spark = SparkSession.builder.getOrCreate()

df = transform(input_df,
               map_letter_to_food,
               schema="*",
               params=dict(mapping=map_dict),
               engine=spark
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
  <summary>PySpark equivalent of Fugue transform()</summary>

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

This syntax is simpler, cleaner, and more maintainable than the PySpark equivalent. At the same time, no edits were made to the original pandas-based function to bring it to Spark. It is still usable on pandas DataFrames. Because the Spark execution engine was used, the returned `df` is now a Spark DataFrame. Fugue `transform()` also supports Dask and Ray as execution engines alongside the default pandas-based engine.

## [FugueSQL](https://fugue-tutorials.readthedocs.io/tutorials/fugue_sql/index.html)

FugueSQL is a SQL-based language capable of expressing end-to-end data workflows. The `map_letter_to_food()` function above is used in the SQL expression below. This is how to use a Python-defined function along with the standard SQL `SELECT` statement.

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

For FugueSQL, we can change the engine by passing it to the `run()` method: `fsql(query,mapping=map_dict_str).run(spark)`.

## Installation

Fugue can be installed through pip or conda. For example:

```bash
pip install fugue
```

It also has the following installation extras:

*   **spark**: to support Spark as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/tutorials/advanced/execution_engine.html)
*   **dask**: to support Dask as the ExecutionEngine.
*   **ray**: to support Ray as the ExecutionEngine.
*   **duckdb**: to support DuckDB as the ExecutionEngine, read [details](https://fugue-tutorials.readthedocs.io/tutorials/integrations/backends/duckdb.html).
*   **ibis**: to enable Ibis for Fugue workflows, read [details](https://fugue-tutorials.readthedocs.io/tutorials/integrations/backends/ibis.html).
*   **cpp_sql_parser**: to enable the CPP antlr parser for Fugue SQL. It can be 50+ times faster than the pure Python parser. For the main Python versions and platforms, there is already pre-built binaries, but for the remaining, it needs a C++ compiler to build on the fly.

For example a common use case is:

```bash
pip install fugue[duckdb,spark]
```

Note if you already installed Spark or DuckDB independently, Fugue is able to automatically use them without installing the extras.

## [Getting Started](https://fugue-tutorials.readthedocs.io/)

The best way to get started with Fugue is to work through the 10 minute tutorials:

*   [Fugue in 10 minutes](https://fugue-tutorials.readthedocs.io/tutorials/quick_look/ten_minutes.html)
*   [FugueSQL in 10 minutes](https://fugue-tutorials.readthedocs.io/tutorials/quick_look/ten_minutes_sql.html)

The [tutorials](https://fugue-tutorials.readthedocs.io/) can also be run in an interactive notebook environment through binder or Docker:

### Using binder

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

**Note it runs slow on binder** because the machine on binder isn't powerful enough for a distributed framework such as Spark. Parallel executions can become sequential, so some of the performance comparison examples will not give you the correct numbers.

### Using Docker

Alternatively, you should get decent performance by running this Docker image on your own machine:

```bash
docker run -p 8888:8888 fugueproject/tutorials:latest
```


## Jupyter Notebook Extension

There is an accompanying [notebook extension](https://pypi.org/project/fugue-jupyter/) for FugueSQL that lets users use the `%%fsql` cell magic. The extension also provides syntax highlighting for FugueSQL cells. It works for both classic notebook and Jupyter Lab. More details can be found in the [installation instructions](https://github.com/fugue-project/fugue-jupyter#install).

![FugueSQL gif](https://miro.medium.com/max/700/1*6091-RcrOPyifJTLjo0anA.gif)


## Ecosystem

By being an abstraction layer, Fugue can be used with a lot of other open-source projects seamlessly.

Python backends:

*   [Pandas](https://github.com/pandas-dev/pandas)
*   [Spark](https://github.com/apache/spark)
*   [Dask](https://github.com/dask/dask)
*   [Ray](http://github.com/ray-project/ray)
*   [Ibis](https://github.com/ibis-project/ibis/)

FugueSQL backends:

*   Pandas - FugueSQL can run on Pandas
*   [Duckdb](https://github.com/duckdb/duckdb) - in-process SQL OLAP database management
*   [dask-sql](https://github.com/dask-contrib/dask-sql) - SQL interface for Dask
*   SparkSQL


Fugue is available as a backend or can integrate with the following projects:

*   [WhyLogs](https://whylogs.readthedocs.io/en/latest/examples/integrations/Fugue_Profiling.html?highlight=fugue) - data profiling
*   [PyCaret](https://fugue-tutorials.readthedocs.io/tutorials/integrations/ecosystem/pycaret.html) - low code machine learning
*   [Nixtla](https://fugue-tutorials.readthedocs.io/tutorials/integrations/ecosystem/nixtla.html) - timeseries modelling
*   [Prefect](https://fugue-tutorials.readthedocs.io/tutorials/integrations/ecosystem/prefect.html) - workflow orchestration
*   [Pandera](https://fugue-tutorials.readthedocs.io/tutorials/integrations/ecosystem/pandera.html) - data validation


## Further Resources

View some of our latest conferences presentations and content. For a more complete list, check the [Content](https://fugue-tutorials.readthedocs.io/tutorials/resources/content.html) page in the tutorials.

### Case Studies

*   [How LyftLearn Democratizes Distributed Compute through Kubernetes Spark and Fugue](https://eng.lyft.com/how-lyftlearn-democratizes-distributed-compute-through-kubernetes-spark-and-fugue-c0875b97c3d9)

### Blogs

*   [Why Pandas-like Interfaces are Sub-optimal for Distributed Computing](https://towardsdatascience.com/why-pandas-like-interfaces-are-sub-optimal-for-distributed-computing-322dacbce43)
*   [Interoperable Python and SQL in Jupyter Notebooks (Towards Data Science)](https://towardsdatascience.com/interoperable-python-and-sql-in-jupyter-notebooks-86245e711352)
*   [Introducing Fugue - Reducing PySpark Developer Friction](https://towardsdatascience.com/introducing-fugue-reducing-pyspark-developer-friction-a702230455de)
*   [Introducing FugueSQL — SQL for Pandas, Spark, and Dask DataFrames (Towards Data Science by Khuyen Tran)](https://towardsdatascience.com/introducing-fuguesql-sql-for-pandas-spark-and-dask-dataframes-63d461a16b27)
*   [Using Pandera on Spark for Data Validation through Fugue (Towards Data Science)](https://towardsdatascience.com/using-pandera-on-spark-for-data-validation-through-fugue-72956f274793)

### Conferences

*   [Distributed Machine Learning at Lyft](https://www.youtube.com/watch?v=_IVyIOV0LgY)
*   [Comparing the Different Ways to Scale Python and Pandas Code](https://www.youtube.com/watch?v=b3ae0m_XTys)
*   [Large Scale Data Validation with Spark and Dask (PyCon US)](https://www.youtube.com/watch?v=2AdvBgjO_3Q)
*   [FugueSQL - The Enhanced SQL Interface for Pandas, Spark, and Dask DataFrames (PyData Global)](https://www.youtube.com/watch?v=OBpnGYjNBBI)
*   [Distributed Hybrid Parameter Tuning](https://www.youtube.com/watch?v=_GBjqskD8Qk)

## Community and Contributing

Feel free to message us on [Slack](http://slack.fugue.ai). We also have [contributing instructions](CONTRIBUTING.md).
