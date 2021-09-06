# <img src="./images/logo.svg" width="200">

[![Doc](https://readthedocs.org/projects/fugue/badge)](https://fugue.readthedocs.org)
[![PyPI version](https://badge.fury.io/py/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI license](https://img.shields.io/pypi/l/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![codecov](https://codecov.io/gh/fugue-project/fugue/branch/master/graph/badge.svg?token=ZO9YD5N3IA)](https://codecov.io/gh/fugue-project/fugue)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/4fa5f2f53e6f48aaa1218a89f4808b91)](https://www.codacy.com/gh/fugue-project/fugue/dashboard?utm_source=github.com&utm_medium=referral&utm_content=fugue-project/fugue&utm_campaign=Badge_Grade)

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

**Fugue is an abstraction layer that helps big data practitioners accelerate development, decrease costs, and simplify maintenance of their projects.**


-   **Framework-agnostic code**: Write code once in native Python or SQL, then port it to Pandas, Dask or Spark with minimal changes. Logic and execution are decoupled as Fugue takes care of bringing the code to distributed computing frameworks.
-   **Rapid iterations for big data projects**: Test code quickly on smaller data, then reliably scale to Dask or Spark when ready. This saves both developer time and hardware expenses.
-   **Friendlier interface for Spark**: Users can get Python/Pandas code running on Spark with significanly less effort. FugueSQL extends SparkSQL to be a more complete programming language.
-   **Highly testable code**: Fugue naturally makes logic more testable because the code will be written in native Python. Unit tests scale seamlessly from local workflows to distributed computing workflows.

## Cross-Framework Execution

The simplest way to use Fugue is the `transform` function. This lets users bring a parallelize the execution of a single function by bringing it to Spark or Dask. In the example below, the `map_to_food` function takes in a mapping and replaces the values in a column with the mapped value.

```python
import pandas as pd
from typing import Dict

df = pd.DataFrame({"id":[0,1,2], "food": (["A", "B", "C"])})
map_dict = {"A": "Apple", "B": "Banana", "C": "Carrot"}

def map_to_food(df: pd.DataFrame, mapping: Dict) -> pd.DataFrame:
    df["food"] = df["food"].map(mapping)
    return df
```

The `map_to_food` function can now be used on the Spark execution engine. This is done by simply invoking the `transform` function of Fugue and passing the output `schema`, params` and `engine`. The `schema` is needed because it's a requirement on Spark.

```python
from fugue import transform
from fugue_spark import SparkExecutionEngine

df = transform(df, 
               replace_food, 
               schema="*",
               params=dict(mapping=map_dict),
               engine=SparkExecutionEngine
            )
df.show()
```
```
+---+------+
| id|  food|
+---+------+
|  0| Apple|
|  1|Banana|
|  2|Carrot|
+---+------+
```

This syntax is simpler, cleaner, and more maintainable than the Spark equivalent. At the same time, no edits were made to the original pandas-based function to bring it to Spark. Because the Spark execution engine was used, the returned `df` is now a Spark DataFrame. Fugue `transform` also supports `DaskExecutionEngine` and the pandas-based `NativeExecutionEngine`.

## Getting Started

View our latest presentations and content

-   [Tutorials](https://fugue-tutorials.readthedocs.io/en/latest/)
-   [Interoperable Python and SQL blog](https://towardsdatascience.com/interoperable-python-and-sql-in-jupyter-notebooks-86245e711352)
-   [Data Science Cross-Framework Library Blog](https://towardsdatascience.com/creating-pandas-and-spark-compatible-functions-with-fugue-8617c0b3d3a8)


### Catch errors faster

Fugue builds a [directed acyclic graph (DAG)](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/advanced/dag.html) before running code, allowing users to receive errors faster. This catches more errors before expensive jobs are run on a cluster. For example, mismatches in specified [schema](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/advanced/schema_dataframes.html#Schema) will raise errors. In the code above, the schema hint comment is read and the schema is enforced during execution. Schema is required for Fugue [extensions](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/extensions.html).

### Spark optimizations

Fugue makes Spark easier to use for people starting with distributed computing. For example, Fugue uses the constructed DAG to smartly [auto-persist](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/advanced/useful_config.html#Auto-Persist) dataframes used multiple times. This often speeds up Spark jobs of users.

### Access to framework configuration

Even if Fugue tries to simplify the experience of using distributed computing frameworks, it does not restrict users from editing configuration when needed. For example, the Spark session can be configured with the following:

```python
from pyspark.sql import SparkSession
from fugue_spark import SparkExecutionEngine

spark_session = (SparkSession
                 .builder
                 .config("spark.executor.cores",4)
                 .config("fugue.dummy","dummy")
                 .getOrCreate())

engine = SparkExecutionEngine(spark_session, {"additional_conf":"abc"})
```

### [Fugue SQL](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/fugue_sql/index.md.html)

A SQL-based language capable of expressing end-to-end workflows. The `fillna` function above is used in the SQL query below. This is how to use a Python-defined transformer along with the standard SQL `SELECT` statement.

```python
fsql("""
    SELECT id, date, value FROM df
    TRANSFORM USING fillna (value=10)
    PRINT
    """).run()
```

For Fugue SQL, we can change the engine by passing it to the `run` method: `fsql(query).run("spark")`.

## Installation

```bash
pip install fugue
```

Fugue has these extras:

-   **sql**: to support [Fugue SQL](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/fugue_sql/index.md.html)
-   **spark**: to support Spark as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/advanced/execution_engine.html)
-   **dask**: to support Dask as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/advanced/execution_engine.html)

For example a common use case is:

```bash
pip install fugue[sql,spark]
```

## Jupyter Notebook Extension

There is an accompanying notebook extension for Fugue SQL that lets users use the `%%fsql` cell magic. The extension also provides syntax highlighting for Fugue SQL cells. (Syntax highlighting is not available yet for JupyterLab).

![Fugue SQL gif](https://miro.medium.com/max/700/1*6091-RcrOPyifJTLjo0anA.gif)

### Installating Notebook Extension

To install the notebook extension:

```bash
pip install fugue
jupyter nbextension install --py fugue_notebook
jupyter nbextension enable fugue_notebook --py
```

### Loading in a notebook

The notebook environment can be setup by using the `setup` function as follows in the first cell of a notebook:

```python
from fugue_notebook import setup
setup()
```

Note that you can automatically load `fugue_notebook` iPython extension at startup,
read [this](https://ipython.readthedocs.io/en/stable/config/extensions/#using-extensions) to configure your Jupyter environment.

### Usage

To use Fugue SQL in a notebook, simply invoke the `%%fsql` cell magic.

```bash
%%fsql
CREATE [[0]] SCHEMA a:int
PRINT
```

To use Spark or Dask as an execution engine, specify it after `%%fsql`

```bash
%%fsql dask
CREATE [[0]] SCHEMA a:int
PRINT
```

## Get started

The best way to start is to go through the [tutorials](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/index.html). We have the tutorials in an interactive notebook environent.

For the API docs, [click here](https://fugue.readthedocs.org)

### Run the tutorial using binder:

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

**But it runs slow on binder**, the machine on binder isn't powerful enough for
a distributed framework such as Spark. Parallel executions can become sequential, so some of the
performance comparison examples will not give you the correct numbers.

### Run the tutorial using Docker

Alternatively, you should get decent performance if running its Docker image on your own machine:

```bash
docker run -p 8888:8888 fugueproject/tutorials:latest
```

## Contributing

Feel free to message us on [Slack](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ). We also have [contributing instructions](CONTRIBUTING.md).
