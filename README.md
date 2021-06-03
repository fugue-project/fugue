# <img src="./images/logo.svg" width="200">

[![Doc](https://readthedocs.org/projects/fugue/badge)](https://fugue.readthedocs.org)
[![PyPI version](https://badge.fury.io/py/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI license](https://img.shields.io/pypi/l/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![codecov](https://codecov.io/gh/fugue-project/fugue/branch/master/graph/badge.svg?token=ZO9YD5N3IA)](https://codecov.io/gh/fugue-project/fugue)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/4fa5f2f53e6f48aaa1218a89f4808b91)](https://www.codacy.com/gh/fugue-project/fugue/dashboard?utm_source=github.com&utm_medium=referral&utm_content=fugue-project/fugue&utm_campaign=Badge_Grade)

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

Fugue is a pure abstraction layer that makes Python and SQL code portable across differing computing frameworks such as Pandas, Spark and Dask.

-   **Framework-agnostic code**: Write code once in native Python or SQL. Fugue makes it runnable on Pandas, Dask or Spark with minimal changes. Logic and code is decoupled from frameworks, even from Fugue itself. Fugue makes the user's code adapt to the underlying computing frameworks. Users can use the Spark and Dask engines without learning the specific framework syntax.
-   **Rapid iterations for big data projects**: Test code on smaller data, then reliably scale to Dask or Spark when ready. This drastically improves project iteration time and reduce cluster usage.  This lessens the frequency spinning up clusters to test code, and reduces expensive mistakes. It also becomes trivial to transition from Pandas-sized data to bigger datasets.
-   **Friendlier interface for Spark**: Fugue provides a friendlier interface compared to Spark user-defined functions (UDF). Users can get Python/Pandas code running on Spark with less effort. Fugue SQL extends Spark SQL to be a more complete programming language. Lastly, Fugue as some optimizations that make the Spark engine easier to use.
-   **Highly testable code**: Fugue naturally makes logic more testable because the code will be written in native Python. Unit tests scale seamlessly from local workflows to distributed computing workflows.

## Getting Started

View our latest presentations and content

-   [Tutorials](https://fugue-tutorials.readthedocs.io/en/latest/)
-   [Interoperable Python and SQL blog](https://towardsdatascience.com/interoperable-python-and-sql-in-jupyter-notebooks-86245e711352)
-   [Data Science Cross-Framework Library Blog](https://towardsdatascience.com/creating-pandas-and-spark-compatible-functions-with-fugue-8617c0b3d3a8)

## Key Features

Here is an example Fugue code snippet that illustrates some of the key features of the framework. A `fillna` function creates a new column named `filled`, which is the same as the column `value` except that the `None` values are filled. Notice that the `fillna` function written below is purely in native Python. The code will still run without Fugue installed.

```python
from fugue import FugueWorkflow
from typing import Iterable, Dict, Any, List

# Creating sample data
data = [
    ["A", "2020-01-01", 10],
    ["A", "2020-01-02", None],
    ["A", "2020-01-03", 30],
    ["B", "2020-01-01", 20],
    ["B", "2020-01-02", None],
    ["B", "2020-01-03", 40]
]
schema = "id:str,date:date,value:double"

# schema: *, filled:double
def fillna(df:Iterable[Dict[str,Any]], value:float=0) -> Iterable[Dict[str,Any]]:
    for row in df:
        row["filled"] = (row["value"] or value)
        yield row

with FugueWorkflow() as dag:
    df = dag.df(data, schema).transform(fillna)
    df.show()
```

### Cross-platform execution

Fugue lets users write scale-agnostic code in Python or SQL, and then port the logic to Pandas, Spark, or Dask. Users can focus on the logic, rather than on what engine it will be executed. To bring it to Spark, simply pass the `SparkExecutionEngine` into the `FugueWorkflow` as follows.

```python
from fugue_spark import SparkExecutionEngine

with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df(data, schema).transform(fillna)
    df.show()
```

Similarly for Dask, we can pass the `DaskExecutionEngine` into the `FugueWorkflow` instead. The example above is to illustrate that native Python can be used on top of Spark. In practice, using the Pandas `fillna` will be easier to use in this case. We can run the Pandas `fillna` on Spark or Dask like follows:

```python
# schema: *, filled:double
def fillna_pandas(df:pd.DataFrame, value:float=0) -> pd.DataFrame:
    df["filled"] = df["value"].fillna(value)
    return df

with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df(data, schema).transform(fillna_pandas)
    df.show()
```

### Catch errors faster

Fugue builds a [directed acyclic graph (DAG)](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/dag.html) before running code, allowing users to receive errors faster. This catches more errors before expensive jobs are run on a cluster. For example, mismatches in specified [schema](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/schema_dataframes.html#Schema) will raise errors. In the code above, the schema hint comment is read and the schema is enforced during execution. Schema is required for Fugue [extensions](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/extensions.html).

### Spark optimizations

Fugue makes Spark easier to use for people starting with distributed computing. For example, Fugue uses the constructed DAG to smartly [auto-persist](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/useful_config.html#Auto-Persist) dataframes used multiple times. This often speeds up Spark jobs of users.

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

### [Fugue SQL](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/fugue_sql/index.html)

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

-   **sql**: to support [Fugue SQL](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/fugue_sql/index.html)
-   **spark**: to support Spark as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/execution_engine.html)
-   **dask**: to support Dask as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/execution_engine.html)

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

## Partner with Us

The Fugue Project is looking for select companies to partner closely with and implement solutions built on any of the Fugue libraries (Fugue core, Fugue SQL, Tune). As part of the partnership, our team will closely work with you, or give trainings and workshops to your team members. If you're interested, please fill out [this form](https://docs.google.com/forms/d/e/1FAIpQLScsBnQg78ScNn4tXMLSEiNnNOBR2ZVxRyOgGUOohzPk8KFGoA/viewform?usp=sf_link) and we'll reach out to you.
