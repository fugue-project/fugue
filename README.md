# <img src="./images/logo.svg" width="200">

[![PyPI version](https://badge.fury.io/py/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI license](https://img.shields.io/pypi/l/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![Doc](https://readthedocs.org/projects/fugue/badge)](https://fugue.readthedocs.org)
[![Coverage Status](https://coveralls.io/repos/github/fugue-project/fugue/badge.svg)](https://coveralls.io/github/fugue-project/fugue)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/4fa5f2f53e6f48aaa1218a89f4808b91)](https://www.codacy.com/gh/fugue-project/fugue/dashboard?utm_source=github.com&utm_medium=referral&utm_content=fugue-project/fugue&utm_campaign=Badge_Grade)

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

Fugue is a pure abstraction layer that makes code portable across differing computing frameworks such as Pandas, Spark and Dask.

-   **Framework-agnostic code**: Write code once in native Python. Fugue makes it runnable on Pandas, Dask or Spark with minimal changes. Logic and code is decoupled from frameworks, even from Fugue itself. Fugue adapts user's code, as well as the underlying computing frameworks.
-   **Rapid iterations for big data projects**: Test code on smaller data, then reliably scale to Dask or Spark when ready. This drastically improves project iteration time and saves cluster expense.  This lessens the frequency spinning up clusters to test code, and reduces expensive mistakes.
-   **Friendlier interface for Spark**: Fugue handles some optimizations on Spark, making it easier for big data practitioners to focus on logic. A lot of Fugue users see performance gains in their Spark jobs. Fugue SQL extends Spark SQL to be a programming language.
-   **Highly testable code**: Fugue naturally makes logic more testable because the code is in native Python. Unit tests scale seamlessly from local workflows to distributed computing workflows.

## Who is it for?

-   Big data practitioners looking to reduce compute costs and increase project velocity
-   Data practitioners who keep switching between data processing frameworks (Pandas, Spark, Dask)
-   Data engineers scaling data pipelines to handle bigger data in a consistent and reliable way
-   Data practitioners looking to write more testable code
-   Spark/Dask users who want to have an easier experience working with distributed computing
-   People who love using SQL. Fugue SQL extends standard SQL to be a programming language

## Key Features

Here is an example Fugue code snippet that illustrates some of the key features of the framework. A fillna function creates a new column named `filled`, which is the same as the column `value` except that the `None` values are filled.

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
    df1 = dag.df(data, schema).transform(fillna)
    df1.show()
```

### Catch errors faster

Fugue builds a [directed acyclic graph (DAG)](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/dag.html) before running code, allowing users to receive errors faster. This catches more errors before expensive jobs are run on a cluster. For example, mismatches in specified [schema](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/schema_dataframes.html#Schema) will raise errors. In the code above, the schema hint comment is read and the schema is enforced during execution. Schema is required for Fugue [extensions](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/extensions.html).

### Cross-platform execution

Notice that the `fillna` function written above is purely in native Python. The code will still run without Fugue. Fugue lets users write code in Python, and then port the logic to Pandas, Spark, or Dask. Users can focus on the logic, rather than on what engine it will be executed. To bring it to Spark, simply pass the `SparkExecutionEngine` into the `FugueWorkflow` as follows.

```python
from fugue_spark import SparkExecutionEngine

with FugueWorkflow(SparkExecutionEngine) as dag:
    df1 = dag.df(data, schema).transform(fillna)
    df1.show()
```

Similarly for Dask, we can pass the `DaskExecutionEngine` into the `FugueWorkflow` instead.

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

### [Fugue SQL](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/sql.html)

A SQL-based language capable of expressing end-to-end workflows. The `fillna` code above is equivalent to the code below. This is how to use a Python-defined transformer along with the standard SQL `SELECT` statement.

```python
with FugueSQLWorkflow() as dag:
    df1 = dag.df(data, schema)
    dag("""
    SELECT id, date, value FROM df1
    TRANSFORM USING fillna (value=10)
    PRINT
    """)
```

Alternatively, there is a simpler way:

```python
df1 = ArrayDataFrame(data, schema)
fsql("""
    SELECT id, date, value FROM df1
    TRANSFORM USING fillna (value=10)
    PRINT
""").run()
```

## Get started

To read the complete static docs, [click here](https://fugue.readthedocs.org)

The best way to start is to go through the tutorials. We have the tutorials in an interactive notebook environent.

### Run the tutorial using binder:

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

**But it runs slow on binder**, the machine on binder isn't powerful enough for
a distributed framework such as Spark. Parallel executions can become sequential, so some of the
performance comparison examples will not give you the correct numbers.

### Run the tutorial using docker

Alternatively, you should get decent performance if running its docker image on your own machine:

```bash
docker run -p 8888:8888 fugueproject/tutorials:latest
```

## Installation

```bash
pip install fugue
```

Fugue has these extras:

-   **sql**: to support [Fugue SQL](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/sql.html)
-   **spark**: to support Spark as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/execution_engine.html)
-   **dask**: to support Dask as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/execution_engine.html)

For example a common use case is:

```bash
pip install fugue[sql,spark]
```

## Jupyter Notebook Extension (since 0.5.1)

```bash
pip install fugue
jupyter nbextension install --py fugue_notebook
jupyter nbextension enable fugue_notebook --py
```

After installing the Jupyter extension, you can have `%%fsql` magic cells, where
the Fugue SQL inside the cell will be highlighted.

We are also able to run this fsql magic cell if you load the ipython extension,
here is an example:

In cell 1

```bash
%load_ext fugue_notebook
```

In cell 2

```bash
%%fsql
CREATE [[0]] SCHEMA a:int
PRINT
```

In cell 3 where you want to use dask

```bash
%%fsql dask
CREATE [[0]] SCHEMA a:int
PRINT
```

Note that you can automatically load `fugue_notebook` ipthon extension at startup,
read [this](https://ipython.readthedocs.io/en/stable/config/extensions/#using-extensions) to configure your jupyter environment.

There is an ad-hoc way to setup your notebook environment, you don't need to install anything or change the startup script.
You only need to do the following at the first cell of each of your notebook, and you will get highlights and `%%fsql` cells become runnable too:

```python
from fugue_notebook import setup
setup()
```

## Contributing

Feel free to message us on [Slack](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ). We also have [contributing instructions](CONTRIBUTING.md).
