# Fugue

[![GitHub release](https://img.shields.io/github/release/fugue-project/fugue.svg)](https://GitHub.com/fugue-project/fugue)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI license](https://img.shields.io/pypi/l/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI version](https://badge.fury.io/py/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![Coverage Status](https://coveralls.io/repos/github/fugue-project/fugue/badge.svg)](https://coveralls.io/github/fugue-project/fugue)
[![Doc](https://readthedocs.org/projects/fugue/badge)](https://fugue.readthedocs.org)

[Join Fugue-Project on Slack](https://join.slack.com/t/fugue-project/shared_invite/zt-he6tcazr-OCkj2GEv~J9UYoZT3FPM4g)

Fugue is a pure abstraction layer that adapts to different computing frameworks
such as Spark and Dask. It is to unify the core concepts of distributed computing and
to help you decouple your logic from specific computing frameworks.

## Installation
```
pip install fugue
```

Fugue has these extras:
* **sql**: to support [Fugue SQL](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/sql.html)
* **spark**: to support Spark as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/execution_engine.html)
* **dask**: to support Dask as the [ExecutionEngine](https://fugue-tutorials.readthedocs.io/en/latest/tutorials/execution_engine.html)

For example a common use case is:
```
pip install fugue[sql,spark]
```


## Docs and Tutorials

To read the complete static docs, [click here](https://fugue.readthedocs.org)

The best way to start is to go through the tutorials. We have the tutorials in an interactive notebook environent.

### Run the tutorial using binder:
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

**But it runs slow on binder**, the machine on binder isn't powerful enough for
a distributed framework such as Spark. Parallel executions can become sequential, so some of the
performance comparison examples will not give you the correct numbers.

### Run the tutorial using docker

Alternatively, you should get decent performance if running its docker image on your own machine:

```
docker run -p 8888:8888 fugueproject/tutorials:latest
```

## Contributing Code

There are three steps to setting-up a development environment
1. Create a virtual environment with your choice of environment manager
2. Install the requirements
3. Install the git hook scripts

### Creating an environment

Below are examples for how to create and activate an environment in virtualenv and conda.

**Using virtualenv**
```
python3 -m venv venv
. venv/bin/activate

```

**Using conda**
```
conda create --name fugue-dev
conda activate fugue-dev
```

### Installing requirements

The Fugue repo has a Makefile that can be used to install the requirements. It supports installation in both
pip and conda.

**Pip install requirements**
```
make setupinpip
```

**Conda install requirements**

```
make setupinconda
```

**Manually install requirements**

For Windows users who don't have the `make` command, you can use your package manager of choice. For pip:

```
pip3 install -r requirements.txt
```

For Anaconda users, first install pip in the newly created environment. If pip install is used without installing pip, conda will use
the system-wide pip

```
conda install pip
pip install -r requirements.txt
```

**Notes for Windows Users**

For Windows users, you will need to download Microsoft C++ Build Tools found [here](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

### Installing git hook scripts

Fugue has pre-commit hooks to check if code is appropriate to be commited. The previous `make` command installs this.
If you installed the requirements manually, install the git hook scripts with:
```
pre-commit install
```


## Update History

### 0.4.1
* Added set operations to programming interface: `union`, `subtract`, `intersect`
* Added `distinct` to programming interface
* Ensured partitioning follows SQL convention: groups with null keys are NOT removed
* Switched `join`, `union`, `subtract`, `intersect`, `distinct` to QPD implementations, so they follow SQL convention
* Set operations in Fugue SQL can directly operate on Fugue statemens (e.g. `TRANSFORM USING t1 UNION TRANSFORM USING t2`)
* Fixed bugs
* Added onboarding document for contributors

### <=0.4.0

* Main features of Fugue core and Fugue SQL
* Support backends: Pandas, Spark and Dask
