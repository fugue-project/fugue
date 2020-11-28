# Replicate

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

## Install

```
pip install -U replicate
```

## Get started

If you prefer **training scripts and the CLI**, [follow the our tutorial to learn how Replicate works](https://replicate.ai/docs/tutorial).

If you prefer **working in notebooks**, <a href="https://colab.research.google.com/drive/1vjZReg--45P-NZ4j8TXAJFWuepamXc7K" target="_blank">follow our notebook tutorial on Colab</a>.

If you like to **learn concepts first**, [read our guide about how Replicate works](https://replicate.ai/docs/learn/how-it-works).

## Get involved

Everyone uses version control for software, but it is much less common in machine learning.

Why is this? We spent a year talking to people in the ML community and this is what we found out:

- **Git doesn’t work well with machine learning.** It can’t handle large files, it can’t handle key/value metadata like metrics, and it can’t commit automatically in your training script. There are some solutions for this, but they feel like band-aids.
- **It should be open source.** There are a number of proprietary solutions, but something so foundational needs to be built by and for the ML community.
- **It needs to be small, easy to use, and extensible.** We found people struggling to integrate with “AI Platforms”. We want to make a tool that does one thing well and can be combined with other tools to produce the system you need.

We think the ML community needs a good version control system. But, version control systems are complex, and to make this a reality we need your help.

Have you strung together some shell scripts to build this for yourself? Are you interested in the problem of making machine learning reproducible?

Here are some ways you can help out:

- [Join our Slack to chat to us and other contributors.](https://discord.gg/QmzJApGjyE)
- [Have your say about what you want from a version control system on our public roadmap.](https://github.com/replicate/replicate/projects/1)
- [Try your hand at one of our issues labelled "help wanted".](https://github.com/replicate/replicate/labels/help%20wanted)

## Contributing & development environment

[Take a look at our contributing instructions.](CONTRIBUTING.md)
