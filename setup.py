import os

from setuptools import find_packages, setup

from fugue_version import __version__

with open("README.md") as f:
    _text = ["# Fugue"] + f.read().splitlines()[1:]
    LONG_DESCRIPTION = "\n".join(_text)


def get_version() -> str:
    tag = os.environ.get("RELEASE_TAG", "")
    if "dev" in tag.split(".")[-1]:
        return tag
    if tag != "":
        assert tag == __version__, "release tag and version mismatch"
    return __version__


setup(
    name="fugue",
    version=get_version(),
    packages=find_packages(include=["fugue*"]),
    description="An abstraction layer for distributed computation",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    author="The Fugue Development Team",
    author_email="hello@fugue.ai",
    keywords="distributed spark dask sql dsl domain specific language",
    url="http://github.com/fugue-project/fugue",
    install_requires=[
        "triad>=0.9.1",
        "adagio>=0.2.4",
        "pyarrow>=0.15.1",
        "pandas>=1.2.0",
        # sql dependencies
        "qpd>=0.4.4",
        "fugue-sql-antlr>=0.1.6",
        "sqlglot",
        "jinja2",
    ],
    extras_require={
        "sql": [
            "qpd>=0.4.4",
            "fugue-sql-antlr>=0.1.6",
            "sqlglot",
            "jinja2",
        ],
        "cpp_sql_parser": ["fugue-sql-antlr[cpp]>=0.1.6"],
        "spark": ["pyspark>=3.1.1"],
        "dask": [
            "dask[distributed,dataframe]; python_version < '3.8'",
            "dask[distributed,dataframe]>=2022.9.0,<2023.7.1; python_version >= '3.8'",
            "qpd[dask]>=0.4.4",
        ],
        "ray": ["ray[data]>=2.1.0", "duckdb>=0.5.0", "pyarrow>=6.0.1"],
        "duckdb": [
            "duckdb>=0.5.0",
            "pyarrow>=6.0.1",
            "numpy",
        ],
        "polars": ["polars"],
        "ibis": [
            "ibis-framework>=2.1.1; python_version < '3.8'",
            "ibis-framework>=3.2.0,<6; python_version >= '3.8'",
        ],
        "notebook": ["notebook", "jupyterlab", "ipython>=7.10.0"],
        "all": [
            "sqlglot",
            "jinja2",
            "fugue-sql-antlr[cpp]>=0.1.6",
            "pyspark>=3.1.1",
            "dask[distributed,dataframe]; python_version < '3.8'",
            "dask[distributed,dataframe]>=2022.9.0,<2023.7.1; python_version >= '3.8'",
            "ray[data]>=2.1.0",
            "qpd[dask]>=0.4.4",
            "notebook",
            "jupyterlab",
            "ipython>=7.10.0",
            "duckdb>=0.5.0",
            "pyarrow>=6.0.1",
            "ibis-framework>=2.1.1; python_version < '3.8'",
            "ibis-framework>=3.2.0,<6; python_version >= '3.8'",
            "polars",
        ],
    },
    classifiers=[
        # "3 - Alpha", "4 - Beta" or "5 - Production/Stable"
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    python_requires=">=3.7",
    package_data={"fugue": ["py.typed"], "fugue_notebook": ["nbextension/*"]},
    entry_points={
        "fugue.plugins": [
            "ibis = fugue_ibis[ibis]",
            "duckdb = fugue_duckdb.registry[duckdb]",
            "duckdb_ibis = fugue_duckdb.ibis_engine[duckdb,ibis]",
            "spark = fugue_spark.registry[spark]",
            "spark_ibis = fugue_spark.ibis_engine[spark,ibis]",
            "dask = fugue_dask.registry[dask]",
            "dask_ibis = fugue_dask.ibis_engine[dask,ibis]",
            "ray = fugue_ray.registry[ray]",
            "polars = fugue_polars.registry[polars]",
        ]
    },
)
