import os

from setuptools import find_packages, setup

from fugue_version import __version__

SQL_DEPENDENCIES = [
    "qpd>=0.4.4",
    "fugue-sql-antlr>=0.2.0",
    "sqlglot<28",  # TODO: 28 breaks ibis, fix it
    "jinja2",
]

with open("README.md", encoding="utf-8") as f:
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
    keywords="distributed spark dask ray sql dsl domain specific language",
    url="http://github.com/fugue-project/fugue",
    install_requires=[
        "triad>=1.0.0",
        "adagio>=0.2.6",
        "pandas<3",  # TODO: remove upper bound on 0.9.7
    ],
    extras_require={
        "sql": SQL_DEPENDENCIES,
        "cpp_sql_parser": ["fugue-sql-antlr[cpp]>=0.2.0"],
        "spark": ["pyspark>=3.1.1", "zstandard>=0.25.0"],
        "dask": [
            "dask[distributed,dataframe]>=2024.4.0",
            "pyarrow>=7.0.0",
            "pandas>=2.0.2",
        ],
        "ray": [
            "ray[data]>=2.30.0",
            "duckdb>=0.5.0",
            "pyarrow>=7.0.0",
            "pandas",
        ],
        "duckdb": SQL_DEPENDENCIES
        + [
            "duckdb>=0.5.0",
            "numpy",
        ],
        "polars": ["polars"],
        "ibis": SQL_DEPENDENCIES + ["ibis-framework[pandas]"],
        "notebook": ["notebook", "jupyterlab", "ipython>=7.10.0"],
        "all": SQL_DEPENDENCIES
        + [
            "pyspark>=3.1.1",
            "dask[distributed,dataframe]>=2024.4.0",
            "dask-sql",
            "ray[data]>=2.30.0",
            "notebook",
            "jupyterlab",
            "ipython>=7.10.0",
            "duckdb>=0.5.0",
            "pyarrow>=6.0.1",
            "pandas>=2.0.2",
            "ibis-framework[pandas,duckdb]",
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3 :: Only",
    ],
    python_requires=">=3.8",
    package_data={"fugue": ["py.typed"], "fugue_notebook": ["nbextension/*"]},
    entry_points={
        "fugue.plugins": [
            "ibis = fugue_ibis[ibis]",
            "duckdb = fugue_duckdb.registry[duckdb]",
            "spark = fugue_spark.registry[spark]",
            "dask = fugue_dask.registry[dask]",
            "ray = fugue_ray.registry[ray]",
            "polars = fugue_polars.registry[polars]",
        ],
        "pytest11": [
            "fugue_test = fugue_test",
            "fugue_test_fixtures = fugue_test.fixtures",
        ],
    },
)
