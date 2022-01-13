from setuptools import setup, find_packages
from fugue_version import __version__
import os

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
    packages=find_packages(),
    description="An abstraction layer for distributed computation",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    author="The Fugue Development Team",
    author_email="hello@fugue.ai",
    keywords="distributed spark dask sql dsl domain specific language",
    url="http://github.com/fugue-project/fugue",
    install_requires=[
        "triad>=0.6.0",
        "adagio>=0.2.3",
        "qpd>=0.2.4",
        "sqlalchemy",
        "pyarrow>=0.15.1",
        "pandas>=1.0.2",
    ],
    extras_require={
        "sql": ["antlr4-python3-runtime", "jinja2"],
        "spark": ["pyspark"],
        "dask": ["qpd[dask]"],
        "duckdb": ["duckdb>=0.3.1", "pyarrow>=5.0.0,<6"],
        "ibis": ["ibis-framework>=2.1.1"],
        "notebook": ["notebook", "jupyterlab", "dash"],
        "all": [
            "antlr4-python3-runtime",
            "jinja2",
            "pyspark",
            "qpd[dask]",
            "notebook",
            "jupyterlab",
            "dash",
            "duckdb",
            "ibis-framework>=2; python_version >= '3.7'",
        ],
    },
    classifiers=[
        # "3 - Alpha", "4 - Beta" or "5 - Production/Stable"
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
    ],
    python_requires=">=3.6",
    package_data={"fugue_notebook": ["nbextension/*"]},
)
