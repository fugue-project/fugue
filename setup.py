from setuptools import setup, find_packages
from fugue import __version__


with open("README.md") as f:
    LONG_DESCRIPTION = f.read()

setup(
    name="fugue",
    version=__version__,
    packages=find_packages(),
    description="An abstraction layer for distributed computation",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license="MIT",
    author="Han Wang",
    author_email="goodwanghan@gmail.com",
    keywords="distributed spark dask sql dsl domain specific language",
    url="http://github.com/fugue-project/fugue",
    install_requires=["triad>=0.3.7", "adagio>=0.1.8", "sqlalchemy"],
    extras_require={
        "sql": ["antlr4-python3-runtime", "jinjasql"],
        "spark": ["pyspark"],
        "dask": ["dask[dataframe]", "cloudpickle>=1.4.0"],
        "all": [
            "antlr4-python3-runtime",
            "jinjasql",
            "pyspark",
            "dask[dataframe]",
            "cloudpickle>=1.4.0",
        ],
    },
    classifiers=[
        # "3 - Alpha", "4 - Beta" or "5 - Production/Stable"
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
    ],
    python_requires=">=3.6",
)
