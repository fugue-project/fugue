# Contributing

## Ways to Contribute

We're happy you're looking to contribute. We recommend you join the [Slack channel](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ) to discuss how to get involved, or how to use Fugue. There are many ways to help this project.

1.  **Use Fugue in your project** - Having more users helps us come across more use cases and make a better framework. We're always happy to help you get started using Fugue for your company use case or personal project. Feel free to message us on [Slack](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ).

2.  **Give us feedback/Post issues** - If you have ideas of how to make Fugue better, or have general questions about Fugue, we'd be happy to hear them. Hearing unclear parts helps us write better documentation. Posting issues helps us fix bugs or make new features.

3.  **Make a blog post or presentation** - Are you interested in presenting Fugue to your company? at a Meetup? or at a conference? We'd be happy to touch base with you and share some resources we have.

4.  **Write code** - Is there an [issue](https://github.com/fugue-project/fugue/issues) you want to take a stab at? We recommend touching base with us before you pick up an issue. Documentation is also a good way to help.

## Project Structure

There are 4 main parts to the codebase

-   fugue - This contains the core of Fugue, including the fundamental classes such as DataFrames, ExecutionEngine, and Extensions.

-   fugue_sql - Fugue SQL is a Domain Specific Language (DSL) for Fugue

-   fugue_spark - Spark specific components (DataFrame and ExecutionEngine)

-   fugue_dask - Dask specific components (DataFrame and ExecutionEngine)

There are 2 main parts to tests

-   fugue_test - Contains suites for testing (dataframe, execution engine). These unify the concepts of distributed computing and ensure consistent behavior across different execution engines (Pandas, Spark, Dask).

-   tests - Contains all tests for the repository

Lastly, there is documentation. Note that tutorials live in another [repository](https://github.com/fugue-project/tutorials).

## Setting up the dev environment

There are three steps to setting-up a development environment

1.  Create a virtual environment with your choice of environment manager

2.  Install the requirements

3.  Install the git hook scripts

### Creating an environment

Below are examples for how to create and activate an environment in virtualenv and conda.

**Using virtualenv**

    python3 -m venv venv
    . venv/bin/activate

**Using conda**

    conda create --name fugue-dev
    conda activate fugue-dev

### Installing requirements

The Fugue repo has a Makefile that can be used to install the requirements. It supports installation in both pip and conda. Instructions to install `make` for Windows users can be found later.

**Pip install requirements**

    make setupinpip

**Conda install requirements**

    make setupinconda

**Manually install requirements**

For Windows users who don't have the `make` command, you can use your package manager of choice. For pip:

    pip3 install -r requirements.txt

For Anaconda users, first install `pip` in the newly created environment. If pip install is used without installing pip, conda will use the system-wide pip

    conda install pip
    pip install -r requirements.txt

**Notes for Windows Users**
For Windows users, you will need to download Microsoft C++ Build Tools found [here](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

`make` is a GNU command that does not come with Windows. An installer can be downloaded [here](http://gnuwin32.sourceforge.net/packages/make.htm)

After installing, add the bin to your PATH environment variable.

### Installing git hook scripts

Fugue has pre-commit hooks to check if code is appropriate to be committed. The previous `make` command installs this.

If you installed the requirements manually, install the git hook scripts with:

    pre-commit install

## Running Tests

The Makefile has the following targets for testing

    make test      - All tests
    make testcore  - All Fugue code not specific to Spark or Dask
    make testspark - Only the Spark specific tests
    make testdask  - Only the Dask specific tests
    make testsql   - For Fugue SQL tests only
