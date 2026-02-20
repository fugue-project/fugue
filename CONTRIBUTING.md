# Contributing

## Ways to Contribute

We're happy you're looking to contribute. We recommend you join the [Slack channel](slack.fugue.ai) to discuss how to get involved, or how to use Fugue. There are many ways to help this project.

1.  **Use Fugue in your project** - Having more users helps us come across more use cases and make a better framework. We're always happy to help you get started using Fugue for your company use case or personal project. Feel free to message us on [Slack](slack.fugue.ai).

2.  **Give us feedback/Post issues** - If you have ideas of how to make Fugue better, or have general questions about Fugue, we'd be happy to hear them. Hearing unclear parts helps us write better documentation. Posting issues helps us fix bugs or make new features.

3.  **Make a blog post or presentation** - Are you interested in presenting Fugue to your company? at a Meetup? or at a conference? We'd be happy to chat with you and share some resources we have.

4.  **Write code** - Is there an [issue](https://github.com/fugue-project/fugue/issues) you want to take a stab at? We recommend touching base with us before you pick up an issue. Documentation is also a good way to help.

## Project Structure

There are 2 main parts to the codebase

-   fugue - This contains the core of Fugue, including the fundamental classes such as DataFrames, ExecutionEngine, and Extensions.
-   fugue_sql - Fugue SQL is a Domain Specific Language (DSL) for Fugue

Then there are backend-specific portions

-   fugue_spark - Spark-specific code
-   fugue_dask - Dask-specific code
-   fugue_ray - Ray-specific code
-   fugue_duckdb - DuckDB-specific code
-   fugue_ibis - Ibis-specific code

There are 2 main parts to tests

-   fugue_test - Contains suites for testing (dataframe, execution engine). These unify the concepts of distributed computing and ensure consistent behavior across different execution engines (Pandas, Spark, Dask).
-   tests - Contains all tests for the repository

Lastly, there is documentation. Note that tutorials live in another [repository](https://github.com/fugue-project/tutorials).

## Setting up the dev environment

Fugue uses [uv](https://docs.astral.sh/uv/) to manage dependencies and virtual environments. If you don't have `uv` installed, follow the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/).

### Setting up with make

The simplest way to set up the full dev environment (dependencies + pre-commit hooks) is:

```bash
make devenv
```

This runs `uv sync` to install all dependencies into a managed virtual environment and installs the pre-commit hooks automatically.

To upgrade all dependencies to their latest allowed versions, pass the `upgrade` flag:

```bash
make devenv upgrade=1
```

### Setting up manually

If you prefer not to use `make`, you can run the underlying commands directly:

```bash
uv sync --dev --all-extras
uv run pre-commit install
```

### Installing pre-commit hooks

Pre-commit hooks are installed automatically by `make devenv`. If you need to install them separately:

```bash
uv run pre-commit install
```

## Running Tests

The Makefile has the following targets for testing

```bash
make test      - All tests
make testcore  - All Fugue code not specific to any backend
make testspark - Only the Spark specific tests
make testdask  - Only the Dask specific tests
make testray   - Only the Ray specific tests
make testduck  - Only the DuckDB specific tests
make testibis  - Only the Ibis specific tests
```
