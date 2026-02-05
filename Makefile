.PHONY: help clean dev docs package test

# .EXPORT_ALL_VARIABLES:
# ARROW_PRE_0_15_IPC_FORMAT = 0

help:
	@echo "The following make targets are available:"
	@echo ""
	@echo "Setup:"
	@echo "  devenv           Set up dev environment with uv (sync all dependencies)"
	@echo "  init_codespace   Initialize GitHub Codespace environment"
	@echo "  clean            Remove __pycache__ directories"
	@echo ""
	@echo "Development:"
	@echo "  lint             Run pre-commit hooks on all files"
	@echo "  docs             Generate API documentation with Sphinx"
	@echo "  lab              Start Jupyter Lab server"
	@echo ""
	@echo "Testing:"
	@echo "  test             Run all tests with coverage"
	@echo "  testcore         Run core fugue tests only"
	@echo "  testnospark      Run all tests except Spark tests"
	@echo "  testspark        Run Spark tests"
	@echo "  testsparkconnect Run Spark Connect tests"
	@echo "  testdask         Run Dask tests"
	@echo "  testray          Run Ray tests"
	@echo "  testduck         Run DuckDB tests"
	@echo "  testibis         Run Ibis tests"
	@echo "  testpolars       Run Polars tests"
	@echo "  testnosql        Run all tests except SQL-related tests"
	@echo "  testnotebook     Test Jupyter notebook integration"
	@echo ""
	@echo "Spark Connect:"
	@echo "  dockerspark      Start Spark Connect server in Docker"
	@echo "  sparkconnect     Set up Spark Connect locally"

clean:
	find . -name "__pycache__" |xargs rm -rf

devenv:
	uv sync --quiet --dev --all-extras $(if $(upgrade),--upgrade,--frozen)
	uv pip freeze
	uv run --no-sync pre-commit install

init_codespace:
	curl -fsSL https://claude.ai/install.sh | bash
	git pull || true
	uv sync --quiet --dev --all-extras --frozen

docs:
	rm -rf docs/api
	rm -rf docs/api_sql
	rm -rf docs/api_spark
	rm -rf docs/api_dask
	rm -rf docs/api_duckdb
	rm -rf docs/api_ibis
	rm -rf docs/build
	uv run sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api fugue/
	uv run sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_sql fugue_sql/
	uv run sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_spark fugue_spark/
	uv run sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_dask fugue_dask/
	uv run sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_ray fugue_ray/
	uv run sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_duckdb fugue_duckdb/
	uv run sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_ibis fugue_ibis/
	uv run sphinx-build -b html docs/ docs/build/

lint:
	uv run pre-commit run --all-files

lab:
	mkdir -p tmp
	uv run fugue-jupyter install startup
	uv run jupyter lab --port=8888 --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'

test:
	uv run --active pytest --reruns 2 --only-rerun 'Overflow in cast' --only-rerun 'Table or view not found' tests/

testnospark:
	uv run --active pytest --ignore=tests/fugue_spark tests/

testcore:
	uv run --active pytest tests/fugue

testspark:
	uv run --active pytest --reruns 2 --only-rerun 'Table or view not found' tests/fugue_spark

testsparkconnect:
	uv run --active pytest --reruns 2 --only-rerun 'Table or view not found' -k SparkConnect tests/fugue_spark/test_spark_connect.py

testdask:
	uv run --active pytest tests/fugue_dask

# https://github.com/ray-project/ray/issues/53848
testray:
	uv run --active pytest tests/fugue_ray

testnosql:
	uv run --active pytest --reruns 2 --only-rerun 'Table or view not found' tests/fugue tests/fugue_spark tests/fugue_dask tests/fugue_ray

testduck:
	uv run --active pytest --reruns 2 --only-rerun 'Overflow in cast' tests/fugue_duckdb

testibis:
	uv run --active pytest tests/fugue_ibis

testpolars:
	uv run --active pytest tests/fugue_polars

testnotebook:
	uv run jupyter contrib nbextension install --user
	uv run jupyter nbextension install --user --py fugue_notebook
	uv run jupyter nbextension enable fugue_notebook --py
	uv run jupyter nbconvert --execute --clear-output tests/fugue_notebook/test_notebook.ipynb

dockerspark:
	docker run -p 15002:15002 -p 4040:4040 -e SPARK_NO_DAEMONIZE=1 apache/spark-py /opt/spark/sbin/start-connect-server.sh --jars https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.4.0/spark-connect_2.12-3.4.0.jar

sparkconnect:
	bash scripts/setupsparkconnect.sh
