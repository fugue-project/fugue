.PHONY: help clean dev docs package test

# .EXPORT_ALL_VARIABLES:
# ARROW_PRE_0_15_IPC_FORMAT = 0

help:
	@echo "The following make targets are available:"
	@echo "  setupinpip	use pip to install requirements in current env"
	@echo "	 setupinconda	use conda to install requirements in current env"
	@echo "	 devenv		create venv and install all deps for dev env (assumes python3 cmd exists)"
	@echo "	 dev 		install all deps for dev env (assumes venv is present)"
	@echo "  docs		create pydocs for all relveant modules (assumes venv is present)"
	@echo "	 package	package for pypi"
	@echo "	 test		run all tests with coverage (assumes venv is present)"
	@echo "	 testcore	run all tests excluding spark tests with coverage (assumes venv is present)"
	@echo "	 testspark	run all tests of spark (assumes venv is present)"
	@echo "	 sql		fugue sql code gen"

clean:
	find . -name "__pycache__" |xargs rm -rf

setupinpip:
	pip3 install -r requirements.txt
	pre-commit install

setupinconda:
	conda install pip
	pip install -r requirements.txt
	pre-commit install

devenv:
	pip3 install -r requirements.txt
	pre-commit install
	pre-commit install-hooks
	pip freeze

devenvlegacy:
	pip3 install -r requirements.txt
	pre-commit install

dev:
	pip3 install -r requirements.txt

docs:
	rm -rf docs/api
	rm -rf docs/api_sql
	rm -rf docs/api_spark
	rm -rf docs/api_dask
	rm -rf docs/api_duckdb
	rm -rf docs/api_ibis
	rm -rf docs/build
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api fugue/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_sql fugue_sql/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_spark fugue_spark/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_dask fugue_dask/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_ray fugue_ray/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_duckdb fugue_duckdb/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_ibis fugue_ibis/
	sphinx-build -b html docs/ docs/build/

lint:
	pre-commit run --all-files

package:
	rm -rf dist/*
	python3 setup.py sdist
	python3 setup.py bdist_wheel

jupyter:
	mkdir -p tmp
	pip install .
	jupyter nbextension install --py fugue_notebook
	jupyter nbextension enable fugue_notebook --py
	jupyter notebook --port=8888 --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'

lab:
	mkdir -p tmp
	pip install .
	pip install fugue-jupyter
	fugue-jupyter install startup
	jupyter lab --port=8888 --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'

test:
	python3 -b -m pytest --reruns 2 --only-rerun 'Overflow in cast' --only-rerun 'Table or view not found' tests/

testnospark:
	python3 -b -m pytest --ignore=tests/fugue_spark tests/

testcore:
	python3 -b -m pytest tests/fugue

testspark:
	python3 -b -m pytest --reruns 2 --only-rerun 'Table or view not found' tests/fugue_spark

testsparkconnect:
	python3 -b -m pytest --reruns 2 --only-rerun 'Table or view not found' -k SparkConnect tests/fugue_spark/test_spark_connect.py

testdask:
	python3 -b -m pytest tests/fugue_dask

testray:
	python3 -b -m pytest tests/fugue_ray

testnosql:
	python3 -b -m pytest --reruns 2 --only-rerun 'Table or view not found' tests/fugue tests/fugue_spark tests/fugue_dask tests/fugue_ray

testduck:
	python3 -b -m pytest --reruns 2 --only-rerun 'Overflow in cast' tests/fugue_duckdb

testibis:
	python3 -b -m pytest tests/fugue_ibis

testpolars:
	python3 -b -m pytest tests/fugue_polars

testnotebook:
	pip install .
	jupyter nbextension install --user --py fugue_notebook
	jupyter nbextension enable fugue_notebook --py
	jupyter nbconvert --execute --clear-output tests/fugue_notebook/test_notebook.ipynb

dockerspark:
	docker run -p 15002:15002 -p 4040:4040 -e SPARK_NO_DAEMONIZE=1 apache/spark-py /opt/spark/sbin/start-connect-server.sh --jars https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.4.0/spark-connect_2.12-3.4.0.jar

sparkconnect:
	bash scripts/setupsparkconnect.sh
