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
	bash scripts/add_avro_jar.sh
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
	rm -rf docs/build
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api fugue/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_sql fugue_sql/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_spark fugue_spark/
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api_dask fugue_dask/
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

test:
	python3 -bb -m pytest tests/

testcore:
	python3 -bb -m pytest tests/fugue

testspark:
	python3 -bb -m pytest tests/fugue_spark

testdask:
	python3 -bb -m pytest tests/fugue_dask

testduck:
	python3 -bb -m pytest tests/fugue_duckdb

testsql:
	python3 -bb -m pytest tests/fugue_sql

testibis:
	python3 -bb -m pytest tests/fugue_ibis

testnotebook:
	pip install .
	jupyter nbextension install --user --py fugue_notebook
	jupyter nbextension enable fugue_notebook --py
	jupyter nbconvert --execute --clear-output tests/fugue_notebook/test_notebook.ipynb

sql:
	java -Xmx500M -jar bin/antlr-4.9-complete.jar -Dlanguage=Python3 -visitor -no-listener fugue_sql/_antlr/fugue_sql.g4
	rm fugue_sql/_antlr/*.interp
	rm fugue_sql/_antlr/*.tokens
