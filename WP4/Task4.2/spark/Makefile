
help:
	@echo "clean - remove all build and Python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "build - package"

all: default

default: clean build

clean: clean-build clean-pyc

clean-build:
	rm -fr dist/

clean-pyc:
	find ./ -name '*.pyc' -exec rm -f {} +
	find ./ -name '*.pyo' -exec rm -f {} +
	find ./ -name '__pycache__' -exec rm -fr {} +

build: clean
	mkdir ./dist
	cp ./main.py ./config.json ./spark_submit.sh ./dist
	zip -x config.json* main.py Makefile spark_submit.sh -r ./dist/jobs.zip .
