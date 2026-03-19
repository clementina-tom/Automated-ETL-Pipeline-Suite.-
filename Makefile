.PHONY: install test lint format build run

install:
	python -m pip install --upgrade pip
	pip install -e .[test]

format:
	ruff format .

lint:
	ruff check .
	mypy .

test:
	pytest -v

build:
	docker build -t automated-etl-pipeline:local .

run:
	python pipeline.py
