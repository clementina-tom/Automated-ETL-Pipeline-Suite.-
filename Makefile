.PHONY: install test run lint

install:
	python -m pip install --upgrade pip
	pip install -e .

test:
	pytest -v

run:
	python pipeline.py

lint:
	python -m py_compile pipeline.py
