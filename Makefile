# === Core Service Makefile ===

PYTHON=python3
VENV=.venv
ACTIVATE=. $(VENV)/bin/activate

# Load .env file if present
export $(shell test -f .env && sed 's/=.*//' .env)

# Tools
BLACK=black
ISORT=isort
FLAKE8=flake8
MYPY=mypy
PYTEST=pytest
UVICORN=uvicorn

.PHONY: help install run test lint format clean

help:
	@echo "Available commands:"
	@echo "  make install     - Create venv and install deps"
	@echo "  make run         - Run the FastAPI server"
	@echo "  make test        - Run tests"
	@echo "  make lint        - Lint with flake8 + mypy"
	@echo "  make format      - Format code with black + isort"
	@echo "  make clean       - Remove virtualenv and caches"

install:
	$(PYTHON) -m venv $(VENV)
	$(ACTIVATE) && pip install --upgrade pip && pip install -r requirements.txt

run:
	$(ACTIVATE) && $(UVICORN) app.main:app --reload --host=0.0.0.0 --port=8000

test:
	$(ACTIVATE) && $(PYTEST)

lint:
	$(ACTIVATE) && $(FLAKE8) .
	$(ACTIVATE) && $(MYPY) .

format:
	$(ACTIVATE) && $(BLACK) .
	$(ACTIVATE) && $(ISORT) .

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache .mypy_cache
