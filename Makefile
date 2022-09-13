# Check style and linting
.PHONY: check-black check-isort check-flake8 check-mypy lint

check-black:
	@echo "--> Running black checks"
	@black --check --diff --exclude=venv .

check-isort:
	@echo "--> Running isort checks"
	@isort --check-only .

check-flake8:
	@echo "--> Running flake8 checks"
	@flake8 .

check-mypy:
	@echo "--> Running mypy checks"
	@mypy --exclude dbt/adapters/clickhouse/__init__.py --exclude conftest.py .

check-yamllint:
	@echo "--> Running yamllint checks"
	@yamllint dbt tests .github 

lint: check-black check-isort check-flake8 check-mypy check-yamllint

# Format code
.PHONY: fmt

fmt:
	@echo "--> Running isort"
	@isort .
	@echo "--> Running black"
	@black .
