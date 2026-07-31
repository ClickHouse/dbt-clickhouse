# Check style and linting
.PHONY: check-ruff-format check-ruff check-mypy lint

check-ruff-format:
	@echo "--> Running ruff format checks"
	@ruff format --check --diff .

check-ruff:
	@echo "--> Running ruff checks"
	@ruff check .

check-mypy:
	@echo "--> Running mypy checks"
	@mypy --exclude dbt/adapters/clickhouse/__init__.py --exclude conftest.py .

check-yamllint:
	@echo "--> Running yamllint checks"
	@yamllint dbt tests .github 

lint: check-ruff-format check-ruff check-mypy check-yamllint

# Format code
.PHONY: fmt

fmt:
	@echo "--> Running ruff fixes"
	@ruff check --fix .
	@echo "--> Running ruff format"
	@ruff format .
