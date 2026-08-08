.DEFAULT_GOAL := help
.PHONY: help up down logs restart build migrate revision shell psql redis-cli bucket lint fmt test test-fast clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Start the whole stack (UI on http://localhost:8000)
	docker compose up --build -d
	@echo "UI    -> http://localhost:8000"
	@echo "Docs  -> http://localhost:8000/docs"
	@echo "S3    -> http://localhost:4566"

down: ## Stop the stack and drop its volumes
	docker compose down -v

logs: ## Tail the API and worker logs
	docker compose logs -f api worker

restart: ## Recreate the API and worker containers
	docker compose up -d --force-recreate --no-deps api worker

build: ## Rebuild the image
	docker compose build

migrate: ## Apply migrations inside the API container
	docker compose exec api alembic upgrade head

revision: ## Autogenerate a migration: make revision m="add thing"
	docker compose exec api alembic revision --autogenerate -m "$(m)"

shell: ## Shell into the API container
	docker compose exec api bash

psql: ## Open psql against the local database
	docker compose exec postgres psql -U filestore -d filestore

redis-cli: ## Open redis-cli against the local queue
	docker compose exec redis redis-cli

bucket: ## List the uploads bucket
	docker compose exec localstack awslocal s3 ls s3://user-uploads --recursive

lint: ## Ruff check and format check
	ruff check .
	ruff format --check .

fmt: ## Apply Ruff fixes and formatting
	ruff check --fix .
	ruff format .

test: ## Run the integration suite with coverage
	pytest -v --cov=app --cov-report=term-missing

test-fast: ## Run the suite without coverage
	pytest -q

clean: ## Remove caches and build artefacts
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage coverage.xml
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
