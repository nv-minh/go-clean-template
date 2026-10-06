# Developer entrypoint. Run `make help`.
SHELL := /bin/bash
.DEFAULT_GOAL := help

# Pinned tool versions: run through `go run`, so nothing needs to be installed globally and CI
# uses exactly the same versions as laptops.
GOLANGCI_LINT_VERSION := v2.14.0
SQLC_VERSION          := v1.31.1
GOVULNCHECK_VERSION   := v1.8.0
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
SQLC          := go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
GOVULNCHECK   := go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
BINS    := api worker migrate
WITH_ENV = set -a && . ./.env && set +a &&

##@ Help
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Build
.PHONY: tidy build clean
tidy: ## go mod tidy
	go mod tidy

build: ## Build all binaries into ./bin
	@for b in $(BINS); do echo "build $$b"; CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$$b ./cmd/$$b || exit 1; done

clean: ## Remove build artifacts
	rm -rf bin coverage.out

##@ Run locally (needs `make infra-up migrate-up`)
.PHONY: env run-api run-worker
env: ## Create .env from .env.example if missing
	@test -f .env || cp .env.example .env

run-api: env ## Run the API
	$(WITH_ENV) go run ./cmd/api

run-worker: env ## Run the worker (outbox relay + consumer)
	$(WITH_ENV) OPS_ADDR=:9091 OPS_PPROF_ADDR=127.0.0.1:6061 go run ./cmd/worker

##@ Quality
.PHONY: fmt lint vuln test test-integration cover bench check
fmt: ## Format code (gofumpt + goimports via golangci-lint)
	$(GOLANGCI_LINT) fmt

lint: ## Lint (golangci-lint v2)
	$(GOLANGCI_LINT) run ./...

vuln: ## Scan dependencies and stdlib for known vulnerabilities
	$(GOVULNCHECK) ./...

test: ## Unit tests with the race detector
	go test -race -count=1 ./...

test-integration: ## Integration tests (needs Docker: testcontainers starts Postgres, Redis, Kafka)
	go test -race -count=1 -tags=integration -timeout=10m ./...

cover: ## Unit test coverage report
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -n 1

bench: ## Benchmarks
	go test -run='^$$' -bench=. -benchmem ./...

check: tidy fmt lint test ## Everything CI checks, locally

##@ Database
.PHONY: sqlc sqlc-check migrate-up migrate-down migrate-new
sqlc: ## Regenerate type safe query code from db/queries
	$(SQLC) generate

sqlc-check: sqlc ## Fail if generated code is out of date
	git diff --exit-code -- internal/adapter/repository/sqlcdb

migrate-up: env ## Apply migrations
	$(WITH_ENV) go run ./cmd/migrate up

migrate-down: env ## Roll back one migration
	$(WITH_ENV) go run ./cmd/migrate down 1

migrate-new: ## Create a migration pair: make migrate-new name=add_foo
	@test -n "$(name)" || (echo "usage: make migrate-new name=add_foo" && exit 1)
	@n=$$(printf "%06d" $$(( $$(ls db/migrations/*.up.sql 2>/dev/null | wc -l) + 1 ))); \
	touch db/migrations/$${n}_$(name).up.sql db/migrations/$${n}_$(name).down.sql; \
	echo "created db/migrations/$${n}_$(name).{up,down}.sql"

##@ Docker
.PHONY: infra-up up down logs docker-build
infra-up: ## Start Postgres, Redis, Kafka, Jaeger, Prometheus, Grafana
	docker compose -f deploy/docker-compose.yml up -d --wait

up: ## Start infra plus migrate, api and worker containers
	docker compose -f deploy/docker-compose.yml --profile app up -d --build --wait

down: ## Stop everything and delete volumes
	docker compose -f deploy/docker-compose.yml --profile app down -v

logs: ## Tail api and worker logs
	docker compose -f deploy/docker-compose.yml --profile app logs -f api worker

docker-build: ## Build the api, worker and migrate images
	@for b in $(BINS); do docker build -f deploy/Dockerfile --target $$b --build-arg VERSION=$(VERSION) -t go-clean-template-$$b:$(VERSION) . || exit 1; done

##@ Load test
.PHONY: load-test
load-test: ## k6 load test against http://localhost:8080 (needs k6)
	k6 run test/load/orders.js
