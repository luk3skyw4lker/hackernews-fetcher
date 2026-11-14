BIN      := bin/hnfetcher
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X github.com/luk3skyw4lker/hackernews-fetcher/internal/cli.Version=$(VERSION)
GOOSE    := go run github.com/pressly/goose/v3/cmd/goose@v3.30.1

# Load .env if present so `make run` uses the same settings as compose.
-include .env
export

.PHONY: help build run snapshot test test-integration lint docker-up docker-down \
        migrate-up migrate-down migrate-status migration seed-up seed-down seed

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

build: ## build the binary into bin/
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN) ./cmd/hnfetcher

run: ## run the API locally (needs DATABASE_URL)
	go run ./cmd/hnfetcher serve

snapshot: ## crawl once and print the text output (no database)
	go run ./cmd/hnfetcher snapshot --format text

test: ## unit + store tests with the race detector (store tests need Docker or TEST_DATABASE_URL)
	go test -race -count=1 ./...

test-integration: ## also hit the real HackerNews API
	go test -race -count=1 -tags integration ./...

lint: ## go vet + golangci-lint
	go vet ./...
	golangci-lint run ./...

docker-up: ## build and start postgres + hn-fetcher
	docker compose up --build -d

docker-down: ## stop the stack (keeps the volume)
	docker compose down

migrate-up: ## apply schema migrations
	go run ./cmd/hnfetcher migrate up

migrate-down: ## roll back the latest schema migration
	go run ./cmd/hnfetcher migrate down

migrate-status: ## show schema and seed status
	go run ./cmd/hnfetcher migrate status
	go run ./cmd/hnfetcher seed status

migration: ## new schema migration: make migration name=add_x
	@test -n "$(name)" || (echo "usage: make migration name=<name>" && exit 2)
	$(GOOSE) -dir internal/store/migrations -s create $(name) sql

seed-up: ## apply seeds (demo snapshot)
	go run ./cmd/hnfetcher seed up

seed-down: ## roll back the latest seed
	go run ./cmd/hnfetcher seed down

seed: ## new seed file: make seed name=more_demo
	@test -n "$(name)" || (echo "usage: make seed name=<name>" && exit 2)
	$(GOOSE) -dir internal/store/seeds -s create $(name) sql
