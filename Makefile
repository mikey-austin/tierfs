BINARY      := tierfs
CMD         := ./cmd/tierfs
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS     := -ldflags "-X main.version=$(VERSION) -s -w"
GOFLAGS     := -trimpath

.PHONY: all build ui test test-unit test-integration lint fmt vet clean docker help

all: build

## build: compile the tierfs binary
build:
	go build $(GOFLAGS) $(LDFLAGS) -o bin/$(BINARY) $(CMD)

## ui: build the admin UI for production
ui:
	cd web/admin && npm install && npm run build

## install: install to GOPATH/bin
install:
	go install $(GOFLAGS) $(LDFLAGS) $(CMD)

## test: run all tests (unit + integration)
test: test-unit test-integration

## test-unit: run unit tests only (no integration tag required)
test-unit:
	go test ./internal/... -count=1 -race -timeout 60s -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

## test-integration: run full-stack integration tests
test-integration:
	go test ./integration/... -count=1 -race -timeout 120s -v

## bench: run benchmarks
bench:
	go test ./internal/... -bench=. -benchmem -run=^$$ -count=3

## lint: run golangci-lint
lint:
	golangci-lint run ./...

## fmt: format all Go source
fmt:
	gofmt -w -s .

## vet: run go vet
vet:
	go vet ./...

## clean: remove build artifacts
clean:
	rm -rf bin/ coverage.out

## docker: build Docker image
docker:
	docker build -t tierfs:$(VERSION) .

## docker-run: run with example config (requires FUSE on the host)
docker-run:
	docker run --rm \
		--cap-add SYS_ADMIN \
		--device /dev/fuse \
		--security-opt apparmor:unconfined \
		-v $(PWD)/tierfs.example.toml:/etc/tierfs/tierfs.toml:ro \
		-v /tmp/tierfs-data:/data \
		tierfs:$(VERSION)

## help: print this message
help:
	@grep -E '^## ' Makefile | sed 's/## //'
