PROJECT					:= binance-proxy
VERSION					:= $(shell git describe --abbrev=0 --tags)
COMMIT_HASH     		:= $(shell git rev-parse --short HEAD)
GOLDFLAGS_VERSION 		:= $(VERSION)-$(COMMIT_HASH)
GOLDFLAGS_BUILD_TIME	:= $(shell date -Is)
LD_FLAGS 				:= -X main.Version='$(GOLDFLAGS_VERSION)' -X main.Buildtime='$(GOLDFLAGS_BUILD_TIME)' -s -w
SOURCE_FILES 			?= ./internal/... ./pkg/... ./cmd/...
UNAME 					:= $(uname -s)

BIN_DIR := bin
TOOLS_BIN_DIR := $(shell pwd)/$(BIN_DIR)
$(TOOLS_BIN_DIR):
	mkdir -p $(TOOLS_BIN_DIR)

GOLANGCI_VER := 2.5.0
GOLANGCI_BIN := golangci
GOLANGCI := $(TOOLS_BIN_DIR)/$(GOLANGCI_BIN)-$(GOLANGCI_VER)

$(info GOLDFLAGS_VERSION=$(GOLDFLAGS_VERSION))
$(info GOLDFLAGS_BUILD_TIME=$(GOLDFLAGS_BUILD_TIME))
$(info LD_FLAGS=$(LD_FLAGS))

export CGO_ENABLED=0
export GO111MODULE=on

.PHONY: all
all: help

.PHONY: help
help:	### Show targets documentation
ifeq ($(UNAME), Linux)
	@grep -P '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
else
	@awk -F ':.*###' '$$0 ~ FS {printf "%15s%s\n", $$1 ":", $$2}' \
		$(MAKEFILE_LIST) | grep -v '@awk' | sort
endif

.PHONY: clean
clean: ### Clean build files
	@rm -rf ./bin
	@go clean

.PHONY: build
build: clean ### Build binary
	@go build -tags netgo -a -v -ldflags "${LD_FLAGS}" -o ./bin/binance-proxy ./cmd/binance-proxy/*.go
	@chmod +x ./bin/*

.PHONY: run
run: ### Quick run
	@CGO_ENABLED=1 go run -race cmd/binance-proxy/*.go

.PHONY: deps
deps: ### Optimize dependencies
	@go mod tidy

.PHONY: vendor
vendor: ### Vendor dependencies
	@go mod vendor

.PHONY: install
install: ### Install binary in your system
	@go install -v cmd/binance-proxy/*.go

.PHONY: fmt
fmt: ### Format
	@gofmt -s -w .

.PHONY: vet
vet: ### Vet
	@go vet ./...

### Lint
.PHONY: lint
lint: $(GOLANGCI) fmt vet
	$(GOLANGCI) run -c .golangci.yml -v


### Clean test 
.PHONY: test-clean
test-clean: ### Clean test cache
	@go clean -testcache ./...

.PHONY: test
test: lint ### Run tests
	@go test -v  -coverprofile=cover.out -timeout 10s ./...

.PHONY: cover
cover: test ### Run tests and generate coverage
	@go tool cover -html=cover.out -o=cover.html


$(GOLANGCI): $(TOOLS_BIN_DIR)
ifeq (,$(wildcard $(GOLANGCI)))
	mkdir -p /tmp/golangci && \
	cd /tmp/golangci && \
	curl -L https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_VER)/golangci-lint-$(GOLANGCI_VER)-linux-amd64.tar.gz -o golangci-lint-$(GOLANGCI_VER)-linux-amd64.tar.gz && \
	tar xvf golangci-lint-$(GOLANGCI_VER)-linux-amd64.tar.gz && \
	mv golangci-lint-$(GOLANGCI_VER)-linux-amd64/golangci-lint $(GOLANGCI) && \
	ln -sf $(GOLANGCI) $(TOOLS_BIN_DIR)/$(GOLANGCI_BIN)
endif