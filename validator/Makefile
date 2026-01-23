# Makefile for GCP Validator

# Project metadata
PROJECT_NAME := gcp-validator
VERSION ?= 0.0.1
IMAGE_REGISTRY ?= quay.io/rh-ee-dawang
IMAGE_TAG ?= latest

# Build metadata
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_TAG := $(shell git describe --tags --exact-match 2>/dev/null || echo "")

# Dev image configuration - set QUAY_USER to push to personal registry
# Usage: QUAY_USER=myuser make image-dev
QUAY_USER ?=
DEV_TAG ?= dev-$(GIT_COMMIT)
BUILD_DATE := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')

# LDFLAGS for build
# Note: Variables are in package main, so use main.varName (not full import path)
LDFLAGS := -w -s
LDFLAGS += -X main.version=$(VERSION)
LDFLAGS += -X main.commit=$(GIT_COMMIT)
LDFLAGS += -X main.buildDate=$(BUILD_DATE)
ifneq ($(GIT_TAG),)
LDFLAGS += -X main.tag=$(GIT_TAG)
endif

# Go parameters
GOCMD := go
GOBUILD := $(GOCMD) build
GOTEST := $(GOCMD) test
GOMOD := $(GOCMD) mod
GOFMT := gofmt
GOIMPORTS := goimports

# Test parameters
TEST_TIMEOUT := 10m
RACE_FLAG := -race
COVERAGE_OUT := coverage.out
COVERAGE_HTML := coverage.html

# Container runtime detection
DOCKER_AVAILABLE := $(shell if docker info >/dev/null 2>&1; then echo "true"; else echo "false"; fi)
PODMAN_AVAILABLE := $(shell if podman info >/dev/null 2>&1; then echo "true"; else echo "false"; fi)

ifeq ($(DOCKER_AVAILABLE),true)
    CONTAINER_RUNTIME := docker
    CONTAINER_CMD := docker
else ifeq ($(PODMAN_AVAILABLE),true)
    CONTAINER_RUNTIME := podman
    CONTAINER_CMD := podman
else
    CONTAINER_RUNTIME := none
    CONTAINER_CMD := sh -c 'echo "No container runtime found. Please install Docker or Podman." && exit 1'
endif

# Install directory (defaults to $GOPATH/bin or $HOME/go/bin)
GOPATH ?= $(shell $(GOCMD) env GOPATH)
BINDIR ?= $(GOPATH)/bin

# Directories
# Find all Go packages, excluding vendor and test directories
PKG_DIRS := $(shell $(GOCMD) list ./... 2>/dev/null | grep -v /vendor/ | grep -v /test/)

.PHONY: help
help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: test
test: ## Run unit tests with race detection
	@echo "Running unit tests..."
	$(GOTEST) -v $(RACE_FLAG) -timeout $(TEST_TIMEOUT) $(PKG_DIRS)

.PHONY: test-coverage
test-coverage: ## Run unit tests with coverage report
	@echo "Running unit tests with coverage..."
	$(GOTEST) -v $(RACE_FLAG) -timeout $(TEST_TIMEOUT) -coverprofile=$(COVERAGE_OUT) -covermode=atomic $(PKG_DIRS)
	@echo "Coverage report generated: $(COVERAGE_OUT)"
	@echo "To view HTML coverage report, run: make test-coverage-html"

.PHONY: test-coverage-html
test-coverage-html: test-coverage ## Generate HTML coverage report
	@echo "Generating HTML coverage report..."
	$(GOCMD) tool cover -html=$(COVERAGE_OUT) -o $(COVERAGE_HTML)
	@echo "HTML coverage report generated: $(COVERAGE_HTML)"

.PHONY: test-integration
test-integration: ## Run integration tests (requires GCP credentials and PROJECT_ID)
	@echo "Running integration tests..."
	@if [ -z "$$PROJECT_ID" ]; then \
		echo "❌ ERROR: PROJECT_ID environment variable is not set"; \
		echo ""; \
		echo "Integration tests require a real GCP project."; \
		echo "Please set PROJECT_ID:"; \
		echo "  export PROJECT_ID=your-gcp-project-id"; \
		echo ""; \
		echo "You also need valid GCP credentials:"; \
		echo "  gcloud auth application-default login"; \
		echo "  OR"; \
		echo "  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"; \
		echo ""; \
		exit 1; \
	fi
	@if [ -d "test/integration" ] && [ -n "$$(find test/integration -name '*_test.go' 2>/dev/null)" ]; then \
		echo "PROJECT_ID: $$PROJECT_ID"; \
		echo "Running tests with -tags=integration..."; \
		$(GOTEST) -tags=integration -v $(RACE_FLAG) -timeout $(TEST_TIMEOUT) ./test/integration/...; \
	else \
		echo "No integration tests found in test/integration directory"; \
		echo "Please add integration tests to test/integration directory"; \
		exit 0; \
	fi

.PHONY: lint
lint: ## Run golangci-lint
	@echo "Running golangci-lint..."
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint cache clean && golangci-lint run; \
	else \
		echo "Error: golangci-lint not found. Please install it:"; \
		echo "  go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	fi

.PHONY: fmt
fmt: ## Format code with gofmt and goimports
	@echo "Formatting code..."
	@if command -v $(GOIMPORTS) > /dev/null; then \
		$(GOIMPORTS) -w .; \
	else \
		$(GOFMT) -w .; \
	fi

.PHONY: mod-tidy
mod-tidy: ## Tidy Go module dependencies
	@echo "Tidying Go modules..."
	$(GOMOD) tidy
	$(GOMOD) verify

.PHONY: binary
binary: ## Build binary
	@echo "Building $(PROJECT_NAME)..."
	@echo "Version: $(VERSION), Commit: $(GIT_COMMIT), BuildDate: $(BUILD_DATE)"
	@mkdir -p bin
	CGO_ENABLED=0 $(GOBUILD) -ldflags="$(LDFLAGS)" -o bin/validator ./cmd/validator

.PHONY: build
build: binary ## Alias for 'binary'

.PHONY: install
install: binary ## Install binary to BINDIR (default: $GOPATH/bin)
	@echo "Installing $(PROJECT_NAME) to $(BINDIR)..."
	@mkdir -p $(BINDIR)
	cp bin/validator $(BINDIR)/validator
	@echo "✅ Installed: $(BINDIR)/validator"

.PHONY: clean
clean: ## Clean build artifacts and test coverage files
	@echo "Cleaning..."
	rm -rf bin/
	rm -f $(COVERAGE_OUT) $(COVERAGE_HTML)

.PHONY: image
image: ## Build container image with Docker or Podman
ifeq ($(CONTAINER_RUNTIME),none)
	@echo "❌ ERROR: No container runtime found"
	@echo "Please install Docker or Podman"
	@exit 1
else
	@echo "Building container image with $(CONTAINER_RUNTIME)..."
	$(CONTAINER_CMD) build --platform linux/amd64 --no-cache --build-arg VERSION=$(VERSION) --build-arg GIT_COMMIT=$(GIT_COMMIT) -t $(IMAGE_REGISTRY)/$(PROJECT_NAME):$(IMAGE_TAG) .
	@echo "✅ Image built: $(IMAGE_REGISTRY)/$(PROJECT_NAME):$(IMAGE_TAG)"
endif

.PHONY: image-push
image-push: image ## Build and push container image to registry
ifeq ($(CONTAINER_RUNTIME),none)
	@echo "❌ ERROR: No container runtime found"
	@echo "Please install Docker or Podman"
	@exit 1
else
	@echo "Pushing image $(IMAGE_REGISTRY)/$(PROJECT_NAME):$(IMAGE_TAG)..."
	$(CONTAINER_CMD) push $(IMAGE_REGISTRY)/$(PROJECT_NAME):$(IMAGE_TAG)
	@echo "✅ Image pushed: $(IMAGE_REGISTRY)/$(PROJECT_NAME):$(IMAGE_TAG)"
endif

.PHONY: image-dev
image-dev: ## Build and push to personal Quay registry (requires QUAY_USER)
ifndef QUAY_USER
	@echo "❌ ERROR: QUAY_USER is not set"
	@echo ""
	@echo "Usage: QUAY_USER=myuser make image-dev"
	@echo ""
	@echo "This will build and push to: quay.io/$$QUAY_USER/$(PROJECT_NAME):$(DEV_TAG)"
	@exit 1
endif
ifeq ($(CONTAINER_RUNTIME),none)
	@echo "❌ ERROR: No container runtime found"
	@echo "Please install Docker or Podman"
	@exit 1
else
	@echo "Building dev image quay.io/$(QUAY_USER)/$(PROJECT_NAME):$(DEV_TAG)..."
	$(CONTAINER_CMD) build --platform linux/amd64 --build-arg BASE_IMAGE=alpine:3.21 --build-arg VERSION=$(VERSION) --build-arg GIT_COMMIT=$(GIT_COMMIT) -t quay.io/$(QUAY_USER)/$(PROJECT_NAME):$(DEV_TAG) .
	@echo "Pushing dev image..."
	$(CONTAINER_CMD) push quay.io/$(QUAY_USER)/$(PROJECT_NAME):$(DEV_TAG)
	@echo ""
	@echo "✅ Dev image pushed: quay.io/$(QUAY_USER)/$(PROJECT_NAME):$(DEV_TAG)"
endif

.PHONY: verify
verify: lint test ## Run all verification checks (lint + test)

.DEFAULT_GOAL := help
