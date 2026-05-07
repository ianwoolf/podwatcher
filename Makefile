APP_NAME = podwatcher
BUILD_DIR = bin
GO_FILES = $(shell find . -name '*.go' -type f)

LDFLAGS = -ldflags="-s -w"

.PHONY: all build run clean test mod docker-build docker-run help

all: clean build

build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME) ./cmd/server
	@echo "Build complete: $(BUILD_DIR)/$(APP_NAME)"

run:
	@echo "Running $(APP_NAME)..."
	go run ./cmd/server $(ARGS)

dev:
	@echo "Running in dev mode..."
	go run ./cmd/server -kubeconfig=$(HOME)/.kube/config

test:
	go test -v ./...

clean:
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR)/
	go clean
	@echo "Clean complete"

mod:
	go mod download
	go mod tidy

fmt:
	go fmt ./...

lint:
	golangci-lint run

docker-build:
	docker build -t $(APP_NAME):latest .

docker-run:
	docker run -v $(HOME)/.kube/config:/root/.kube/config -p 8080:8080 $(APP_NAME):latest

help:
	@echo "Available targets:"
	@echo "  build       - Build the application"
	@echo "  run         - Run the application (use ARGS='-kubeconfig=/path' for flags)"
	@echo "  dev         - Run in dev mode with default kubeconfig"
	@echo "  clean       - Clean build artifacts"
	@echo "  test        - Run tests"
	@echo "  mod         - Download and tidy dependencies"
	@echo "  fmt         - Format code"
	@echo "  docker-build- Build Docker image"
	@echo "  docker-run  - Run Docker container"