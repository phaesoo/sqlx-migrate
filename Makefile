# Meta
NAME := sqlx-migrate

# Install dependencies
.PHONY: deps
deps:
	go mod download

# Run all unit tests
.PHONY: test
test:
	go test -short ./...

# Run all benchmarks
.PHONY: bench
bench:
	go test -short -bench=. ./...

# test with coverage turned on
.PHONY: cover
cover:
	go test -short -cover -covermode=atomic ./...
