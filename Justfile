alias b := build
alias c := clean
alias t := test
alias l := lint

# Build the binary
build:
    go build .

# Run tests
test:
    go test -v ./...

# Run linter
lint:
    golangci-lint run

# Clean build artifacts
clean:
    go clean
