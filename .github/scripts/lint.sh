#!/bin/bash

# Download golangci-lint binary
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/v2.0.2/install.sh | sh -s -- -b $(go env GOPATH)/bin

# Run golangci-lint on changed files
golangci-lint run --timeout 20m --verbose ./...