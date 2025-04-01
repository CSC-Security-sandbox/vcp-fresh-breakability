# Run Go tests without coverage
.PHONY: test
test:
	go test ./...

# Run Go tests with coverage
.PHONY: test-with-coverage
test-with-coverage:
	scripts/test.sh

.PHONY: lint
lint: 
	scripts/lint.sh