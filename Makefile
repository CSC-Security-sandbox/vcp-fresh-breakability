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

.PHONY: fix-imports
fix-imports:
	go get golang.org/x/tools/cmd/goimports
	goimports -local -format-only -w .