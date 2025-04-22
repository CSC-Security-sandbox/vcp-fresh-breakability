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
	.github/scripts/lint.sh

.PHONY: fix-imports
fix-imports:
	go get golang.org/x/tools/cmd/goimports
	goimports -local -format-only -w .

.PHONY: generate-mocks
generate-mocks:
	go get github.com/vektra/mockery/v2@v2.43.2
	mockery --config .mockery.yaml
