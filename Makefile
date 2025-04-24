# Run Go tests without coverage
.PHONY: test
test:
	go test ./...

# Run Go tests with coverage
.PHONY: test-with-coverage-unfiltered
test-with-coverage-unfiltered:
	scripts/test.sh

.PHONY: test-with-coverage-filtered
test-with-coverage-filtered:
	scripts/test.sh --filtered

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

.PHONY: generate-cvp-client
generate-cvp-client:
	rm -rf clients/cvp/cvpapi clients/cvp/models
	cd clients/cvp;swagger generate client -f swagger-gcp.yaml -c cvpapi -A cvp

