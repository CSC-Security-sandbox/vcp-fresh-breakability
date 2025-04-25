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

