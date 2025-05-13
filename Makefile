imageVersion ?= latest
IMAGE_TAG_GOOGLE_PROXY_MIGRATE := ghcr.io/vcp-vsa-control-plane/vcp-db-migrate:${imageVersion}

.PHONY: fix-imports
fix-imports:
	go get golang.org/x/tools/cmd/goimports
	goimports -local -format-only -w .

.PHONY: generate-mocks
generate-mocks:
	go get github.com/vektra/mockery/v2@v2.53.2	
	mockery --config .mockery.yaml

.PHONY: generate-cvp-client
generate-cvp-client:
	rm -rf clients/cvp/cvpapi clients/cvp/models
	cd clients/cvp;swagger generate client -f swagger-gcp.yaml -c cvpapi -A cvp

.PHONY: vcp-db-migrate-image
vcp-db-migrate-image: vcp-db-migrate-linux
        docker buildx build -t ${IMAGE_TAG_GOOGLE_PROXY_MIGRATE} --platform "linux/amd64,linux/arm64" --push -f core/migrate.Dockerfile .

.PHONY: vcp-db-migrate-linux
vcp-db-migrate-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o core/build/linux/bin/vcp-db-migrate ./tools/migrate
	
