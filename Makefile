imageVersion ?= latest
IMAGE_TAG_GOOGLE_PROXY_MIGRATE := vcp-db-migrate:${imageVersion}
GHVSA_PAT := ${GHVSA_PAT}

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
	docker buildx build -t ${IMAGE_TAG_GOOGLE_PROXY_MIGRATE} --platform linux/amd64 -f core/migrate.Dockerfile .

.PHONY: vcp-db-migrate-linux
vcp-db-migrate-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o core/build/linux/bin/vcp-db-migrate ./tools/migrate

.PHONY: generate-google-proxy
generate-google-proxy:
	go run github.com/ogen-go/ogen/cmd/ogen@v1.10.1 --clean --package gcpserver --config google-proxy/api/.ogenserver.yml --target google-proxy/api/gcp-servergen google-proxy/api/gcp-api.yaml

.PHONY: generate-core-api
generate-core-api:
	go run github.com/ogen-go/ogen/cmd/ogen@v1.10.1 --clean --package coreapiserver --config core/core-api/.ogenserver.yml --target core/core-api/core-servergen core/core-api/api.yaml

.PHONY: generate-google-proxy-client
generate-google-proxy-client:
	go run github.com/ogen-go/ogen/cmd/ogen@v1.10.1 --clean --package googleproxyclient --config clients/google-proxy-client/.ogenserver.yml --target clients/google-proxy-client google-proxy/api/gcp-api.yaml

.PHONY: generate-retry-engine-wrapper
generate-retry-engine-wrapper:
	cd cmd/retry-engine-generator; go run main.go
	cd scripts; ./generate-retry-engine.sh

.PHONY: test
PACKAGES="./..."
test:
	go test -coverprofile=vcp-coverage.out $(PACKAGES)

.PHONY: build-all-binaries
build-all-binaries-dev:
	docker build --build-arg GHVSA_PAT=$(GHVSA_PAT) -f builder/Dockerfile.build-all -t vsa-binaries-builder builder
	mkdir -p artifacts
	docker run --rm \
		-e GHVSA_PAT=$(GHVSA_PAT) \
		-v $(PWD):/src \
		-v $(HOME)/.cache/go-build:/go-build-cache \
		-v $(HOME)/go/pkg/mod:/go/pkg/mod \
		-e GOCACHE=/go-build-cache \
		-e GOMODCACHE=/go/pkg/mod \
		vsa-binaries-builder sh -c '\
		go build  -o /src/artifacts/vcp-worker ./worker/ && \
		go build -o /src/artifacts/google-proxy ./google-proxy/ && \
		go build  -o /src/artifacts/telemetry ./telemetry/'

.PHONY: skaffold-dev
skaffold-dev:
	skaffold dev -p dev
%:
	@:
