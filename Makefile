imageVersion ?= latest
IMAGE_TAG_GOOGLE_PROXY_MIGRATE := vcp-db-migrate:${imageVersion}
GHVSA_PAT := ${GHVSA_PAT}

# Tool versions
MOCKERY_VERSION := v2.53.4

# Registry and timestamp configuration
DEV_REGISTRY ?= ghcr.io/vcp-vsa-control-plane
TIMESTAMP := $(shell date +%Y%m%d-%H%M%S)
IMAGE_TAG := $(TIMESTAMP)

.PHONY: fix-imports
fix-imports:
	go get golang.org/x/tools/cmd/goimports
	goimports -local -format-only -w .

.PHONY: generate-mocks
generate-mocks:
	go install github.com/vektra/mockery/v2@$(MOCKERY_VERSION)
	mockery --config .mockery.yaml
	mockery --config .monkeyMocks.yaml

.PHONY: generate-monkey-mocks
generate-monkey-mocks:
	go install github.com/vektra/mockery/v2@$(MOCKERY_VERSION)
	mockery --config .monkeyMocks.yaml

.PHONY: generate-cvp-client
generate-cvp-client:
	go install github.com/go-swagger/go-swagger/cmd/swagger@v0.25.0
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

.PHONY: generate-core-api-client
generate-core-api-client:
	go run github.com/ogen-go/ogen/cmd/ogen@v1.10.1 --clean --package coreapi --config clients/core-api/.ogenclient.yml --target clients/core-api core/core-api/api.yaml

.PHONY: generate-retry-engine-wrapper
generate-retry-engine-wrapper:
	cd cmd/retry-engine-generator; go run main.go vcp core
	cd scripts; ./generate-retry-engine.sh vcp core
	cd cmd/retry-engine-generator; go run main.go metrics telemetry
	cd scripts; ./generate-retry-engine.sh metrics telemetry

.PHONY: test
PACKAGES="./..."
test:
	go test -coverprofile=vcp-coverage.out $(shell go list $(PACKAGES) | grep -v scripts/sanity)

GOMODCACHE := $(shell go env GOMODCACHE)
GOCACHE := $(shell go env GOCACHE)

.PHONY: build-all-binaries-dev
build-all-binaries-dev:
	docker build --build-arg GHVSA_PAT=$(GHVSA_PAT) -f builder/Dockerfile.build-all.dev -t vsa-binaries-builder builder
	mkdir -p app
	docker run --rm \
		-e GHVSA_PAT=$(GHVSA_PAT) \
		-v $(PWD):/src \
		-v $(GOCACHE):/go-build-cache \
		-v $(GOMODCACHE):/go/pkg/mod \
		-e GOCACHE=/go-build-cache \
		-e GOMODCACHE=/go/pkg/mod \
		vsa-binaries-builder sh -c '\
		go build -gcflags="all=-N -l" -o /src/app/vcp-worker ./worker/ && \
		go build -gcflags="all=-N -l" -o /src/app/google-proxy ./google-proxy/ && \
		go build -gcflags="all=-N -l" -o /src/app/telemetry ./telemetry/ && \
		go build -gcflags="all=-N -l" -o /src/app/ontap-proxy ./ontap-proxy/'

.PHONY: skaffold-dev
skaffold-dev:
	export $(cat skaffold.env | xargs)
	skaffold dev -p dev

.PHONY: build-all-binaries-prod
build-all-binaries-prod:
	docker build --build-arg GHVSA_PAT=$(GHVSA_PAT) -f builder/Dockerfile.build-all -t vsa-binaries-builder .
	mkdir -p artifacts
	docker rm -f vsa-binaries-builder-run || true
	docker run --name vsa-binaries-builder-run \
		-e GHVSA_PAT=$(GHVSA_PAT) \
		-v $(GOCACHE):/go-build-cache \
		-v $(GOMODCACHE):/go/pkg/mod \
		-e GOCACHE=/go-build-cache \
		-e GOMODCACHE=/go/pkg/mod \
		vsa-binaries-builder sh -c '\
		go build -o /src/artifacts/vcp-worker ./worker/ && \
		go build -o /src/artifacts/google-proxy ./google-proxy/ && \
		go build -o /src/artifacts/core ./core && \
		go build -o /src/artifacts/telemetry ./telemetry/ && \
        go build -o /src/artifacts/ontap-proxy ./ontap-proxy/'
	docker cp vsa-binaries-builder-run:/src/artifacts/. ./artifacts/
	ls artifacts
	docker rm vsa-binaries-builder-run

.PHONY: clean-artifacts
clean-artifacts:
	rm -rf artifacts

# Individual service build targets
.PHONY: build-google-proxy
build-google-proxy:
	@echo "Building google-proxy service..."
	docker build --build-arg GHVSA_PAT=$(GHVSA_PAT) -f builder/Dockerfile.build-all.dev -t vsa-binaries-builder builder
	mkdir -p app
	docker run --rm \
		-e GHVSA_PAT=$(GHVSA_PAT) \
		-v $(PWD):/src \
		-v $(GOCACHE):/go-build-cache \
		-v $(GOMODCACHE):/go/pkg/mod \
		-e GOCACHE=/go-build-cache \
		-e GOMODCACHE=/go/pkg/mod \
		vsa-binaries-builder sh -c 'go build -gcflags="all=-N -l" -o /src/app/google-proxy ./google-proxy'

.PHONY: build-ontap-proxy
build-ontap-proxy:
	@echo "Building ontap-proxy service..."
	docker build --build-arg GHVSA_PAT=$(GHVSA_PAT) -f builder/Dockerfile.build-all.dev -t vsa-binaries-builder builder
	mkdir -p app
	docker run --rm \
		-e GHVSA_PAT=$(GHVSA_PAT) \
		-v $(PWD):/src \
		-v $(GOCACHE):/go-build-cache \
		-v $(GOMODCACHE):/go/pkg/mod \
		-e GOCACHE=/go-build-cache \
		-e GOMODCACHE=/go/pkg/mod \
		vsa-binaries-builder sh -c 'go build -gcflags="all=-N -l" -o /src/app/ontap-proxy ./ontap-proxy'
		
.PHONY: build-worker
build-worker:
	@echo "Building vcp-worker service..."
	docker build --build-arg GHVSA_PAT=$(GHVSA_PAT) -f builder/Dockerfile.build-all.dev -t vsa-binaries-builder builder
	mkdir -p app
	docker run --rm \
		-e GHVSA_PAT=$(GHVSA_PAT) \
		-v $(PWD):/src \
		-v $(GOCACHE):/go-build-cache \
		-v $(GOMODCACHE):/go/pkg/mod \
		-e GOCACHE=/go-build-cache \
		-e GOMODCACHE=/go/pkg/mod \
		vsa-binaries-builder sh -c 'go build -gcflags="all=-N -l" -o /src/app/vcp-worker ./worker'

.PHONY: base-image
base-image:
	@echo "Building base development image..."
	docker build -f common/Dockerfile.dev -t base:dev .

.PHONY: google-proxy-dev-image
google-proxy-dev-image: build-google-proxy base-image
	@echo "Building google-proxy development Docker image..."
	docker build --build-arg BASE=base:dev --build-arg GHVSA_PAT=$(GHVSA_PAT) -f google-proxy/Dockerfile.dev -t $(DEV_REGISTRY)/google-proxy:$(IMAGE_TAG) .

.PHONY: worker-dev-image
worker-dev-image: build-worker base-image
	@echo "Building vcp-worker development Docker image..."
	docker build --build-arg BASE=base:dev --build-arg GHVSA_PAT=$(GHVSA_PAT) -f worker/Dockerfile.dev -t $(DEV_REGISTRY)/vcp-worker:$(IMAGE_TAG) .

.PHONY: ontap-proxy-dev-image
ontap-proxy-dev-image: build-ontap-proxy base-image
	@echo "Building ontap-proxy development Docker image..."
	docker build --build-arg BASE=base:dev --build-arg GHVSA_PAT=$(GHVSA_PAT) -f ontap-proxy/Dockerfile.dev -t $(DEV_REGISTRY)/ontap-proxy:$(IMAGE_TAG) .

# Error Framework Validation
.PHONY: validate-errors
validate-errors:
	@echo "🔍 Running error framework validation..."
	@cd core/errors && ./validate.sh

# Quick error framework status check
.PHONY: error-status
error-status:
	@echo "📊 Error Framework Status"
	@echo "========================"
	@cd core/errors && ./validate.sh --status-only 2>/dev/null || echo "Status check not available"

%:
	@:

.PHONY: run-single-test
PACKAGES="./..."
run-single-test:
	go test -coverprofile=vcp-coverage.out $(shell go list $(PACKAGES) | grep -v scripts/sanity) -run $(TEST_NAME)
