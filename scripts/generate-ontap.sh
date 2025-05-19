#!/bin/bash

set -e

source generate-util.sh

command_exists() {
  command -v "\$1" >/dev/null 2>&1
}

install_go_swagger() {
  if ! command_exists swagger; then
    local version="v0.31.0"

    echo "go-swagger is not installed. Installing go-swagger ${version} ..."

    local dir
    dir=$(mktemp -d)

    local os_arch
    os_arch=$(uname | tr '[:upper:]' '[:lower:]')_amd64

    local download_url="https://github.com/go-swagger/go-swagger/releases/download/${version}/swagger_${os_arch}"

    curl -s -o "$dir/swagger" -L'#' "$download_url"
    if [ $? -ne 0 ]; then
      echo "Failed to download go-swagger from $download_url"
      rm -rf "$dir"
      exit 1
    fi

    chmod 755 "$dir/swagger"
    mkdir -p ~/bin
    mv "$dir/swagger" ~/bin/swagger
    rm -rf "$dir"

    export PATH="$HOME/bin:$PATH"

    if ! echo "$PATH" | grep -q "$HOME/bin"; then
      echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
      echo 'export PATH="$HOME/bin:$PATH"' >> ~/.profile
    fi

    echo "go-swagger ${version} installed successfully."
  else
    echo "go-swagger is already installed."
  fi
}

install_goimports() {
  if ! command_exists "goimports"; then
    echo "goimports is not installed. Installing goimports ..."

    GO111MODULE=on go install golang.org/x/tools/cmd/goimports@latest
    if [ $? -ne 0 ]; then
      echo "Failed to install goimports"
      exit 1
    fi

    echo "goimports installed successfully."
  else
    echo "goimports is already installed."
  fi
}

generate_client_code() {
  local include_swagger_operations=$1
  local include_swagger_models=$2

  local cmd="swagger generate client -f swagger.yaml"

  if [[ "$include_swagger_operations" == "true" ]]; then
    cmd+=" $(awk '{print "-O " $0}' ./swagger_operations.txt)"
  fi

  if [[ "$include_swagger_models" == "true" ]]; then
    cmd+=" $(awk '{print "-M " $0}' ./swagger_models.txt)"
  fi

  eval $cmd

  gofmt -w .
  go mod tidy
}

generate_models_for_given_operations(){
  echo "starting to generate swagger models for given operations"
  go run ../../scripts/fetch_models.go
  echo "successfully created swagger models for given operations"
}

cleanup() {
  rm -f ./swagger_models.txt
}

generate_ontap_mocks() {
  echo "Generating mocks for ONTAP REST API..."

  go run ../../cmd/mock-generator client/cloud/cloud_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/cloud/
  go run ../../cmd/mock-generator client/cluster/cluster_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/cluster/
  go run ../../cmd/mock-generator client/n_a_s/nas_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/n_a_s/
  go run ../../cmd/mock-generator client/networking/networking_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/networking/
  go run ../../cmd/mock-generator client/object_store/object_store_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/object_store/
  go run ../../cmd/mock-generator client/s_a_n/san_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/s_a_n/
  go run ../../cmd/mock-generator client/security/security_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/security/
  go run ../../cmd/mock-generator client/snapmirror/snapmirror_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/snapmirror/
  go run ../../cmd/mock-generator client/storage/storage_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/storage/
  go run ../../cmd/mock-generator client/svm/svm_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/svm/

  echo "Generated mocks for ONTAP REST API."
}

generate_ontap_priv_mocks() {
  echo "Generating mocks for private CLI passthrough ONTAP REST API..."

  go run ../../../cmd/mock-generator client/object_store/object_store_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/object_store/
  go run ../../../cmd/mock-generator client/operations/operations_client.go ClientService && mv client_service_mock.go client_service_mock_test.go client/operations/

  echo "Generated mocks for private CLI passthrough ONTAP REST API."
}

# Generate client code for selective ONTAP REST API
generate_ontap() {
  echo "Generating client code for selective ONTAP REST API..."

  pushd ../clients/ontap-rest > /dev/null

  if ! generate_ontap_checksums; then
    echo "Failed to generate checksums due to missing files."
    exit 1
  fi

  if ! cmp ../../checksums/ontap-rest-checksums newChecksumsFile.checksum; then
    rm -rf ./client
    rm -rf ./models

    generate_models_for_given_operations

    sort -u swagger_operations.txt > tempFile && mv tempFile swagger_operations.txt
    sort -u swagger_models.txt > tempFile && mv tempFile swagger_models.txt

    generate_client_code true true

    generate_ontap_mocks

    generate_ontap_checksums

    mv newChecksumsFile.checksum ../../checksums/ontap-rest-checksums

    cleanup
  else
    echo "Everything is up to date. Client code is already the latest."
    rm -f newChecksumsFile.checksum
  fi

  popd &> /dev/null
}

# Generate client code for private CLI passthrough ONTAP REST API
generate_ontap_priv() {
  echo "Generating client code for private CLI passthrough ONTAP REST API..."

  pushd ../clients/ontap-rest/priv > /dev/null

  if ! generate_ontap_priv_checksums; then
    echo "Failed to generate checksums due to missing files."
    exit 1
  fi

  if ! cmp ../../../checksums/ontap-rest-priv-checksums newChecksumsFile.checksum; then
    rm -rf ./client
    rm -rf ./models

    generate_client_code false false

    generate_ontap_priv_mocks

    generate_ontap_priv_checksums

    mv newChecksumsFile.checksum ../../../checksums/ontap-rest-priv-checksums
  else
    echo "Everything is up to date. Client code is already the latest."
    rm -f newChecksumsFile.checksum
  fi

  popd &> /dev/null
}

install_go_swagger
install_goimports

generate_ontap
generate_ontap_priv