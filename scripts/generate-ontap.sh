#!/bin/bash

set -e

source generate-util.sh

command_exists() {
  command -v "$1" >/dev/null 2>&1
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

    sort -u swagger_operations.txt > tempFile && mv tempFile swagger_operations.txt
    sort -u swagger_models.txt > tempFile && mv tempFile swagger_models.txt

    generate_client_code true true

    generate_ontap_checksums

    mv newChecksumsFile.checksum ../../checksums/ontap-rest-checksums
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

    generate_ontap_priv_checksums

    mv newChecksumsFile.checksum ../../../checksums/ontap-rest-priv-checksums
  else
    echo "Everything is up to date. Client code is already the latest."
    rm -f newChecksumsFile.checksum
  fi

  popd &> /dev/null
}

install_go_swagger

generate_ontap
generate_ontap_priv