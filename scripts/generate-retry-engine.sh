#!/bin/bash

set -e

source generate-util.sh

command_exists() {
  command -v "\$1" >/dev/null 2>&1
}

generate_retry_engine_code_for_DB_operations(){
  echo "starting to generate retry engine code for DB operations"
  pushd ../../cmd/retry-engine-generator > /dev/null
  go run main.go
  echo "successfully created retry engine code for DB operations"
}

cleanup() {
  rm -f ./swagger_models.txt
}

# Generate retry engine code for DB operation
generate_retry_engine() {
  echo "Generating retry engine code for DB operations..."

  pushd ../database/vcp > /dev/null

  if ! generate_retryEngineWrapper_checksums; then
    echo "Failed to generate checksums due to missing files."
    exit 1
  fi

  if ! cmp ../checksums/retry-engine-checksums newChecksumsFile.checksum; then

    generate_retry_engine_code_for_DB_operations

    pushd ../../database/vcp
    generate_retryEngineWrapper_checksums

    mv newChecksumsFile.checksum ../../checksums/retry-engine-checksums

  else
    echo "Everything is up to date. Retry engine code is already the latest."
    rm -f newChecksumsFile.checksum
  fi

  popd &> /dev/null
}

generate_retry_engine