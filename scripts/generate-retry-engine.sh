#!/bin/bash


set -e

ARGUMENT="$1"
PACKAGE_NAME="$2"

source generate-util.sh

command_exists() {
  command -v "\$1" >/dev/null 2>&1
}

generate_retry_engine_code_for_DB_operations(){
  echo "starting to generate retry engine code for DB operations"
  pushd ../../cmd/retry-engine-generator > /dev/null
  if [ -n "$ARGUMENT" ]; then
    echo "Generating retry engine code for DB operations with argument: $ARGUMENT"
    echo "Package name: $PACKAGE_NAME"
    go run main.go "$ARGUMENT" "$PACKAGE_NAME"
  else
    go run main.go
  fi
  echo "successfully created retry engine code for DB operations"
}

cleanup() {
  rm -f ./swagger_models.txt
}

# Generate retry engine code for DB operation
generate_retry_engine() {
  echo "Generating retry engine code for DB operations..."

  pushd ../database/$ARGUMENT > /dev/null

  if ! generate_retryEngineWrapper_checksums; then
    echo "Failed to generate checksums due to missing files."
    exit 1
  fi

  if ! cmp ../checksums/retry-engine-checksums.$ARGUMENT newChecksumsFile.checksum.$ARGUMENT; then

    generate_retry_engine_code_for_DB_operations

    pushd ../../database/$ARGUMENT
    generate_retryEngineWrapper_checksums
    pwd
    mv newChecksumsFile.checksum ../../checksums/retry-engine-checksums.$ARGUMENT

  else
    echo "Everything is up to date. Retry engine code is already the latest."
    rm -f newChecksumsFile.checksum
  fi

  popd &> /dev/null
}

generate_retry_engine