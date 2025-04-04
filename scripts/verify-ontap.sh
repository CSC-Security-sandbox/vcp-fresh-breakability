#!/bin/bash

set -e

source ./generate-utils.sh

FAILED=0

pushd ../clients/ontap-rest &> /dev/null

generate_checksums

if ! cmp ../../checksums/ontap-rest-checksums newChecksumsFile.checksum; then
  echo "Changes detected in the client code or Swagger files."
  FAILED=1
else
  echo "No changes detected. Client code and Swagger files are up-to-date."
fi

rm -f newChecksumsFile.checksum

popd &> /dev/null

if [ "$FAILED" -ne "0" ]; then
  exit 1
fi
