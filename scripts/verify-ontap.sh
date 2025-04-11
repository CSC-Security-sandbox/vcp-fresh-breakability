#!/bin/bash

set -e

source generate-util.sh

FAILED=0

verify_ontap() {
  echo "Verifying checksums for ONTAP REST API client code and Swagger files..."

  pushd ../clients/ontap-rest &> /dev/null

  generate_ontap_checksums

  if ! cmp ../../checksums/ontap-rest-checksums newChecksumsFile.checksum; then
    echo "Changes detected in the ONTAP REST API client code or Swagger files."
    FAILED=1
  else
    echo "No changes detected. ONTAP REST API client code and Swagger files are up-to-date."
  fi

  rm -f newChecksumsFile.checksum

  popd &> /dev/null
}

verify_ontap_priv() {
  echo "Verifying checksums for private ONTAP REST API client code and Swagger files..."

  pushd ../clients/ontap-rest/priv &> /dev/null

  generate_ontap_priv_checksums

  if ! cmp ../../../checksums/ontap-rest-priv-checksums newChecksumsFile.checksum; then
    echo "Changes detected in the private ONTAP REST API client code or Swagger files."
    FAILED=1
  else
    echo "No changes detected. Private ONTAP REST API client code and Swagger files are up-to-date."
  fi

  rm -f newChecksumsFile.checksum

  popd &> /dev/null
}

verify_ontap
verify_ontap_priv

if [ "$FAILED" -ne "0" ]; then
  echo "Verification failed: Changes detected in one or more components."
  exit 1
else
  echo "Verification successful: No changes detected in any components."
fi
