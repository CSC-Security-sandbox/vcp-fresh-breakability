#!/bin/bash

set -e

source generate-util.sh

FAILED=0

verify_retryEngineWrapper() {
  echo "Verifying checksums for Retry Engine Wrapper ..."

  pushd ../database &> /dev/null

  generate_retryEngineWrapper_checksums

  if ! cmp ../checksums/retry-engine-checksums newChecksumsFile.checksum; then
    echo "Changes detected in the retryEngineWrapper code."
    FAILED=1
  else
    echo "No changes detected. Retry Engine Wrapper code is up-to-date."
  fi

  rm -f newChecksumsFile.checksum

  popd &> /dev/null
}

verify_retryEngineWrapper

if [ "$FAILED" -ne "0" ]; then
  echo "Verification failed: Changes detected in one or more components."
  exit 1
else
  echo "Verification successful: No changes detected in any components."
fi
