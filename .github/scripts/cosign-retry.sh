#!/usr/bin/env bash
# Shared retry helpers for cosign operations against container registries.
#
# GHCR (and occasionally GCP Artifact Registry) are eventually consistent: a
# signature just written by `cosign sign` is sometimes not yet readable by
# `cosign verify` ("Error: no signatures found"), and concurrent layer pushes
# can transiently return "unknown blob". These helpers retry such transient
# failures with exponential backoff so the release pipeline is not flaky.

# retry <max_attempts> <initial_delay_seconds> <command...>
retry() {
  local max_attempts=$1 delay=$2
  shift 2
  local attempt=1
  until "$@"; do
    if (( attempt >= max_attempts )); then
      echo "ERROR: command failed after ${attempt} attempts: $*" >&2
      return 1
    fi
    echo "WARN: attempt ${attempt}/${max_attempts} failed: $*; retrying in ${delay}s..." >&2
    sleep "$delay"
    attempt=$(( attempt + 1 ))
    delay=$(( delay * 2 ))
  done
}

# cosign_sign_verify <private_key> <public_key> <image_ref>
# Signs and then verifies a single reference, retrying transient registry errors.
cosign_sign_verify() {
  local key=$1 pub=$2 ref=$3
  retry 4 5 env COSIGN_PASSWORD="" cosign sign --key "$key" "$ref" --tlog-upload=false
  retry 4 5 cosign verify --key "$pub" "$ref" --insecure-ignore-tlog=true
}
