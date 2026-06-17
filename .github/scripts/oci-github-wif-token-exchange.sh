#!/usr/bin/env bash
# SPDX-License-Identifier: UPL-1.0
# Exchange GitHub Actions OIDC JWT for an OCI UPST and write ~/.oci profile DEFAULT.
# Equivalent to gtrevorrow/oci-token-exchange-action (Oracle JWT token exchange flow).
# Ref: doc/architecture/designs/ghcr-to-oci-object-storage-mirror/workload-identity-federation-github-oci.md §3.7
#
# Env:
#   DEBUG=true — set -x plus safe stderr logs (URL, HTTP status, response body size-truncated).
#   OCI_TOKEN_EXCHANGE_CURL_VERBOSE=true — adds curl -v (stderr may include Authorization: Basic — redact logs).

set -euo pipefail

: "${OIDC_CLIENT_IDENTIFIER:?}"
: "${DOMAIN_BASE_URL:?}"
: "${OCI_TENANCY:?}"
: "${OCI_USER_OCID:=${OCI_USER:-}}"
: "${OCI_USER_OCID:?}"
: "${OCI_REGION:?}"
: "${GITHUB_OIDC_JWT_FILE:?}"

DOMAIN_BASE_URL="${DOMAIN_BASE_URL%/}"

if [[ "${DEBUG:-}" == "true" ]]; then
  echo "[oci-github-wif-token-exchange] DEBUG=true (safe debug logging enabled; bash xtrace disabled to avoid leaking secrets)" >&2
fi

if [[ ! -f "$GITHUB_OIDC_JWT_FILE" ]]; then
  echo "Missing JWT file: $GITHUB_OIDC_JWT_FILE" >&2
  exit 1
fi

WORKDIR="${HOME}/.oci/DEFAULT"
mkdir -p "$WORKDIR"
openssl genrsa -out "$WORKDIR/private_key.pem" 2048
chmod 600 "$WORKDIR/private_key.pem"
openssl rsa -in "$WORKDIR/private_key.pem" -pubout -out "$WORKDIR/public_key.pem"

PUBLIC_KEY_B64=$(openssl rsa -in "$WORKDIR/private_key.pem" -pubout -outform DER 2>/dev/null | base64 -w0)

FP_HEX=$(openssl rsa -in "$WORKDIR/private_key.pem" -pubout -outform DER 2>/dev/null | openssl dgst -md5 -hex | awk '{print $2}')
FINGERPRINT=$(echo "$FP_HEX" | sed 's/../&:/g;s/:$//')

BASIC=$(printf '%s' "$OIDC_CLIENT_IDENTIFIER" | base64 -w0)

TOKEN_URL="${DOMAIN_BASE_URL}/oauth2/v1/token"

# subject_token from file avoids shell escaping issues with JWT punctuation.
# Matches Oracle B7: POST .../oauth2/v1/token (application/x-www-form-urlencoded, Basic client auth).
BODY_FILE="$(mktemp)"
trap 'rm -f "${BODY_FILE}"' EXIT

declare -a CURL_CMD=(
  curl -sS
  -X POST "$TOKEN_URL"
  -H "Content-Type: application/x-www-form-urlencoded"
  -H "Authorization: Basic ${BASIC}"
  --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:token-exchange"
  --data-urlencode "requested_token_type=urn:oci:token-type:oci-upst"
  --data-urlencode "public_key=${PUBLIC_KEY_B64}"
  --data-urlencode "subject_token@${GITHUB_OIDC_JWT_FILE}"
  --data-urlencode "subject_token_type=jwt"
  -o "${BODY_FILE}"
  -w "%{http_code}"
)

# Full curl -v on stderr (TLS, redirects, and Authorization header). Enable only for private debugging.
if [[ "${OCI_TOKEN_EXCHANGE_CURL_VERBOSE:-}" == "true" ]]; then
  CURL_CMD+=(-v)
  echo "[oci-github-wif-token-exchange] OCI_TOKEN_EXCHANGE_CURL_VERBOSE=true: stderr may include Authorization — redact before sharing logs." >&2
fi

set +e
HTTP_CODE="$("${CURL_CMD[@]}")"
CURL_EXIT=$?
set -e
RESP="$(cat "${BODY_FILE}")"

if [[ "${DEBUG:-}" == "true" ]]; then
  echo "[oci-github-wif-token-exchange] POST ${TOKEN_URL}" >&2
  echo "[oci-github-wif-token-exchange] HTTP status: ${HTTP_CODE:-unknown} (curl exit ${CURL_EXIT})" >&2
  if [[ -n "${RESP}" ]] && [[ "${#RESP}" -lt 800 ]]; then
    echo "[oci-github-wif-token-exchange] response body: ${RESP}" >&2
  elif [[ -n "${RESP}" ]]; then
    echo "[oci-github-wif-token-exchange] response body (truncated): ${RESP:0:400}…" >&2
  fi
fi

if [[ "${CURL_EXIT}" -ne 0 ]]; then
  echo "curl token exchange request failed (exit ${CURL_EXIT}). HTTP=${HTTP_CODE:-} body=${RESP}" >&2
  exit 1
fi
if [[ -z "${HTTP_CODE}" ]] || [[ "${HTTP_CODE}" != "200" ]]; then
  echo "[oci-github-wif-token-exchange] HTTP ${HTTP_CODE:-?} (Oracle often returns 4xx JSON for auth/rule errors; checking .token next)" >&2
fi

UPST=$(echo "$RESP" | jq -r '.token // empty')
if [[ -z "$UPST" ]]; then
  echo "Token exchange failed. Response: ${RESP}" >&2
  desc=$(echo "$RESP" | jq -r '.error_description // empty')
  if [[ "$desc" == *'No rules matched'* ]] || [[ "$desc" == *impersonation* ]]; then
    cat >&2 <<'EOF'

OCI Identity Propagation Trust did not match this GitHub OIDC token (impersonation rule).

Fix in Oracle IAM / Identity Domain (not this shell script):
  • With ACTIONS_OCI_DEBUG=true, read “GitHub OIDC claims” in the prior step — copy the exact "sub" value.
  • Edit the Identity Propagation Trust impersonation rule so it matches that sub (repository / branch / ref).
    Examples: repo:ORG/REPO:ref:refs/heads/main  or  repo:ORG/REPO:ref:refs/heads/*
  • Confirm the confidential OAuth client (OIDC_CLIENT_IDENTIFIER) appears in the trust’s oauthClients list.
  • If your trust expects a different JWT audience, set workflow env OCI_GITHUB_OIDC_AUDIENCE and align the trust.

See: doc/architecture/designs/ghcr-to-oci-object-storage-mirror/workload-identity-federation-github-oci.md (§4.5, §5).
EOF
    # Print exact JWT sub so Oracle rules can match without digging through debug logs.
    if command -v python3 >/dev/null 2>&1; then
      jwt_sub="$(
        python3 -c '
import json, base64, sys
path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as f:
        t = f.read().strip()
    parts = t.split(".")
    if len(parts) < 2:
        sys.exit(0)
    seg = parts[1]
    pad = "=" * (-len(seg) % 4)
    payload = json.loads(base64.urlsafe_b64decode(seg + pad))
    sub = payload.get("sub", "")
    if sub:
        print(sub)
except Exception:
    pass
' "$GITHUB_OIDC_JWT_FILE" 2>/dev/null || true
)"
      if [[ -n "${jwt_sub}" ]]; then
        echo "" >&2
        echo "Decoded JWT claim sub for this run (must match an impersonation rule):" >&2
        echo "  ${jwt_sub}" >&2
        echo "Oracle rule example (exact match): sub eq '${jwt_sub}'" >&2
      fi
    fi
  fi
  exit 1
fi

printf '%s' "$UPST" > "$WORKDIR/session"
chmod 600 "$WORKDIR/session"

mkdir -p "${HOME}/.oci"
CONFIG="${HOME}/.oci/config"
{
  echo '[DEFAULT]'
  echo "user=${OCI_USER_OCID}"
  echo "fingerprint=${FINGERPRINT}"
  echo "key_file=${WORKDIR}/private_key.pem"
  echo "tenancy=${OCI_TENANCY}"
  echo "region=${OCI_REGION}"
  echo "security_token_file=${WORKDIR}/session"
} > "$CONFIG"
chmod 600 "$CONFIG"

echo "OCI CLI configured with UPST (profile DEFAULT)."
