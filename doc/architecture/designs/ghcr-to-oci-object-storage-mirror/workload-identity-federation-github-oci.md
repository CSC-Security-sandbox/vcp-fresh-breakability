# Workload Identity Federation: GitHub Actions → OCI Object Storage

Operational guide for configuring **OCI IAM Identity Domains** so **GitHub Actions** obtains a short-lived **User Principal Session Token (UPST)** via JWT token exchange (no long-lived OCI API keys in CI). Intended for **scripted SCIM/REST automation** and **GitHub Actions YAML**.

**Scope**


| Layer               | Role                                                                |
| ------------------- | ------------------------------------------------------------------- |
| GitHub              | OIDC issuer: `https://token.actions.githubusercontent.com`          |
| OCI Identity Domain | **Identity Propagation Trust** + token endpoint `…/oauth2/v1/token` |
| OCI Tenancy         | **IAM policies** on compartments for Object Storage                 |


---

## Table of contents

1. [Automation quick reference](#1-automation-quick-reference)
2. [Prerequisites](#2-prerequisites)
3. [Part B — SCIM and REST](#3-part-b--scim-and-rest)
4. [Part C — GitHub Actions workflow](#4-part-c--github-actions-workflow)
5. [Troubleshooting](#5-troubleshooting)
6. [References](#6-references)

---

## 1. Automation quick reference

### 1.1 Canonical environment variables

Use the same names in shell scripts and CI templates. Override values for your tenancy.


| Variable                | Required          | Description                                                                                                     |
| ----------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------- |
| `DOMAIN_URL`            | Yes (B1–B5)       | Identity Domain base URL, **no trailing slash** (e.g. `https://idcs-xxxx.identity.oraclecloud.com`)             |
| `IDA_ACCESS_TOKEN`      | Yes (B1–B5)       | Domain **PAT** or admin token; **only** for `Authorization: Bearer` to `${DOMAIN_URL}/admin/v1/...`             |
| `REGION`                | Yes (B6, runtime) | OCI region (e.g. `us-ashburn-1`)                                                                                |
| `COMPARTMENT_OCID`      | Yes (B6)          | **Compartment** OCID: `ocid1.compartment.oc1..` — **not** `ocid1.domain.oc1..`                                  |
| `TENANCY_OCID`          | Yes (GitHub)      | Tenancy OCID (GitHub secret `OCI_TENANCY`)                                                                      |
| `USER_OCID`             | Yes (GitHub)      | **User OCID** for OCI CLI config validation (GitHub secret `OCI_USER_OCID`)                                     |
| `OAUTH_CLIENT_APP_NAME` | B4, B5            | SCIM `displayName` of the confidential OAuth app (e.g. `github-actions-token-exchange`)                         |
| `OAUTH_CLIENT_ID`       | B4, B5, trust     | Client identifier: `clientId` from App, else `name` from SCIM response                                          |
| `OAUTH_CLIENT_SECRET`   | B5, GitHub        | From app **create** response (or rotate/re-fetch per Oracle docs); store as `client_id:client_secret` in GitHub |
| `SERVICE_USER_NAME`     | B2–B4             | e.g. `gh-actions-obj-upload`                                                                                    |
| `GROUP_NAME`            | B3                | e.g. `github-actions-object-upload`                                                                             |
| `DOMAIN_GROUP_PREFIX`   | B6, policy text   | Identity domain name as used in IAM policy (e.g. `controlplane-nb` or `Default`)                                |
| `BUCKET_NAME`           | B6                | Target bucket (policy condition or operational)                                                                 |
| `POLICY_NAME`           | B6                | IAM policy resource name (e.g. `github-actions-object-upload`)                                                  |


### 1.2 Fixed GitHub OIDC values


| Setting        | Value                                                          |
| -------------- | -------------------------------------------------------------- |
| Issuer (`iss`) | `https://token.actions.githubusercontent.com`                  |
| JWKS URL       | `https://token.actions.githubusercontent.com/.well-known/jwks` |


Workflows **must** include `permissions: id-token: write`. Repository/environment restriction uses **impersonation rules** on the trust, not different issuer URLs.

### 1.3 Execution order (automated path)

Run API sections in this **order** (**B5** before **B4** — labels **B4**/**B5** are legacy; execution order matters):


| Step | Section                                       | Action                                                                                              |
| ---- | --------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| 1    | [B1](#31-b1-domain-admin-token)               | Obtain `IDA_ACCESS_TOKEN` (PAT or admin app)                                                        |
| 2    | [B2](#32-b2-create-service-user-scim)         | Create service user                                                                                 |
| 3    | [B3](#33-b3-group-and-membership-scim)        | Create group, add user                                                                              |
| 4    | [B5](#34-b5-token-exchange-oauth-client-scim) | Create + activate **confidential OAuth client** (must exist before trust)                           |
| 5    | [B4](#35-b4-identity-propagation-trust-scim)  | Create **Identity Propagation Trust** (`oauthClients[]` references client from step 4)              |
| 6    | [B6](#36-b6-iam-policy-compartment)           | Create **IAM policy** on compartment (uses API key signing / `oci raw-request`, **not** Bearer PAT) |
| 7    | [Part C](#4-part-c--github-actions-workflow)  | Configure GitHub secrets/variables and workflow                                                     |


### 1.4 Non-negotiable rules (automation)

1. `**impersonationServiceUsers[].value`**: use SCIM user `**id`** (short). A user **OCID** often exceeds length limits → HTTP 400 `stringExceedsMaxLimit`.
2. **CreatePolicy** `compartmentId`: must be `**ocid1.compartment.oc1..`**. Using a **domain** OCID → `401` on control-plane APIs.
3. **B1–B5** use **Bearer** to the **domain** host. **B6** uses **Oracle Signature V1** (user API key) to `identity.{region}.oci.oraclecloud.com` — the domain PAT is **not** a substitute.
4. **GitHub** `sub` in the JWT must **match** an impersonation **rule** (e.g. `repo:ORG/REPO:ref:refs/heads/main`).

---

## 2. Prerequisites

- Tenancy uses **Identity Domains** ([documentation](https://docs.oracle.com/en-us/iaas/Content/Identity/getstarted/identity-domains.htm)).
- Rights to create **Users**, **Groups**, **Apps**, **Identity Propagation Trusts** in the domain, and to create **IAM policies** on the target compartment (or use an API key with that access for B6).
- You know: **tenancy OCID**, **region**, **compartment** for the bucket, and **Identity Domain URL** (`https://idcs-….identity.oraclecloud.com`).

---

## 3. Part B — SCIM and REST

Use for **IaC** and **CI setup jobs**. **Read in order:** B1 → B2 → B3 → **B5** (§3.4) → **B4** (§3.5) → B6 → (B7 reference) → B8. The **B4** / **B5** labels are legacy Oracle doc numbering; **B5 must run before B4** because the trust references `oauthClients[]`.

### 3.1 B1: Domain admin token

**Option A — Personal Access Token**  
[Generate Personal Access Tokens](https://docs.oracle.com/en-us/iaas/Content/Identity/usersettings/generate-personal-access-tokens.htm) with roles that can manage users, groups, apps, and **Identity Propagation Trusts** (often **Identity Domain Administrator** for bootstrap).

**Option B — Separate confidential “admin” app** with domain admin app role; protect like root credentials. Oracle recommends splitting admin app vs token-exchange app ([create applications](https://docs.oracle.com/en-us/iaas/Content/Identity/api-getstarted/json_web_token_exchange.htm#jwt_token_exchange__create-applications)).

```bash
export DOMAIN_URL="https://idcs-2f39c95a6e524478a1cb2a8c09f8185e.identity.oraclecloud.com"
export IDA_ACCESS_TOKEN="$(cat ~/path/to/pat.tok)"
```

**Auth model:** `Authorization: Bearer ${IDA_ACCESS_TOKEN}` is valid only for `**${DOMAIN_URL}/admin/v1/...`**. It is **not** accepted as the sole auth for **tenancy** APIs in B6 (`identity.{region}.oci.oraclecloud.com`).

---

### 3.2 B2: Create service user (SCIM)

```bash
curl -sS -X POST "${DOMAIN_URL}/admin/v1/Users" \
  -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "schemas": [
      "urn:ietf:params:scim:schemas:core:2.0:User",
      "urn:ietf:params:scim:schemas:oracle:idcs:extension:user:User"
    ],
    "urn:ietf:params:scim:schemas:oracle:idcs:extension:user:User": {
      "serviceUser": true
    },
    "userName": "gh-actions-obj-upload"
  }'
```

Persist `**id**` (SCIM) and `**ocid**` from the response.

---

### 3.3 B3: Group and membership (SCIM)

#### Create group

```bash
export GROUP_NAME="github-actions-object-upload"

GROUP_ID="$(
  curl -sS -X POST "${DOMAIN_URL}/admin/v1/Groups" \
    -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{
      \"schemas\": [\"urn:ietf:params:scim:schemas:core:2.0:Group\"],
      \"displayName\": \"${GROUP_NAME}\"
    }" | jq -r '.id'
)"

echo "GROUP_ID=${GROUP_ID}"
```

On duplicate (409), resolve existing group:

```bash
GROUP_ID="$(
  curl -sS -G "${DOMAIN_URL}/admin/v1/Groups" \
    --data-urlencode "filter=displayName eq \"${GROUP_NAME}\"" \
    -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
    -H "Accept: application/json" | jq -r '.Resources[0].id'
)"
echo "GROUP_ID=${GROUP_ID}"
```

#### Add service user to group

```bash
export SERVICE_USER_NAME="gh-actions-obj-upload"

SERVICE_USER_ID="$(
  curl -sS -G "${DOMAIN_URL}/admin/v1/Users" \
    --data-urlencode "filter=userName eq \"${SERVICE_USER_NAME}\"" \
    -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
    -H "Accept: application/json" | jq -r '.Resources[0].id'
)"

if [ -z "${SERVICE_USER_ID}" ] || [ "${SERVICE_USER_ID}" = "null" ]; then
  echo "Error: service user not found: ${SERVICE_USER_NAME}"
  exit 1
fi
echo "SERVICE_USER_ID=${SERVICE_USER_ID}"

curl -sS -X PATCH "${DOMAIN_URL}/admin/v1/Groups/${GROUP_ID}" \
  -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"schemas\": [\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],
    \"Operations\": [
      {
        \"op\": \"add\",
        \"path\": \"members\",
        \"value\": [{\"value\": \"${SERVICE_USER_ID}\"}]
      }
    ]
  }" | jq .
```

#### Verify membership

```bash
curl -sS -X GET "${DOMAIN_URL}/admin/v1/Groups/${GROUP_ID}?attributes=members" \
  -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
  -H "Accept: application/json" | jq '.members'
```

---

### 3.4 B5: Token-exchange OAuth client (SCIM)

Create this **before** [B4 (Identity Propagation Trust)](#35-b4-identity-propagation-trust-scim): the trust’s `oauthClients[]` must reference this app’s client identifier. [Creating and Activating an OAuth Client App](https://docs.oracle.com/en-us/iaas/Content/Identity/api-getstarted/CreateActivateOAuthClientApp.htm).

#### B5.1 Create app

```bash
export OAUTH_CLIENT_APP_NAME="github-actions-token-exchange"

APP_JSON="$(
  curl -sS -X POST "${DOMAIN_URL}/admin/v1/Apps" \
    -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{
      \"schemas\": [\"urn:ietf:params:scim:schemas:oracle:idcs:App\"],
      \"displayName\": \"${OAUTH_CLIENT_APP_NAME}\",
      \"description\": \"GitHub Actions token exchange client for OCI UPST\",
      \"active\": true,
      \"isOAuthClient\": true,
      \"clientType\": \"confidential\",
      \"basedOnTemplate\": {\"value\": \"CustomWebAppTemplateId\"},
      \"allowedGrants\": [\"client_credentials\"]
    }"
)"

echo "${APP_JSON}" | jq .

export APP_RESOURCE_ID="$(echo "${APP_JSON}" | jq -r '.id')"
export OAUTH_CLIENT_SECRET="$(echo "${APP_JSON}" | jq -r '.clientSecret // empty')"
export OAUTH_CLIENT_ID="$(echo "${APP_JSON}" | jq -r '.clientId // .name // empty')"

if [ -z "${APP_RESOURCE_ID}" ] || [ "${APP_RESOURCE_ID}" = "null" ]; then
  echo "Error: App create failed"
  exit 1
fi
echo "APP_RESOURCE_ID=${APP_RESOURCE_ID}"
echo "OAUTH_CLIENT_ID=${OAUTH_CLIENT_ID}"
```

Store `**OAUTH_CLIENT_ID:OAUTH_CLIENT_SECRET**` once in GitHub (secret). GET responses may omit **clientSecret** later.

On **409 duplicate**, list existing app:

```bash
curl -sS -G "${DOMAIN_URL}/admin/v1/Apps" \
  --data-urlencode "filter=displayName eq \"${OAUTH_CLIENT_APP_NAME}\"" \
  -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
  -H "Accept: application/json" | jq '.Resources[0] | {id, name, clientSecret}'
```

#### B5.2 Activate app

```bash
curl -sS -X PUT "${DOMAIN_URL}/admin/v1/AppStatusChanger/${APP_RESOURCE_ID}" \
  -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "schemas": ["urn:ietf:params:scim:schemas:oracle:idcs:AppStatusChanger"],
    "active": true
  }' | jq .
```

#### B5.3 Map to GitHub and trust


| Use                                              | Source                                                                                                                   |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| Client identifier (Basic auth user for exchange) | `OAUTH_CLIENT_ID` (`name` or `clientId`)                                                                                 |
| Secret                                           | `clientSecret` from create response; if missing, use Oracle’s app rotation / admin procedures — do not log full App JSON |
| `oauthClients[]` in trust                        | Same identifier as `OAUTH_CLIENT_ID`                                                                                     |


---

### 3.5 B4: Identity Propagation Trust (SCIM)

**Create:** `oci identity-domains identity-propagation-trust create --from-json …` (same resource as `POST ${DOMAIN_URL}/admin/v1/IdentityPropagationTrusts`).

**Prerequisite:** The confidential OAuth client from **[B5](#34-b5-token-exchange-oauth-client-scim)** must **already exist**. **B4** only registers the trust and references `oauthClients[]`.

Oracle reference: [Create Identity Propagation Trust](https://docs.oracle.com/en-us/iaas/Content/Identity/api-getstarted/json_web_token_exchange.htm#jwt_token_exchange__create-identity-propagation-trust).

```bash
export OAUTH_CLIENT_APP_NAME="github-actions-token-exchange"

OAUTH_CLIENT_ID="$(
  curl -sS -G "${DOMAIN_URL}/admin/v1/Apps" \
    --data-urlencode "filter=displayName eq \"${OAUTH_CLIENT_APP_NAME}\"" \
    -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
    -H "Accept: application/json" \
    | jq -r '.Resources[0] | (.clientId // .name // empty)'
)"

if [ -z "${OAUTH_CLIENT_ID}" ] || [ "${OAUTH_CLIENT_ID}" = "null" ]; then
  echo "Error: OAuth app not found: displayName=${OAUTH_CLIENT_APP_NAME}"
  exit 1
fi
echo "OAUTH_CLIENT_ID=${OAUTH_CLIENT_ID}"

export SERVICE_USER_NAME="gh-actions-obj-upload"
SERVICE_USER_JSON="$(
  curl -sS -G "${DOMAIN_URL}/admin/v1/Users" \
    --data-urlencode "filter=userName eq \"${SERVICE_USER_NAME}\"" \
    -H "Authorization: Bearer ${IDA_ACCESS_TOKEN}" \
    -H "Accept: application/json" | jq -c '.Resources[0]'
)"
SERVICE_USER_ID="$(echo "${SERVICE_USER_JSON}" | jq -r '.id')"
SERVICE_USER_OCID="$(echo "${SERVICE_USER_JSON}" | jq -r '.ocid')"

if [ -z "${SERVICE_USER_ID}" ] || [ "${SERVICE_USER_ID}" = "null" ]; then
  echo "Error: service user not found: ${SERVICE_USER_NAME}"
  exit 1
fi
if [ "${#SERVICE_USER_ID}" -gt 40 ]; then
  echo "Error: SERVICE_USER_ID length ${#SERVICE_USER_ID} exceeds limit for impersonationServiceUsers.value"
  exit 1
fi
echo "SERVICE_USER_ID=${SERVICE_USER_ID}"
echo "SERVICE_USER_OCID=${SERVICE_USER_OCID:-<none>}"

IMPERSONATION_RULE="${IMPERSONATION_RULE:-sub eq 'repo:VCP-VSA-control-Plane/vsa-control-plane:ref:refs/heads/VSCP-5947'}"
TRUST_JSON="$(mktemp)"
trap 'rm -f "${TRUST_JSON}"' EXIT

jq -n \
  --arg oauth "${OAUTH_CLIENT_ID}" \
  --arg suid "${SERVICE_USER_ID}" \
  --arg rule "${IMPERSONATION_RULE}" \
  '{
    schemas: ["urn:ietf:params:scim:schemas:oracle:idcs:IdentityPropagationTrust"],
    active: true,
    allowImpersonation: true,
    issuer: "https://token.actions.githubusercontent.com",
    name: "github-actions-trust",
    oauthClients: [$oauth],
    publicKeyEndpoint: "https://token.actions.githubusercontent.com/.well-known/jwks",
    subjectClaimName: "sub",
    subjectMappingAttribute: "userName",
    subjectType: "User",
    type: "JWT",
    impersonationServiceUsers: [
      { rule: $rule, value: $suid }
    ]
  }' > "${TRUST_JSON}"

if ! CREATE_OUT="$(
  oci identity-domains identity-propagation-trust create \
    --endpoint "${DOMAIN_URL}" \
    --authorization "Bearer ${IDA_ACCESS_TOKEN}" \
    --from-json "file://${TRUST_JSON}"
)"; then
  echo "oci identity-domains identity-propagation-trust create failed." >&2
  exit 1
fi
echo "${CREATE_OUT}" | jq .
```

**Why `get` shows `impersonation-service-users`: null**

A default `**oci identity-domains identity-propagation-trust get`** (no `**--attributes`**) often returns `**impersonation-service-users`: null** — that usually means the attribute was **not included in this response projection**, not that IAM cleared your rules. SCIM treats some fields as **return-on-request** only.

Confirm impersonation rules actually persisted:

```bash
TRUST_ID="$(echo "${CREATE_OUT}" | jq -r '.data.id // empty')"
oci identity-domains identity-propagation-trust get \
  --endpoint "${DOMAIN_URL}" \
  --identity-propagation-trust-id "${TRUST_ID}" \
  --authorization "Bearer ${IDA_ACCESS_TOKEN}" \
  --attributes "impersonationServiceUsers" | jq '.data."impersonation-service-users"'
```

#### Patch impersonation rules (recommended workflow)

If token exchange fails with `**401**` and `**No rules matched from given token to find impersonation user**`, patch `**impersonationServiceUsers**` so at least one rule matches the workflow’s JWT `**sub**` **exactly**.

1) Create `patch.json` (note the single-quotes inside the `rule` string):

```json
{
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
  "Operations": [
    {
      "op": "replace",
      "path": "impersonationServiceUsers",
      "value": [
        {
          "rule": "sub eq 'repo:VCP-VSA-control-Plane/vsa-control-plane:ref:refs/heads/main'",
          "value": "c6e4c965a6ea48d793f270ca1a6b7209"
        },
        {
          "rule": "sub eq 'repo:VCP-VSA-control-Plane/vsa-control-plane:ref:refs/heads/VSCP-5947'",
          "value": "c6e4c965a6ea48d793f270ca1a6b7209"
        }
      ]
    }
  ]
}
```

2) Apply the patch:

```bash
ENDPOINT="https://idcs-2f39c95a6e524478a1cb2a8c09f8185e.identity.oraclecloud.com"
TRUST_ID="487f60c31edd4e6e92c7221aea99ffdc"

oci identity-domains identity-propagation-trust patch \
  --endpoint "$ENDPOINT" \
  --identity-propagation-trust-id "$TRUST_ID" \
  --from-json file://patch.json
```

3) Verify the stored rules (use `--attributes` so the field is included in the projection):

```bash
oci identity-domains identity-propagation-trust get \
  --endpoint "$ENDPOINT" \
  --identity-propagation-trust-id "$TRUST_ID" \
  --attributes impersonationServiceUsers
```

Notes:
- This uses `**op=replace**`, which **overwrites the full list**. Include all entries you want to keep.
- `**sub eq *`** is not a valid “match all” for these rules; use exact `sub` values (or confirm supported pattern operators in your Identity Domains release).

If **that** output is still empty or null, the create payload did not apply `**impersonationServiceUsers`** (check `**CREATE_OUT`** / CLI exit code).

**Rules**

- **value**: SCIM user `id` only — **not** OCID (length / validation).
- **rule**: Adjust `repo:ORG/REPO:…` (override via `**IMPERSONATION_RULE`**) to match GitHub JWT `**sub`**. The `**jq**` output file is passed to `**oci … create --from-json**`.

---

### 3.6 B6: IAM policy (compartment)

Grant the domain group access to Object Storage. Policies attach to the **compartment OCID** that contains the bucket.

**Auth:** [Oracle Signature V1](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/signingrequests.htm) with an **IAM user API key** (`~/.oci/config`) — **not** `Bearer ${IDA_ACCESS_TOKEN}`. Prefer `**oci raw-request`** to avoid hand-signing.


| API family                     | Host                                                                                |
| ------------------------------ | ----------------------------------------------------------------------------------- |
| Domain SCIM (B1–B5)            | `${DOMAIN_URL}`                                                                     |
| Identity / Object Storage (B6) | `identity.${REGION}.oci.oraclecloud.com`, `objectstorage.${REGION}.oraclecloud.com` |


APIs: [CreatePolicy](https://docs.oracle.com/en-us/iaas/api/#/en/identity/20160918/Identity/CreatePolicy), [GetBucket](https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/20160918/Object/GetBucket).

#### Example policy statements (reference)

**Compartment-wide** (domain name `controlplane-nb`, compartment name matches):

```text
Allow group 'controlplane-nb'/'github-actions-object-upload' to manage buckets in compartment controlplane-nb
Allow group 'controlplane-nb'/'github-actions-object-upload' to manage objects in compartment controlplane-nb
```

**Narrow (single bucket, OCID in statement):** use `where target.bucket.name = '<BUCKET_NAME>'` on `manage objects`.

#### List / describe policies (OCI CLI, API key profile)

```bash
export COMPARTMENT_OCID="ocid1.compartment.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

oci iam policy list \
  --compartment-id "${COMPARTMENT_OCID}" \
  --all \
  --query 'data[*].{name:name,id:id,"lifecycle-state":"lifecycle-state"}' \
  --output table

export POLICY_ID="ocid1.policy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
oci iam policy get --policy-id "${POLICY_ID}" | jq .

POLICY_ID="$(
  oci iam policy list \
    --compartment-id "${COMPARTMENT_OCID}" \
    --all \
    --query 'data[?name==`github-actions-object-upload`].id | [0]' \
    --raw-output
)"
oci iam policy get --policy-id "${POLICY_ID}" | jq '.data | {name, statements, "compartment-id":"compartment-id", "lifecycle-state":"lifecycle-state"}'
```

#### Variables and policy JSON

```bash
export REGION="us-ashburn-1"
export COMPARTMENT_OCID="ocid1.compartment.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
export BUCKET_NAME="demo"
export POLICY_NAME="github-actions-object-upload"
export DOMAIN_GROUP_PREFIX="controlplane-nb"
```

**Bucket-scoped** single statement:

```bash
POLICY_STMT="Allow group '${DOMAIN_GROUP_PREFIX}'/'github-actions-object-upload' to manage objects in compartment id ${COMPARTMENT_OCID} where target.bucket.name = '${BUCKET_NAME}'"

jq -nc \
  --arg cid "${COMPARTMENT_OCID}" \
  --arg pn "${POLICY_NAME}" \
  --arg stmt "${POLICY_STMT}" \
  '{
    compartmentId: $cid,
    description: "Allow GitHub WIF service user group to upload objects",
    name: $pn,
    statements: [$stmt]
  }' > /tmp/create-policy-body.json
```

**Compartment-wide** (two statements, compartment **name** in allow clause):

```bash
export GITHUB_GROUP="github-actions-object-upload"
export COMPARTMENT_NAME="${DOMAIN_GROUP_PREFIX}"

POLICY_STMTS_JSON="$(jq -nc \
  --arg p "${DOMAIN_GROUP_PREFIX}" \
  --arg g "${GITHUB_GROUP}" \
  --arg c "${COMPARTMENT_NAME}" \
  --arg q "'" \
  '[
    ("Allow group " + $q + $p + $q + "/" + $q + $g + $q + " to manage buckets in compartment " + $c)
  ]')"

jq -nc \
  --arg cid "${COMPARTMENT_OCID}" \
  --arg pn "${POLICY_NAME}" \
  --argjson stmts "${POLICY_STMTS_JSON}" \
  '{
    compartmentId: $cid,
    description: "Allow GitHub WIF service user group to upload objects",
    name: $pn,
    statements: $stmts
  }' > /tmp/create-policy-body.json
```

#### Create policy

```bash
oci raw-request \
  --http-method POST \
  --target-uri "https://identity.${REGION}.oci.oraclecloud.com/20160918/policies" \
  --request-body file:///tmp/create-policy-body.json
```

Alternative: signed `curl` to the same URL with body `@/tmp/create-policy-body.json`.


| Issue                       | Action                                                                           |
| --------------------------- | -------------------------------------------------------------------------------- |
| `401` with signature        | `compartmentId` must be `ocid1.compartment.oc1..`, not domain OCID               |
| `401` without valid signing | Fix API key, fingerprint, user OCID, or use `oci raw-request`                    |
| `403`                       | Caller cannot create policy on compartment, or statement/domain group name wrong |


---

### 3.7 B7: Token exchange (reference)

`POST https://<domainURL>/oauth2/v1/token` with:

- `grant_type=urn:ietf:params:oauth:grant-type:token-exchange`
- `requested_token_type=urn:oci:token-type:oci-upst`
- `subject_token` = GitHub OIDC JWT
- `subject_token_type=jwt`
- Client authentication for the confidential app

Full parameter list: [Step 7: Get the OCI UPST](https://docs.oracle.com/en-us/iaas/Content/Identity/api-getstarted/json_web_token_exchange.htm#jwt_token_exchange__get-oci-upst).

---

### 3.8 B8: Upload after UPST

**CLI**

```bash
oci --auth security_token os object put \
  --namespace-name "$(oci os ns get --query data --raw-output)" \
  --bucket-name "demo" \
  --name "CODING_GUIDELINES.md" \
  --file "./CODING_GUIDELINES.md"
```

**REST:** [PutObject](https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/20160918/Object/PutObject). Prefer CLI or an SDK that accepts session tokens unless you already implement UPST signing.

---

## 4. Part C — GitHub Actions workflow

Apply after OCI configuration (**Part B**): service user, group, IAM policy, OAuth client, trust + impersonation.

### 4.1 Prerequisites checklist


| Requirement               | Source                                        |
| ------------------------- | --------------------------------------------- |
| Service user + group      | [B2–B3](#33-b3-group-and-membership-scim)     |
| IAM policy                | [B6](#36-b6-iam-policy-compartment)           |
| Confidential app on trust | [B5](#34-b5-token-exchange-oauth-client-scim) |
| Trust + `sub` rule        | [B4](#35-b4-identity-propagation-trust-scim)  |


Decode failing JWT `**sub`** and compare to the impersonation rule ([§5](#5-troubleshooting)).

### 4.2 Secrets and variables

Aligned with this repo's workflow `.github/workflows/oci-vcp-vsa-image-copy.yaml` and in-repo token exchange script `.github/scripts/oci-github-wif-token-exchange.sh`:


| Purpose            | GitHub                              | Value                                                           |
| ------------------ | ----------------------------------- | --------------------------------------------------------------- |
| OAuth client       | Secret `OIDC_CLIENT_IDENTIFIER`     | `client_id:client_secret`                                       |
| Tenancy            | Secret `OCI_TENANCY`                | Tenancy OCID                                                    |
| User OCID          | Secret `OCI_USER_OCID`              | OCI IAM user OCID for session profile                           |
| GHCR pull token    | Secret `GHVSA_PAT`                  | PAT with `read:packages`                                        |
| Source profile user| Secret `OCI_USER_OCID_OPENLABCP`    | Source tenancy user OCID for API key profile                    |
| Source tenancy     | Secret `OCI_TENANCY_OCID_OPENLABCP` | Source tenancy OCID                                              |
| Source fingerprint | Secret `OCI_FINGERPRINT_OPENLABCP`  | API key fingerprint                                              |
| Source key         | Secret `OCI_API_PRIVATE_KEY_OPENLABCP` | PEM private key                                              |
| Source key passphrase | Secret `OCI_API_KEY_PASSPHRASE_OPENLABCP` | Optional                                                   |
| Region             | Workflow env `OCI_REGION`           | e.g. `us-ashburn-1`                                             |
| Domain             | Workflow env `DOMAIN_BASE_URL`      | `https://idcs-….identity.oraclecloud.com`                        |
| Bucket             | Workflow env `OCI_BUCKET_NAME`      | e.g. `oci-vcp-vsa-artifacts`                                    |
| Bucket compartment | Workflow env `OCI_COMPARTMENT_OCID` | Target compartment OCID                                          |


Optional variable: `OCI_GITHUB_OIDC_AUDIENCE` (defaults to `https://cloud.oracle.com` when empty).

### 4.3 Permissions

```yaml
permissions:
  id-token: write
  contents: read
```

Fork PRs and reusable workflows: see [GitHub OIDC](https://docs.github.com/en/actions/how-tos/security-for-github-actions/security-hardening-your-deployments/using-openid-connect-in-your-workflow); caller must grant `id-token: write` and pass secrets if needed.

### 4.4 Example workflow pattern (in-repo script flow)

```yaml
name: oci-object-storage-smoke

on:
  workflow_dispatch:

permissions:
  id-token: write
  contents: read

jobs:
  upload:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install OCI CLI
        run: |
          python3 -m venv "$HOME/.local/oci-cli-venv"
          "$HOME/.local/oci-cli-venv/bin/pip" install "oci-cli==3.68.1"
          mkdir -p "$HOME/.local/bin"
          ln -sf "$HOME/.local/oci-cli-venv/bin/oci" "$HOME/.local/bin/oci"
          echo "$HOME/.local/bin" >> "$GITHUB_PATH"

      - name: Get GitHub OIDC token
        uses: actions/github-script@v8
        with:
          script: |
            const fs = require('fs');
            const token = await core.getIDToken('https://cloud.oracle.com');
            fs.writeFileSync(`${process.env.RUNNER_TEMP}/github_oidc.jwt`, token, 'utf8');

      - name: Exchange GitHub OIDC token for OCI UPST
        env:
          OIDC_CLIENT_IDENTIFIER: ${{ secrets.OIDC_CLIENT_IDENTIFIER }}
          DOMAIN_BASE_URL: ${{ vars.DOMAIN_BASE_URL }}
          OCI_TENANCY: ${{ secrets.OCI_TENANCY }}
          OCI_USER_OCID: ${{ secrets.OCI_USER_OCID }}
          OCI_REGION: us-ashburn-1
          GITHUB_OIDC_JWT_FILE: ${{ runner.temp }}/github_oidc.jwt
        run: bash .github/scripts/oci-github-wif-token-exchange.sh

      - name: Verify session
        env:
          PYTHONWARNINGS: ignore::FutureWarning
        run: oci os ns get --auth security_token

      - name: Upload object (example)
        env:
          BUCKET_NAME: ${{ vars.OCI_BUCKET_NAME }}
          OBJECT_NAME: smoke/github-actions-${{ github.run_id }}.txt
        run: |
          NS="$(oci os ns get --auth security_token --query data --raw-output)"
          echo "test ${{ github.sha }}" > artifact.txt
          oci os object put --auth security_token \
            --namespace-name "$NS" \
            --bucket-name "$BUCKET_NAME" \
            --name "$OBJECT_NAME" \
            --file artifact.txt
```

Manual exchange: obtain GitHub OIDC token (`core.getIDToken()` in `@actions/github-script`) and POST to `/oauth2/v1/token` per [B7](#37-b7-token-exchange-reference), or call `.github/scripts/oci-github-wif-token-exchange.sh` directly as shown above.

### 4.5 `sub` claim vs impersonation rules


| Context                        | Example `sub`                          |
| ------------------------------ | -------------------------------------- |
| Branch                         | `repo:ORG/REPO:ref:refs/heads/main`    |
| All branches (pattern in rule) | `repo:ORG/REPO:ref:refs/heads/*`       |
| Environment                    | `repo:ORG/REPO:environment:production` |


**Exact branch example (feature branch):** `repo:ORG/REPO:ref:refs/heads/VSCP-5947` — your workflow log’s `**sub`** must match a rule **character-for-character**.

**Wildcard caveat:** Rules often use `**sub eq 'repo:…:ref:refs/heads/*'`**. In many Identity Domains, `**eq**` is **exact match** and `*`** is not treated as a glob** unless Oracle documents otherwise for your release. If exchange fails with **“No rules matched”** while a `*` rule exists, **PATCH** the trust and add a **second** `impersonationServiceUsers` entry whose `**rule`** is `**sub eq '<paste exact sub from Actions>'**` (see decoded `**sub**` in `oci-github-wif-token-exchange.sh` logs), or confirm supported pattern operators in current Oracle docs.

---

## 5. Troubleshooting

### Domain / SCIM / trust


| Symptom                                | Check                                                                        |
| -------------------------------------- | ---------------------------------------------------------------------------- |
| `401` / invalid client on exchange     | Client id/secret; app in trust `oauthClients`                                |
| `403` on SCIM                          | PAT scopes / roles                                                           |
| `400` `stringExceedsMaxLimit` on trust | Use SCIM user `**id**`, not OCID, for `impersonationServiceUsers.value`      |
| Invalid JWT / signature                | Issuer exactly `https://token.actions.githubusercontent.com`; JWKS reachable |


### GitHub Actions


| Symptom                                           | Check                                                                                                                                                                                                                    |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Cannot request OIDC token                         | `permissions: id-token: write`; fork / context restrictions                                                                                                                                                              |
| Exchange `401` / `invalid_client`                 | Secret is exactly `client_id:client_secret`; no whitespace                                                                                                                                                               |
| Exchange `403`                                    | Trust issuer/JWKS; [OIDC hardening](https://docs.github.com/en/actions/how-tos/security-for-github-actions/security-hardening-your-deployments/using-openid-connect-in-your-workflow)                                    |
| Exchange `400` impersonation / “No rules matched” | Decode `**sub**` (workflow debug or token-exchange script). `**eq**` rules may not glob — feature branches need `**sub eq '…refs/heads/branchname'**` or multiple entries ([§4.5](#45-sub-claim-vs-impersonation-rules)) |
| Object Storage `403` after UPST                   | IAM policy domain group name, compartment, bucket condition                                                                                                                                                              |
| Wrong actor in audit                              | Rule vs actual `sub` — decode JWT in a controlled environment                                                                                                                                                            |


Use `ACTIONS_OCI_DEBUG=true` only in trusted contexts; the in-repo exchange script prints additional decoded token context and exchange diagnostics when debug is enabled.

### IAM policy (B6)


| Symptom                            | Check                                                |
| ---------------------------------- | ---------------------------------------------------- |
| `401` on CreatePolicy with signing | `compartmentId` is compartment OCID, not domain OCID |
| `403`                              | IAM permissions to create policy; statement syntax   |


---

## 6. References

- [Signing REST Requests](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/signingrequests.htm)
- [Identity CreatePolicy](https://docs.oracle.com/en-us/iaas/api/#/en/identity/20160918/Identity/CreatePolicy)
- [Object Storage GetBucket](https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/20160918/Object/GetBucket)
- [Object Storage PutObject](https://docs.oracle.com/en-us/iaas/api/#/en/objectstorage/20160918/Object/PutObject)
- [Token Exchange (JWT → UPST)](https://docs.oracle.com/en-us/iaas/Content/Identity/api-getstarted/json_web_token_exchange.htm)
- [Adding a Confidential Application](https://docs.oracle.com/en-us/iaas/Content/Identity/applications/add-confidential-application.htm)
- [Generate Personal Access Tokens](https://docs.oracle.com/en-us/iaas/Content/Identity/usersettings/generate-personal-access-tokens.htm)
- [GitHub: OpenID Connect in workflows](https://docs.github.com/en/actions/how-tos/security-for-github-actions/security-hardening-your-deployments/using-openid-connect-in-your-workflow)

---

*Operational guidance. Policy syntax and API behavior change; verify against current Oracle documentation for your tenancy.*