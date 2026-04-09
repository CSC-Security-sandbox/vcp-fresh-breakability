# Runbook: CMEK Failure Due to VPC Service Controls (VPC-SC)

This runbook documents the diagnosis and resolution of CMEK policy failures caused by VPC Service Controls blocking NetApp service accounts from accessing customer Cloud KMS keys. It covers both ingress rules (on the KMS key project) and egress rules (on the storage pool project).

---

## Symptoms

1. **Pool creation failed** with `"an internal error has occurred"` followed by `"Invalid KMS configuration state for pool creation: ERROR"`.
2. **Subsequent pool creation attempts** were rejected with `"FAILED_PRECONDITION: Service account lacks the required permissions to access the KMS key"`.
4. **Existing storage pools** using the same CMEK policy continued working without issues.
5. **Data plane error**: `"GCP KMS key is not reachable from ONTAP - Service account lacks permission, retrying again"` on `CheckVsaKmsConfigReachableActivity`.

---

## Network Topology and Communication Flows

### Projects Involved

| Project | Project Number | Role |
|---------|---------------|------|
| Storage Pool Project | `912591718483` | Where GCNV pools/volumes and CMEK policies are created |
| KMS Key Project | `106346362510` | Where Cloud KMS key rings and crypto keys reside |
| NetApp SDE eu-w4 | `977013937889` | NetApp backend for europe-west4 |
| NetApp SDE eu-w12 | `133074621613` | NetApp backend for europe-west12 |
| NetApp SDE eu-w8 | `446937060239` | NetApp backend for europe-west8 |

### Regions

- `europe-west4` (eu-w4)
- `europe-west8` (eu-w8)
- `europe-west12` (eu-w12)

### Communication Diagram

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                         CUSTOMER'S VPC-SC PERIMETER                                    │
│                                                                                        │
│  ┌──────────────────────────────────┐      ┌──────────────────────────────────┐        │
│  │  Storage Pool Project             │      │  KMS Key Project                 │        │
│  │  912591718483                     │      │  106346362510                    │        │
│  │                                   │      │                                  │        │
│  │  - CMEK Policies                  │      │  - KeyRings                      │        │
│  │  - Storage Pools                  │      │  - CryptoKeys                    │        │
│  │  - Volumes                        │      │                                  │        │
│  │                                   │      │  Receives requests FROM:         │        │
│  │  Sends requests TO:               │      │  - NetApp SDE projects           │        │
│  │  - KMS Key Project                │      │  - Storage Pool Project          │        │
│  │    (cloudkms.googleapis.com)      │      │  - Google backend (tenant) prjs  │        │
│  │                                   │      │                                  │        │
│  │  ➤ Needs EGRESS rule              │      │  ➤ Needs INGRESS rule            │        │
│  └──────────────────────────────────┘      └──────────────────────────────────┘        │
│            │                                              ▲                             │
│            │         cloudkms.googleapis.com              │                             │
│            └──────────────────────────────────────────────┘                             │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
                        ▲                          ▲
                        │                          │
         ┌──────────────┴──────┐       ┌───────────┴──────────────────────┐
         │ Storage Pool Project │       │ NetApp SDE Projects              │
         │ 912591718483         │       │ (outside perimeter)              │
         │                     │       │                                  │
         │ CMEK SAs originate  │       │ eu-w4:  977013937889             │
         │ from here (egress)  │       │ eu-w8:  446937060239             │
         │                     │       │ eu-w12: 133074621613             │
         └─────────────────────┘       │                                  │
                                       │ VCP Workers impersonate CMEK SAs │
                                       │ and call Cloud KMS (ingress)     │
                                       └──────────────────────────────────┘

                                       ┌──────────────────────────────────┐
                                       │ NetApp CMEK Project              │
                                       │ netapp-cmek-prod                 │
                                       │                                  │
                                       │ CMEK SAs (per region):           │
                                       │  n-cmek-euwe4-912591718483@...   │
                                       │  n-cmek-euwe8-912591718483@...   │
                                       │  n-cmek-euwe12-912591718483@...  │
                                       └──────────────────────────────────┘
```

### Two Types of Rules Needed

| Direction | On Which Project | What It Allows |
|-----------|-----------------|----------------|
| **Ingress** | KMS Key Project (`106346362510`) | External identities (CMEK SAs) coming from SDE projects and the storage pool project to call `cloudkms.googleapis.com` on the key project |
| **Egress** | Storage Pool Project (`912591718483`) | CMEK SAs originating from the storage pool project to call `cloudkms.googleapis.com` on the KMS key project |

### Why Two Rules?

```
NetApp SDE project (eu-w4: 977013937889)
  │
  │ VCP Worker impersonates CMEK SA
  │ n-cmek-euwe4-912591718483@netapp-cmek-prod.iam.gserviceaccount.com
  │
  │ ──── crosses into perimeter ────►  KMS Key Project (106346362510)
  │                                     cloudkms.googleapis.com
  │                                     CryptoKeys.Get() / CryptoKeys.Encrypt()
  │
  │  This is an INBOUND request to the KMS project
  │  ➤ Needs INGRESS rule on KMS project (106346362510)
  │
  ▼

Storage Pool Project (912591718483)
  │
  │ ONTAP VMs (in tenant project VPC) use same CMEK SA credentials
  │ Request originates from within the perimeter (pool project)
  │ going to the KMS project (also in perimeter, but different project)
  │
  │ ──── outbound from pool project ────►  KMS Key Project (106346362510)
  │                                         cloudkms.googleapis.com
  │
  │  This is an OUTBOUND request from the pool project
  │  ➤ Needs EGRESS rule on Storage Pool Project (912591718483)
```

---

## Two Access Paths That VPC-SC Blocks

### Path 1: Control Plane (VCP → Cloud KMS)

```
VCP Worker (SDE project, e.g. 977013937889)
  │ impersonates
  ▼
CMEK SA (netapp-cmek-prod)                     VPC-SC checks IDENTITY + SOURCE
  e.g. n-cmek-euwe4-912591718483@...           ───────────────────────────────
  │                                             WHO is calling?
  │ CryptoKeys.Get()                            From WHICH project?
  │ CryptoKeys.Encrypt()
  ▼
cloudkms.googleapis.com (106346362510)          ✗ SECURITY_POLICY_VIOLATED
                                                (identity not in ingress rules)
```

**When it happens**: CMEK creation, health check, pool creation (`verifyKmsConfigReachability`), volume creation.

### Path 2: Data Plane (ONTAP → Cloud KMS)

```
ONTAP VSA VM (tenant project, gce-internal-ip)
  │ uses same CMEK SA credentials pushed via        VPC-SC checks NETWORK + IDENTITY
  │ ConfigureKmsForSvmActivity                      ──────────────────────────────────
  │                                                 WHICH VPC network?
  │ ONTAP REST: GET /api/security/gcp-kms/{uuid}    WHO is calling?
  │ → ONTAP internally calls Cloud KMS
  ▼
cloudkms.googleapis.com (106346362510)              ✗ NETWORK_NOT_IN_SAME_SERVICE_PERIMETER
                                                    (tenant VPC not in perimeter)
```

**When it happens**: Pool creation (`CheckVsaKmsConfigReachableActivity`), ongoing ONTAP encrypt/decrypt.

### Pool Create Workflow — Where Each Check Happens

```
Pool Create Workflow (CREATE_POOL)
│
├── 1. verifyKmsConfigReachability()                    ← CONTROL PLANE (Path 1)
│       ├── VCP impersonates CMEK SA
│       ├── CryptoKeys.Get()
│       └── CryptoKeys.Encrypt()
│
├── 2. FindTenancyProject
├── 3. Deploy VSA VMs in tenant project
├── 4. Configure ONTAP cluster
├── 5. Create SVM
│
├── 6. _configureKmsConfigForSvmActivity()              ← DATA PLANE (Path 2)
│       ├── CreateDnsActivity (8.8.8.8 / 8.8.4.4)
│       ├── ConfigureKmsForSvmActivity
│       │    └── ONTAP REST: POST /api/security/gcp-kms
│       │        (pushes SA credentials + key info to ONTAP)
│       └── CheckVsaKmsConfigReachableActivity
│            └── ONTAP REST: GET /api/security/gcp-kms/{uuid}
│                └── ONTAP calls cloudkms.googleapis.com
│                    from tenant project VPC
│
└── Pool state → AVAILABLE (if both paths succeed)
                 ERROR     (if either path fails)
```

---

## VPC-SC Rules to Apply

### CMEK Service Accounts (Per Region)

The CMEK SA naming convention is: `n-cmek-{shortRegion}-{storagePoolProjectNumber}@netapp-cmek-prod.iam.gserviceaccount.com`

| Region | Short Region | CMEK SA |
|--------|-------------|---------|
| europe-west4 | `euwe4` | `n-cmek-euwe4-912591718483@netapp-cmek-prod.iam.gserviceaccount.com` |
| europe-west8 | `euwe8` | `n-cmek-euwe8-912591718483@netapp-cmek-prod.iam.gserviceaccount.com` |
| europe-west12 | `euwe12` | `n-cmek-euwe12-912591718483@netapp-cmek-prod.iam.gserviceaccount.com` |

To find the exact SA emails:

```bash
export CUST_NUM="912591718483"
gcloud iam service-accounts list --project=netapp-cmek-prod \
  --format="table(email)" \
  --filter="email:${CUST_NUM}" | grep "^n-cmek"
```

### Source Projects (NetApp SDE + Storage Pool Project)

| Project | Number | Purpose |
|---------|--------|---------|
| `netapp-eu-w4-sde` | `977013937889` | SDE backend for eu-w4 (VCP workers run here) |
| `netapp-eu-w12-sde` | `133074621613` | SDE backend for eu-w12 |
| `netapp-eu-w8-sde` | `446937060239` | SDE backend for eu-w8 |
| Storage Pool Project | `912591718483` | Customer project where CMEK policies live |

---

### Rule 1: Ingress Rule on KMS Key Project (`106346362510`)

This allows the CMEK SAs (coming from the SDE projects and the storage pool project) to call `cloudkms.googleapis.com` on the KMS key project.

```json
{
  "ingressFrom": {
    "identities": [
      "serviceAccount:n-cmek-euwe4-912591718483@netapp-cmek-prod.iam.gserviceaccount.com",
      "serviceAccount:n-cmek-euwe12-912591718483@netapp-cmek-prod.iam.gserviceaccount.com",
      "serviceAccount:n-cmek-euwe8-912591718483@netapp-cmek-prod.iam.gserviceaccount.com"
    ],
    "sources": [
      {
        "resource": "projects/977013937889"
      },
      {
        "resource": "projects/133074621613"
      },
      {
        "resource": "projects/446937060239"
      },
      {
        "resource": "projects/912591718483"
      }
    ]
  },
  "ingressTo": {
    "operations": [
      {
        "methodSelectors": [
          {
            "method": "*"
          }
        ],
        "serviceName": "cloudkms.googleapis.com"
      }
    ],
    "resources": [
      "projects/106346362510"
    ]
  },
  "title": "CMEK for NetApp Volumes"
}
```

**Breakdown:**

| Field | Values | Why |
|-------|--------|-----|
| `identities` | 3 CMEK SAs (one per region: euwe4, euwe8, euwe12) | These are the impersonated identities that actually call Cloud KMS |
| `sources` | 3 SDE projects + 1 storage pool project | VCP workers run in SDE projects; ONTAP data flows through the storage pool project |
| `operations` | `cloudkms.googleapis.com`, all methods | Covers `CryptoKeys.Get()`, `CryptoKeys.Encrypt()`, `CryptoKeyVersions.useToDecrypt`, etc. |
| `resources` | `projects/106346362510` | The KMS key project where the keys reside — the target of all calls |

### Rule 2: Egress Rule on Storage Pool Project (`912591718483`)

This allows the CMEK SAs originating from the storage pool project to make outbound calls to `cloudkms.googleapis.com` on the KMS key project.

```json
{
  "egressFrom": {
    "identities": [
      "serviceAccount:n-cmek-euwe4-912591718483@netapp-cmek-prod.iam.gserviceaccount.com",
      "serviceAccount:n-cmek-euwe12-912591718483@netapp-cmek-prod.iam.gserviceaccount.com",
      "serviceAccount:n-cmek-euwe8-912591718483@netapp-cmek-prod.iam.gserviceaccount.com"
    ],
    "sources": [
      {
        "resource": "projects/912591718483"
      }
    ]
  },
  "egressTo": {
    "operations": [
      {
        "methodSelectors": [
          {
            "method": "*"
          }
        ],
        "serviceName": "cloudkms.googleapis.com"
      }
    ],
    "resources": [
      "projects/106346362510"
    ]
  },
  "title": "CMEK For netapp volumes"
}
```

**Breakdown:**

| Field | Values | Why |
|-------|--------|-----|
| `identities` | Same 3 CMEK SAs | Same identities, just controlling outbound direction |
| `sources` | `projects/912591718483` only | Egress originates from the storage pool project |
| `operations` | `cloudkms.googleapis.com`, all methods | Same KMS operations |
| `resources` | `projects/106346362510` | The destination — KMS key project |

---

## Step-by-Step: How to Apply the Rules

### Prerequisites

```bash
export KMS_PROJECT_NUM="106346362510"
export POOL_PROJECT_NUM="912591718483"
export PERIMETER_NAME="<perimeter-name>"        # e.g. Netapp_CVO_SE_PROD
export POLICY_ID="<access-policy-id>"            # e.g. 1096472195658
```

### Step 1: Check restricted services

```bash
gcloud access-context-manager perimeters describe $PERIMETER_NAME \
  --policy=$POLICY_ID \
  --format="yaml(status.restrictedServices)"
```

Confirm `cloudkms.googleapis.com` is in the restricted services list.

### Step 2: Backup existing rules

**⚠️ CRITICAL**: `set-ingress-policies` and `set-egress-policies` **replace** all existing rules. Always backup first.

```bash
# Backup ingress rules
gcloud access-context-manager perimeters describe $PERIMETER_NAME \
  --policy=$POLICY_ID \
  --format="yaml(status.ingressPolicies)" > existing-ingress-backup.yaml

# Backup egress rules
gcloud access-context-manager perimeters describe $PERIMETER_NAME \
  --policy=$POLICY_ID \
  --format="yaml(status.egressPolicies)" > existing-egress-backup.yaml

cat existing-ingress-backup.yaml
cat existing-egress-backup.yaml
```

### Step 3: Create the ingress policy YAML

Save to `ingress-policy.yaml` (merge with any existing rules from the backup):

```yaml
# Ingress Rule: Allow NetApp CMEK SAs to access Cloud KMS on the KMS key project
- ingressFrom:
    identities:
      - serviceAccount:n-cmek-euwe4-912591718483@netapp-cmek-prod.iam.gserviceaccount.com
      - serviceAccount:n-cmek-euwe12-912591718483@netapp-cmek-prod.iam.gserviceaccount.com
      - serviceAccount:n-cmek-euwe8-912591718483@netapp-cmek-prod.iam.gserviceaccount.com
    sources:
      - resource: projects/977013937889
      - resource: projects/133074621613
      - resource: projects/446937060239
      - resource: projects/912591718483
  ingressTo:
    operations:
      - serviceName: cloudkms.googleapis.com
        methodSelectors:
          - method: "*"
    resources:
      - projects/106346362510
  title: "CMEK for NetApp Volumes"
```

### Step 4: Create the egress policy YAML

Save to `egress-policy.yaml` (merge with any existing rules from the backup):

```yaml
# Egress Rule: Allow CMEK SAs from the storage pool project to reach Cloud KMS
- egressFrom:
    identities:
      - serviceAccount:n-cmek-euwe4-912591718483@netapp-cmek-prod.iam.gserviceaccount.com
      - serviceAccount:n-cmek-euwe12-912591718483@netapp-cmek-prod.iam.gserviceaccount.com
      - serviceAccount:n-cmek-euwe8-912591718483@netapp-cmek-prod.iam.gserviceaccount.com
    sources:
      - resource: projects/912591718483
  egressTo:
    operations:
      - serviceName: cloudkms.googleapis.com
        methodSelectors:
          - method: "*"
    resources:
      - projects/106346362510
  title: "CMEK For netapp volumes"
```

### Step 5: Apply the ingress rule

```bash
gcloud access-context-manager perimeters update $PERIMETER_NAME \
  --policy=$POLICY_ID \
  --set-ingress-policies=ingress-policy.yaml
```

### Step 6: Apply the egress rule

```bash
gcloud access-context-manager perimeters update $PERIMETER_NAME \
  --policy=$POLICY_ID \
  --set-egress-policies=egress-policy.yaml
```

### Step 7: Verify both rules are applied

```bash
echo "=== Ingress Rules ==="
gcloud access-context-manager perimeters describe $PERIMETER_NAME \
  --policy=$POLICY_ID \
  --format="yaml(status.ingressPolicies)"

echo ""
echo "=== Egress Rules ==="
gcloud access-context-manager perimeters describe $PERIMETER_NAME \
  --policy=$POLICY_ID \
  --format="yaml(status.egressPolicies)"
```

### Step 8: Verify CMEK works

#### Via CCFE (autopush)

```bash
export TOKEN=$(gcloud auth print-access-token)
export PROJECT_ID="<customer-project-id>"         # e.g. tim-inf-prd-cu00000092p70-l0
export REGION="europe-west4"
export KMS_CONFIG_ID="<kms-config-resource-id>"    # e.g. cmek-europe-west4-prod
export CCFE_BASE="https://autopush-netapp.sandbox.googleapis.com/v1beta1"

# 1. Verify CMEK reachability
curl -s -X POST "${CCFE_BASE}/projects/${PROJECT_ID}/locations/${REGION}/kmsConfigs/${KMS_CONFIG_ID}:verify" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" | jq '.'

# 2. Check CMEK state
curl -s "${CCFE_BASE}/projects/${PROJECT_ID}/locations/${REGION}/kmsConfigs/${KMS_CONFIG_ID}" \
  -H "Authorization: Bearer ${TOKEN}" | jq '.state, .stateDetails'
```

For **production**, replace the base URL:
```bash
export CCFE_BASE="https://netapp.googleapis.com/v1"
```

For **staging**:
```bash
export CCFE_BASE="https://staging-netapp.sandbox.googleapis.com/v1beta1"
```

#### Via Proxy

```bash
export PROJECT_NUMBER="912591718483"
export REGION="europe-west4"
export KMS_CONFIG_UUID="<kms-config-uuid>"         # UUID from VCP database
export PROXY_BASE="https://ncv.<region-short>.autopush-tst.internal.npd.gnf.netapp.com/v1beta"

# 1. Check CMEK health
curl -s "${PROXY_BASE}/projects/${PROJECT_NUMBER}/locations/${REGION}/storage/kmsConfig/${KMS_CONFIG_UUID}/check" \
  -H "Authorization: <jwt-token>" | jq '.'

# 2. Check CMEK state
curl -s "${PROXY_BASE}/projects/${PROJECT_NUMBER}/locations/${REGION}/storage/kmsConfig/${KMS_CONFIG_UUID}" \
  -H "Authorization: <jwt-token>" | jq '.kmsState, .kmsStateDetails'
```

#### Expected Results

| Check | Before Fix | After Fix |
|-------|-----------|-----------|
| CMEK verify/check | `isHealthy: false`, `SECURITY_POLICY_VIOLATED` | `isHealthy: true` |
| CMEK state | `ERROR` | `READY` or `IN_USE` |
| Pool creation with CMEK | `Invalid KMS configuration state: ERROR` | Succeeds |
| Encrypt volumes migration | `CMEK Configuration needs to be in either Ready or In_Use state` | Succeeds |

#### Retry pool creation

After CMEK state recovers, retry pool creation to verify both control plane and data plane paths work end-to-end:

**CCFE:**
```bash
curl -X POST "${CCFE_BASE}/projects/${PROJECT_ID}/locations/${REGION}/storagePools?storagePoolId=<pool-name>" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -d '{
    "serviceLevel": "FLEX",
    "capacityGib": "2048",
    "network": "projects/<project>/global/networks/<vpc>",
    "kmsConfig": "projects/<project>/locations/<region>/kmsConfigs/<cmek-id>"
  }'
```

**Proxy:**
```bash
curl -X POST "${PROXY_BASE}/projects/${PROJECT_NUMBER}/locations/${REGION}/storage/pools" \
  -H "Content-Type: application/json" \
  -H "Authorization: <jwt-token>" \
  -d '{
    "resourceId": "<pool-name>",
    "serviceLevel": "FLEX",
    "sizeInBytes": "2199023255552",
    "type": "UNIFIED",
    "network": "<vpc-name>",
    "kmsConfigId": "<kms-config-uuid>"
  }'
```

Poll until pool reaches `READY` state — this confirms both `verifyKmsConfigReachability` (control plane) and `CheckVsaKmsConfigReachableActivity` (data plane) pass.

---

## Discovery Commands

### Find the SN host and tenant projects

```bash
export CUSTOMER_PROJECT="<customer-project-id>"

# Find SN host from VPC peering
gcloud compute networks peerings list --project=$CUSTOMER_PROJECT \
  --format="yaml(name, peerings[].name, peerings[].network)"

# Pick the relevant SN host project
export SN_HOST="<sn-host-from-peering>"
export SN_NUM=$(gcloud projects describe $SN_HOST --format="value(projectNumber)")

# List tenant projects and their regions
for proj in $(gcloud compute shared-vpc associated-projects list $SN_HOST --format="value(RESOURCE_ID)"); do
  num=$(gcloud projects describe "$proj" --format="value(projectNumber)" 2>/dev/null)
  region=$(gcloud compute networks subnets list --project="$proj" --format="value(region)" --limit=1 2>/dev/null)
  echo "$proj | number: $num | region: $region"
done
```

### Find CMEK SAs

```bash
export CUST_NUM="912591718483"
export CMEK_SA_PROJECT="netapp-cmek-prod"

gcloud iam service-accounts list --project=$CMEK_SA_PROJECT \
  --format="table(email)" \
  --filter="email:${CUST_NUM}" | grep "^n-cmek"
```

### Check VPC-SC audit logs for violations

```bash
gcloud logging read 'protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.VpcServiceControlAuditMetadata"' \
  --project=$CUSTOMER_PROJECT \
  --freshness=7d \
  --format=json \
  --limit=100
```

### Test KMS access as the CMEK SA

```bash
gcloud kms keys describe <KEY_NAME> \
  --project=106346362510 \
  --keyring=<KEYRING> \
  --location=<LOCATION> \
  --impersonate-service-account=n-cmek-euwe4-912591718483@netapp-cmek-prod.iam.gserviceaccount.com
```

If this returns `PERMISSION_DENIED` with `vpcServiceControlsUniqueIdentifier`, VPC-SC is still blocking.

### Check if `iamcredentials.googleapis.com` is restricted

```bash
gcloud access-context-manager perimeters describe $PERIMETER_NAME \
  --policy=$POLICY_ID \
  --format="yaml(status.restrictedServices)" | grep iamcredentials
```

If restricted, a separate ingress rule is needed for the impersonation step.

---

## Prevention: Checklist for VPC-SC + CMEK

- [ ] Customer's VPC-SC perimeter identified
- [ ] `cloudkms.googleapis.com` in restricted services? → Ingress + Egress rules needed
- [ ] `iamcredentials.googleapis.com` in restricted services? → Impersonation ingress rule needed
- [ ] SN host project discovered from VPC peerings
- [ ] Tenant project numbers discovered from shared-vpc associated-projects
- [ ] CMEK SA emails identified from CMEK project (per region)
- [ ] Existing ingress and egress rules backed up
- [ ] Ingress rule applied on KMS key project (CMEK SAs + SDE projects + pool project as sources)
- [ ] Egress rule applied on storage pool project (CMEK SAs from pool project to KMS project)
- [ ] CMEK check endpoint returns healthy
- [ ] Pool creation succeeds end-to-end (both control and data plane checks pass)

---

## Related Resources

- **CMEK DNS Resolution Runbook**: `doc/infrastructure/runbooks/cmek_dns_resolution_failure.md`
- **KMS Config Create Failures Runbook**: `doc/infrastructure/runbooks/create_kms_config_failures.md`
- **VPC-SC Simulation Guide**: `doc/infrastructure/runbooks/cmek_vpc_sc_simulation_guide.md`
- **KMS Workflows Documentation**: `doc/workflows/kms/kms-workflows.md`
- **Google Public Doc — VPC-SC Ingress Rules for CMEK**: https://docs.cloud.google.com/netapp/volumes/docs/configure-and-use/cmek/verify-key-access#configure-vpc-service-controls-ingress-rule-for-cmek
- **Google Public Doc — VPC-SC Ingress/Egress Rules**: https://docs.cloud.google.com/vpc-service-controls/docs/ingress-egress-rules

---

## Key Code References

| Component | File | Function |
|-----------|------|----------|
| Impersonation setup | `hyperscaler/google/provider.go` | `GetImpersonatedKmsService()` |
| KMS verification (control plane) | `core/orchestrator/activities/kms_activities/kms_common_activities.go` | `_accessCryptoKeyAndEncryptData()` |
| KMS verification (data plane) | `core/orchestrator/activities/kms_activities/kms_config_ontap_activities.go` | `CheckVsaKmsConfigReachableActivity()` |
| ONTAP KMS reachability | `core/vsa/kms_config.go` | `IsGcpKmsReachable()` |
| KMS config push to ONTAP | `core/orchestrator/activities/kms_activities/kms_config_ontap_activities.go` | `ConfigureKmsForSvmActivity()` |
| Health check update | `core/orchestrator/activities/kms_activities/kms_common_activities.go` | `_updateKmsConfigHealth()` |
| Pool KMS validation | `core/orchestrator/factory/gcp/pool.go` | `_validateCreatePoolParams()` |
| Pool create KMS check | `core/orchestrator/workflows/pool_workflows.go` | `_verifyKmsConfigReachability()` |
| Pool create ONTAP KMS | `core/orchestrator/workflows/pool_workflows.go` | `_configureKmsConfigForSvmActivity()` |
| KMS state conversion | `core/orchestrator/common/kms_config.go` | `ConvertKmsConfigStateV1beta()` |
| IsKmsConfigInUse | `database/vcp/kms_config.go` | `_isKmsConfigInUse()` |
| Permission denied check | `utils/kms_utils.go` | `IsKmsPermissionDenied()` |
