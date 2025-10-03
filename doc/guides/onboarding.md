# Engineer Onboarding Guide

Purpose: Help new engineers obtain required GCNV, configure networking (Private Services Access) and prepare to use the VSA Control Plane.

---

## 1. Obtain Personal Consumer Projects
1. Clone the Jira template ticket:
   - https://jira.ngage.netapp.com/browse/NFSAAS-116530
2. In the cloned ticket replace placeholders (Project Name, Google Project ID) with your details.
3. Email SRE requesting creation of the consumer project.

### 1.1 Email Template (Send One Consolidated Email For Multiple New Joiners)
Subject: Request: Create Consumer Project(s) for VCP Development

Body:
```
Hello SRE Team,
Please create the following consumer project(s) for VCP development:

Engineer(s): <Name(s)>
Requested Project ID(s): <project-id-1>, <project-id-2>
Purpose: VSA Control Plane development & validation.

Thanks.
```

---

## 2. Configure Private Services Access (PSA)
You may also do this later via UI during Storage Pool creation. Below are CLI steps. PSA is required so Google Cloud NetApp Volumes (GCNV) can allocate internal addresses for VSA clusters.

### 2.1 CIDR Range Selection
Allowed: RFC 1918 or privately used public IP ranges (except 6.0.0.0/8, 7.0.0.0/8).
Minimum assignable range: /24 (NetApp Volumes consumes /28 or /27 subranges).

### 2.2 IP Consumption Rules (Summary)
- Storage Pool needs ≥ /28 subrange.
- Standard/Premium/Extreme (non Flex) volumes can share IPs → many pools + volumes fit in one /28.
- Each Flex service level Storage Pool requires a unique IP (all its volumes reuse same IP) → /28 supports up to 12 Flex pools (subtracting 4 unusable subnet IPs).
- Large Capacity Extreme volumes require /27.
- Different regions in same project: additional /28 or /27 per region/pool type.
- Multiple service projects (Shared VPC): distinct /28 or /27 each.
- /24 maximum theoretical combos: up to 16 region–service project combinations (assuming /28 usage).
- Intercluster / migration / FlexCache reserve a separate /27.
- Service may consume additional subranges as existing ones fill.

### 2.3 Enable Service Networking API
```bash
gcloud services enable servicenetworking.googleapis.com \
  --project=PROJECT_ID
```

### 2.4 Reserve Address Range For Peering
```bash
gcloud compute addresses create netapp-addresses-vpc1 \
  --project=PROJECT_ID \
  --global \
  --purpose=VPC_PEERING \
  --prefix-length=24 \
  --network=VPC_NAME \
  --no-user-output-enabled
```
Optional: Explicit base address:
```bash
gcloud compute addresses create netapp-addresses-vpc1 \
  --project=PROJECT_ID \
  --global \
  --purpose=VPC_PEERING \
  --addresses=192.168.0.0 \
  --prefix-length=24 \
  --network=VPC_NAME \
  --no-user-output-enabled
```
Replace:
- `PROJECT_ID` with your consumer project id.
- `VPC_NAME` with the target VPC.
- `192.168.0.0` with desired base if manually chosen.

### 2.5 Peer Networks
```bash
gcloud services vpc-peerings connect \
  --project=PROJECT_ID \
  --service=netapp-tst-autopush-endpoint.appspot.com \  # or netapp-sqa-autopush-endpoint.appspot.com
  --ranges=netapp-addresses-vpc1,ADDITIONAL_IP_RANGES \
  --network=VPC_NAME
```
`ADDITIONAL_IP_RANGES` is a comma‑separated list (or omit if none).

### 2.6 Enable Custom Route Propagation
(GCNV creates peering `sn-netapp-prod` automatically.)
```bash
gcloud compute networks peerings update sn-netapp-prod \
  --project=PROJECT_ID \
  --network=VPC_NAME \
  --import-custom-routes \
  --export-custom-routes
```

### 2.7 Common Error: Organization Policy
Error: `Constraint constraints/compute.restrictVpcPeering violated for project`
Action: Request an exception from org admins (include peering justification + project id).

---

## 3. Google Allowlisting
Add project to allowlisting sheet (internal only):
https://docs.google.com/spreadsheets/d/1xj0-CZjBqTra7OARClUj_arVk-AvcikPqofKD1tDolw/edit?gid=0#gid=0
Provide: Project ID, Owner, Purpose, Date.

---

## 4. Create Your First Storage Pool (CCFE Autopush API)
This step provisions an initial Storage Pool (backed by a VSA cluster) using the Google CCFE (autopush) endpoint. It also triggers creation of the regional tenant project resources so you can subsequently exercise your local control plane.

### 4.1 Prerequisites
- Consumer project ready (allowlisted, PSA configured)
- Network (VPC) name available
- `gcloud auth login` completed and project set: `gcloud config set project <PROJECT_ID>`
- (Optional) CMEK key & KMS configuration already created in the region (if you plan on CMEK encryption)

### 4.2 Environment Variables (Recommended)
```bash
export PROJECT_NUMBER=<your-project-number>
export LOCATION=<region-or-zone>        # e.g. us-east1 or us-east1-b (match API expectation)
export POOL_ID=first-pool               # storage_pool_id value
export NETWORK=<vpc-name>               # VPC network name (not full URI)
# Optional CMEK variables
export KMS_KEY_RESOURCE="projects/<kms-project>/locations/<kms-region>/keyRings/<ring>/cryptoKeys/<key>"
```

### 4.3 Helper Alias
```bash
alias gcurl='curl -sS -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application/json"'
```

### 4.4 Create FLEX (Unified) Storage Pool
Notes:
- `serviceLevel: FLEX` with `unifiedPool: true` enables unified (block + file) capability.
- `customPerformanceEnabled: true` allows independent scaling of throughput & IOPS.
- Adjust `capacityGib`, `totalThroughputMibps`, `totalIops` per minimums (>=1024 GiB, >=64 MiBps, >=1024 IOPS). If `totalIops < totalThroughputMibps*16` the backend normalizes upward.
- Endpoint version: Some environments expose `v1beta1`. If your environment only supports `v1beta`, change the path accordingly.

```bash
gcurl -X POST \
  "https://autopush-netapp.sandbox.googleapis.com/v1beta1/projects/${PROJECT_NUMBER}/locations/${LOCATION}/storagePools?storage_pool_id=${POOL_ID}" \
  --data @- <<'EOF'
{
  "serviceLevel": "FLEX",
  "capacityGib": 1024,
  "description": "first unified flex pool",
  "network": "${NETWORK}",
  "globalAccessAllowed": true,
  "customPerformanceEnabled": true,
  "totalThroughputMibps": 64,
  "totalIops": 1024,
  "unifiedPool": true
}
EOF
```

#### Optional: With CMEK
If CMEK is required, include `"kmsConfig": { "keyFullPath": "${KMS_KEY_RESOURCE}" }` (or the field name your environment expects—some builds use `kmsConfigId` referencing a pre-created KMS config). Example snippet inside the JSON body:
```json
  "kmsConfig": {
    "keyFullPath": "projects/example/locations/us/keyRings/ring/cryptoKeys/key"
  }
```

### 4.5 Interpreting the Response
- On success you may receive either the StoragePool object (synchronous) or an Operation resource (LRO). If an Operation is returned, poll it until `done: true`.
- Poll endpoint (if LRO):
```bash
gcurl "https://autopush-netapp.sandbox.googleapis.com/v1beta1/projects/${PROJECT_NUMBER}/locations/${LOCATION}/operations/<operation-id>"
```

### 4.6 List Storage Pools
```bash
gcurl "https://autopush-netapp.sandbox.googleapis.com/v1beta1/projects/${PROJECT_NUMBER}/locations/${LOCATION}/storagePools"
```

### 4.7 Common Issues
| Symptom | Likely Cause | Action |
|---------|-------------|--------|
| 403 / PERMISSION_DENIED | Missing allowlisting or IAM on project | Verify project in allowlist sheet & roles |
| 400 invalid network | Network name mismatch | Run `gcloud compute networks list` to confirm |
| 409 already exists | Reusing `storage_pool_id` | Choose a new POOL_ID |
| LRO stuck CREATING | Network or quota issue | Check GCP logs & PSA peering state |

---

## 5. Next Steps
1. Follow `getting-started.md` to build and run services locally.
2. Create a Pool → HostGroup → Volume sequence to validate environment.
3. Review `resources/pool.md` deep dive to understand provisioning path.
4. Set up backups / replication using `advanced-usage.md` when basic flows succeed.

---

## 6. Quick Reference (Placeholders)
| Placeholder | Meaning |
|-------------|---------|
| PROJECT_ID | Your consumer (tenant) GCP project id |
| PROJECT_NUMBER | Numeric project id used in resource paths |
| LOCATION | Region or zone (match API spec; some endpoints expect region) |
| VPC_NAME / NETWORK | Target VPC network name hosting volumes |
| ADDITIONAL_IP_RANGES | Extra reserved ranges for future pools/regions |
| POOL_ID | storage_pool_id query parameter value |

---

## 7. Verification Checklist
- [ ] Service Networking API enabled
- [ ] Address range reserved (gcloud list addresses)
- [ ] Peering state = ACTIVE (`gcloud services vpc-peerings list`)
- [ ] Routes imported/exported (check peering details)
- [ ] Project added to allowlisting sheet
- [ ] First pool created (READY state) via CCFE autopush API

If any step fails, capture command output + timestamp and open a Jira with SRE component.

---
End of Onboarding Guide.
