# External Cluster API — Test Plan

API surface (core-api): `vcp-core/api.yaml`

| Operation | Path | Method |
|-----------|------|--------|
| Onboard | `/v1/externalClusters/onboard` | POST |
| Get | `/v1/externalClusters/{externalClusterId}` | GET |
| Update | `/v1/externalClusters/{externalClusterId}` | PUT |
| Delete | `/v1/externalClusters/{externalClusterId}` | DELETE |

## Setup

1. Start PostgreSQL and run migrations (`RUN_MIGRATION_ON_START=true` or `go run ./tools/migrate -migrate`).
2. Start **core-api** with `ENV=local`, `CORE_API_PORT=8082`.
3. Use header `x-correlation-id: <unique-id>` on every call.
4. Use `Content-Type: application/json` on POST and PUT.

## Functional tests

### POST onboard — positive

| ID | Case | Request | Expected |
|----|------|---------|----------|
| TC-01 | Single host (full payload) | All required + optional fields + credentials | **201** |
| TC-01b | Omit `protocol`/`port` | `managementIp` required; default **INSECURE_HTTPS** / **443** | **201** |
| TC-01c | `protocol: HTTP` | Default port **80** | **201** |
| TC-16 | Invalid `protocol` (e.g. NFS) | **400** | |
| TC-12 | Multiple hosts | Two hosts in `hosts[]` | **201**, array length 2, distinct IDs |
| TC-19 | Same hostname, different location | Same `hostName`, different `locationId` | Both **201** (uniqueness is `(locationId, hostName)`) |

### POST onboard — negative / validation

| ID | Case | Expected |
|----|------|----------|
| TC-02 | Duplicate `(locationId, hostName)` while active | **409** |
| TC-03 | Missing `locationId` | **400** (ogen validation) |
| TC-04 | Empty `hosts[]` | **400** |
| TC-05 | Empty `password` | **400** |
| TC-05b | Missing `managementIp` | **400** |
| TC-14 | Empty `username` | **400** |
| TC-15 | No JSON Content-Type | **415** |

### GET

| ID | Case | Expected |
|----|------|----------|
| TC-06 | Valid `externalClusterId` after onboard | **200**, fields match onboard; **no password** |
| TC-07 | Unknown UUID | **404** |
| TC-10 | After soft delete | **404** |
| TC-13 | Malformed UUID in path | **400** |

### DELETE

| ID | Case | Expected |
|----|------|----------|
| TC-08 | Delete active host | **200**, `deletedAt` set |
| TC-09 | Delete again | **200** idempotent (`deletedAt` still set) |
| TC-20 | Delete unknown UUID | **404** |
| TC-21 | Invalid path UUID | **400** |
| TC-29 | UUID longer than 36 chars | **400** |

### Batch edge cases

| ID | Case | Expected |
|----|------|----------|
| TC-22 | New host then duplicate in same POST | **409**; document whether first host is persisted |
| TC-23 | Same `hostName` twice in one POST | **409**; document orphan behavior |
| TC-27 | Duplicate first, new host second | **409**; typically no rows created |
| TC-24 | Onboard → delete → onboard same `(location, host)` | **201** with new UUID |

### Security / data

| ID | Case | Expected |
|----|------|----------|
| TC-11 | API response | No `password` / `adminPassword` in JSON |
| TC-DB | Row in `external_cluster_hosts` | `admin_password` encrypted (`V1:...`), not plaintext |

### PUT update (summary)

| Group | IDs | Focus |
|-------|-----|--------|
| Single-field happy path | TC-U01–U12 | description, label, managementIp, protocol/port, credentials |
| Multi-field | TC-U20–U22 | combined updates |
| Optional fields | TC-U30–U32 | omit unchanged optional fields, empty body **400** |
| Protocol/port | TC-U40–U44 | defaults, invalid enum/range |
| Validation | TC-U50–U57 | empty credentials, lengths, Content-Type |
| Path/errors | TC-U60–U64 | 404/400, deleted host |
| Immutability | TC-U70–U73 | hostName, locationId, ontapVersion |
| Security/DB | TC-U80–U84 | encryption, JSONB |
| Sequences | TC-U90–U94 | onboard→put→get, delete |
| Response contract | TC-U100–U103 | read-only fields, `label` omitempty |

## Automated tests (CI / local)

```bash
go test ./vcp-core/handlers/... -run 'ExternalCluster|UpdateExternal' -count=1
go test ./database/vcp/... -run 'ExternalCluster|UpdateExternal' -count=1
go test ./core/orchestrator/factory/gcp/... -run 'ExternalCluster|UpdateExternal' -count=1
python3 doc/testing/external-cluster-api/run_manual_tests.py   # manual matrix vs local :8082
```

## Backlog

- JWT auth paths (401/403) with `ENV` ≠ local
- OCI factory implementation (PUT update returns not implemented today)
- Transactional batch onboard (optional product fix for TC-22/23 orphans)

## Manual test runs

| Date | Report | Result |
|------|--------|--------|
| 2026-06-05 | [manual-test-run-2026-06-05.md](./manual-test-run-2026-06-05.md) | 40/40 pass (paths `/onboard`, `/{externalClusterId}`, local core-api :8082) |
