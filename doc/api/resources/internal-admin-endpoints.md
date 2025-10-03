# Internal / Admin Endpoints Guide (Non-Public)

These endpoints live under `/v1beta/internal/...` and are NOT part of the public GCNV contract. They exist to support orchestration, workflow coordination, replication lifecycle, and operational debugging. Clients outside the control plane / trusted automation must not call them.

> WARNING: Stability and backward compatibility are not guaranteed. Fields may change without notice.

## 1. Volume Replication (Internal)
| Purpose | Method & Path |
|---------|---------------|
| Create replication (low-level) | POST /v1beta/internal/projects/{projectNumber}/locations/{locationId}/volumeReplication |
| Authorize replication source | POST /v1beta/internal/.../volumeReplication/authorize |
| Describe replication | GET /v1beta/internal/.../volumeReplication/{volumeReplicationId} |
| Update replication | PUT /v1beta/internal/.../volumeReplication/{volumeReplicationId} |
| Delete replication | DELETE /v1beta/internal/.../volumeReplication/{volumeReplicationId} (query flags: destinationOnly, sourceOnly, skipPeeringCleanup, cleanupAfterReverse) |
| Stop replication | POST /v1beta/internal/.../volumeReplication/{volumeReplicationId}/stop |
| Resume replication | POST /v1beta/internal/.../volumeReplication/{volumeReplicationId}/resume |
| Reverse replication | POST /v1beta/internal/.../volumeReplication/{volumeReplicationId}/reverse |
| Reverse + resume (attributes) | POST /v1beta/internal/.../volumeReplication/{volumeReplicationId}/updateVolumeReplicationAttributes |
| Mount DP volume after baseline | POST /v1beta/internal/.../volumeReplication/{volumeReplicationId}/mount |
| Release replication row | DELETE /v1beta/internal/.../volumeReplicationRow/{volumeReplicationId}/release |

## 2. Replication Utility
| Purpose | Method & Path |
|---------|---------------|
| Count replications | GET /v1beta/internal/.../volumeReplication/count |
| Batch get replications | POST /v1beta/internal/.../getMultipleReplications |

## 3. Volume (Internal Describe / Update)
| Purpose | Method & Path |
|---------|---------------|
| Get internal volume view | GET /v1beta/internal/.../volumes/{volumeId} |
| Update internal volume | PUT /v1beta/internal/.../volumes/{volumeId} |
| Get volume count | GET /v1beta/internal/.../volumes/count |

## 4. Pool (Internal Describe)
| Purpose | Method & Path |
|---------|---------------|
| Internal pool details | GET /v1beta/internal/.../pool/{poolName} |

## 5. Jobs / Operational
| Purpose | Method & Path |
|---------|---------------|
| Active replication jobs by pool | GET /v1beta/internal/.../ReplicationJobs?poolUUID= |

## 6. SnapMirror Snapshot Cleanup
| Purpose | Method & Path |
|---------|---------------|
| Delete internal snapmirror snapshots for volume | DELETE /v1beta/internal/.../volumes/{volumeId}/snapmirrorSnapshots |

## 7. Cluster Peer Management
| Purpose | Method & Path |
|---------|---------------|
| Accept cluster peer | POST /v1beta/internal/.../clusterPeer |

## 8. Resource Events (Project / State)
Public wrappers exist, but internal variants may perform enriched state transitions—avoid direct use unless operating control-plane automation.

## 9. Authentication & Authorization
Internal endpoints rely on service-to-service auth (e.g., mTLS, workload identity, signed service tokens). Bypassing public validation layers—strict auditing is required.

## 10. Operational Safety Guidelines
- Never invoke delete operations directly during an in-progress public LRO—can desynchronize Job / workflow state.
- Use *Describe* internal endpoints only when public describe lacks required diagnostic fields (e.g., intercluster LIF IPs). Prefer adding non-sensitive fields to public API rather than relying on internal describe.
- Batch getters should be rate-limited to avoid DB hot-spotting.

## 11. Failure Handling
Internal endpoints return the same error envelope; however, error codes may map to unreleased categories (future stabilization). If an internal endpoint returns a 5xx repeatedly, escalate rather than retry aggressively.

## 12. Migration & Deprecation Notes
As functionality stabilizes, some internal endpoints may be:
- Promoted to public (with path /v1beta/...)
- Replaced by workflow queries (Temporal signal/query handlers)
- Consolidated (e.g., replication attribute update + resume flows)

Track deprecations in release notes; do not bake internal paths into customer-facing tooling.

---
End of Internal / Admin Endpoints Guide.

