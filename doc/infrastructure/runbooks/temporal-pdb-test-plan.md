# Temporal PDB Fix Test Plan

**Related Incident**: VSCP-4784 (GKE Node Pool Upgrade — Multi-Method Failures)  
**Date Created**: 2026-03-25  

## 1. Overview

**Objective**: Validate that properly configured Pod Disruption Budgets (PDBs) for Temporal pods prevent "service failures" during GKE node drains by ensuring controlled pod eviction.

**Target Environment**: Autopush TST

**Root Cause (from VSCP-4784 RCA)**: Temporal uses a ring-based shard ownership system. When history/matching pods are evicted during GKE node drains, the remaining pods must re-acquire shards (30-60s). During rebalancing, calls to affected shards fail with "service failures", causing `context deadline exceeded` errors in google-proxy.

---

## 2. The Fix — PDB Configuration

The current `kubernetes/temporal/values.yaml` has empty PDB configurations:

```yaml
frontend:
  podDisruptionBudget: {}   # EMPTY — no protection
history:
  podDisruptionBudget: {}   # EMPTY
matching:
  podDisruptionBudget: {}   # EMPTY
worker:
  podDisruptionBudget: {}   # EMPTY
```

### 2.1 Recommended PDB Values

```yaml
server:
  # IMPORTANT: replicaCount must be >1 for PDB to be created (see template condition)
  replicaCount: 3  # Minimum 3 for meaningful PDB testing

  frontend:
    replicaCount: 3
    podDisruptionBudget:
      minAvailable: 2  # Allow max 1 pod unavailable at a time

  history:
    replicaCount: 3
    podDisruptionBudget:
      minAvailable: 2  # CRITICAL — history owns workflow state shards

  matching:
    replicaCount: 3
    podDisruptionBudget:
      minAvailable: 2  # matching routes tasks to workers

  worker:
    replicaCount: 3
    podDisruptionBudget:
      minAvailable: 2  # worker handles system workflows
```

**Why `minAvailable: 2` with `replicaCount: 3`?**
- GKE can only evict 1 pod at a time
- The remaining 2 pods can absorb shards before the next eviction
- This gives Temporal 30-60s to rebalance shards between evictions

---

## 3. Choose the autopush env
- We will be using the au-se-tst autopush env for our initial testing.

## 4. Deploy the PDB Fix

### 4.1 Create Test Values File
- Raise a PR in the au-se1-tst deployment repo with the required overrides. Get it reviewed & merged.

### 4.2 Verify PDBs Created

```bash
# Wait for rollout
kubectl rollout status deployment -n temporal -l app.kubernetes.io/name=temporal --timeout=5m

# Check PDBs exist
kubectl get pdb -n temporal

# Expected output:
# NAME                              MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS   AGE
# temporal-frontend-pdb             2               N/A               1                     1m
# temporal-history-pdb              2               N/A               1                     1m
# temporal-matching-pdb             2               N/A               1                     1m
# temporal-worker-pdb               2               N/A               1                     1m

# Verify PDB details
kubectl describe pdb -n temporal
```

---

## 5. Test Procedure — Simulate Node Eviction

### 5.1 Preparation — Set Up Monitoring

Open **4 terminal windows** for monitoring:

**Terminal 1 — Watch Temporal pods:**
```bash
watch -n 1 'kubectl get pods -n temporal -l app.kubernetes.io/name=temporal -o wide'
```

**Terminal 2 — Watch PDB status:**
```bash
watch -n 2 'kubectl get pdb -n temporal'
```

**Terminal 3 — Tail Temporal logs for "service failures":**
```bash
kubectl logs -n temporal -l app.kubernetes.io/component=history -f --since=1m 2>&1 | grep -i "service failure\|fail to process"
```

**Terminal 4 — Run test requests (see section 5.3)**

### 5.2 Method A: Manual Pod Delete (Quick Test)

This simulates what happens when GKE evicts a pod. Use this for quick validation.

```bash
# Get a history pod name (history is most critical for shard rebalancing)
HISTORY_POD=$(kubectl get pods -n temporal -l app.kubernetes.io/component=history -o jsonpath='{.items[0].metadata.name}')
echo "Will delete: $HISTORY_POD"

# Delete the pod (simulates eviction)
kubectl delete pod -n temporal $HISTORY_POD

# Watch PDB — "ALLOWED DISRUPTIONS" should go to 0 momentarily
# Then recover to 1 after new pod is ready
```

### 5.3 Method B: Node Drain (Full GKE Upgrade Simulation)

This fully simulates what happens during a GKE node pool upgrade.

```bash
# Find a node that has Temporal pods
NODE_WITH_TEMPORAL=$(kubectl get pods -n temporal -l app.kubernetes.io/component=history \
  -o jsonpath='{.items[0].spec.nodeName}')
echo "Node with Temporal pods: $NODE_WITH_TEMPORAL"

# Cordon the node (prevent new pods from scheduling)
kubectl cordon $NODE_WITH_TEMPORAL

# Verify node is cordoned
kubectl get node $NODE_WITH_TEMPORAL

# Drain the node (respects PDBs!)
# --delete-emptydir-data: delete pods using emptyDir
# --ignore-daemonsets: ignore DaemonSets (they stay on the node)
# --timeout=300s: wait up to 5 minutes for pods to be evicted
kubectl drain $NODE_WITH_TEMPORAL \
  --delete-emptydir-data \
  --ignore-daemonsets \
  --timeout=300s

# With PDBs in place, kubectl drain will:
# 1. Evict one Temporal pod
# 2. Wait for replacement to be Ready
# 3. Then evict the next one (if any on this node)
```

**After testing, uncordon the node:**
```bash
kubectl uncordon $NODE_WITH_TEMPORAL
```

### 5.4 Method C: Simulated GKE Node Pool Upgrade (Most Realistic)

If your environment allows, trigger an actual node pool upgrade:

```bash
# Get current node pool name
NODE_POOL=$(gcloud container node-pools list --cluster $CLUSTER --region $REGION --format='value(name)' | head -1)

# Check current version
gcloud container node-pools describe $NODE_POOL --cluster $CLUSTER --region $REGION --format='value(version)'

# Get available upgrade versions
gcloud container get-server-config --region $REGION --format='yaml(validNodeVersions)'

# Trigger upgrade (to same or newer version — this forces pod evictions)
# WARNING: This will cause temporary disruption, ensure you're in a test env!
gcloud container node-pools upgrade $NODE_POOL \
  --cluster $CLUSTER \
  --region $REGION \
  --node-pool-version=<target-version>
```

---

## 6. Workflow Triggering During Drain

While draining nodes, start a bunch of dummy workflows to see the success/failure rate.

---

## 7. Success Criteria

### 7.1 Expected Behavior WITH PDBs

| Phase | Without PDBs (Incident) | With PDBs (Expected) |
|-------|-------------------------|----------------------|
| Pod evicted | Immediate, no wait | Wait for Ready replacement |
| Shard rebalancing | 30-60s "service failures" | Controlled, <5s per eviction |
| Workflow starts | `context deadline exceeded` | Success (may see brief retries) |
| Total disruption | Minutes (cascading) | Seconds (sequential) |

**Note:** PDBs cannot eliminate shard rebalancing time; they limit blast radius. During each 30–60s rebalancing window after a pod eviction, up to ~1% of workflow starts may fail transiently (e.g., `context deadline exceeded`) as long as they succeed on retry and overall success rate remains ≥99%.

### 7.2 Verification Checks

**Check 1: PDB Respected During Drain**
```bash
# During drain, watch PDB — "ALLOWED DISRUPTIONS" should alternate between 0 and 1
# It should NEVER show 0 for all PDBs simultaneously for more than ~30s
kubectl get pdb -n temporal
```

**Check 2: No "Service Failures" Storm**
```bash
# Query Cloud Logging for Temporal errors during test window
START_TIME=$(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ)
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)

gcloud logging read "resource.type=\"k8s_container\" \
  resource.labels.container_name=~\"temporal.*\" \
  severity>=WARNING \
  timestamp>=\"$START_TIME\" \
  timestamp<=\"$END_TIME\"" \
  --project $PROJECT \
  --format json | jq -r '.[].jsonPayload.message // .[].textPayload' | \
  grep -c "service failure" || echo "0 service failures"
```

**Check 3: Workflow Starts Succeeded**
```bash
# Check load test results
# Success rate should be >95%
```

**Check 4: Sequential Eviction Timeline**
```bash
# Check Kubernetes events for pod scheduling timeline
kubectl get events -n temporal --sort-by='.lastTimestamp' | grep -E 'Killing|Scheduled|Started'

# Events should show:
# 1. Pod A killed
# 2. Pod A' scheduled
# 3. Pod A' started
# 4. Pod B killed (only after A' is Ready)
# ...
```

---

## 8. Rollback Plan

If issues occur, rollback by removing PDB values & raise another PR.

---

## 9. After Successful Test — Production Rollout

Once validated in lower environment:

1. **Create PR** with PDB configuration for production values
2. **Coordinate with SRE** for scheduled rollout window
3. **Monitor first production GKE upgrade** after PDB deployment

---
