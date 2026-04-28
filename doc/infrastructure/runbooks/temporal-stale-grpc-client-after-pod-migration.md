# Temporal: Persistent EOF errors after pod replacement (stale matching client cache)

**Symptom**: Persistent `error reading server preface: EOF` errors from Temporal frontend / history calling matching, leading to `Failed to poll for task` and `no healthy upstream` on workers, even though all Temporal pods appear healthy and the membership ring is clean.

**First seen**: 2026-04-22 on `netapp-us-c1-staging-sde` after migrating Temporal pods from one GKE node pool (pod CIDR `240.71.x.x`) to another (`10.52.x.x`).

**Root cause** (short version): The Temporal Go process keeps a long-lived gRPC `ClientConn` to every matching pod it has ever talked to. When a matching pod's IP disappears (pod rescheduled to a new IP), the `ClientConn` for the old IP is **not** closed automatically. It sits in `TRANSIENT_FAILURE` and keeps retrying its TCP connection to the dead IP forever. Because the destination is in STRICT mTLS or no longer exists, the underlying TCP either RSTs immediately or never sends an HTTP/2 server preface — the Go gRPC client surfaces this as `error reading server preface: EOF`.

**Fix** (two steps, in this order):

1. **Consolidate all Temporal pods and their callers (VCP, VLM, etc.) onto a single pod-CIDR / single node pool.** This eliminates cross-pool networking as a confounder, completes any in-flight node-pool migration, and gives the Envoy + ringpop layers a clean topology to converge on. In the 2026-04-26 incident this meant finishing the move onto `us-c1-new-pool` (CIDR `10.52.x.x`) so no pod was left on the old `240.71.x.x` CIDR. Confirm via `kubectl get po -A -o wide` that no Pending pods remain (rolling restart can't complete with Pending surge replicas — see Section 4.1).
2. **Rolling-restart Temporal in matching → history → frontend → worker order** so each Go process rebuilds its in-memory matching `ClientConn` cache from scratch. See Section 4.3.

The membership table itself does not need a manual fix; it self-cleans via the heartbeat TTL. Step 1 alone will not clear the EOFs — the stale `ClientConn`s persist across pod-IP changes inside surviving Temporal Go processes until those processes restart.

---

## 1. Symptoms

### 1.1 Frontend logs

```
{"level":"info","msg":"matching client encountered error","service":"frontend",
 "error":"connection error: desc = \"error reading server preface: EOF\"",
 "service-error-type":"serviceerror.Unavailable",
 "logging-call-at":".../client/matching/metric_client.go:197"}

{"level":"error","msg":"Unable to call matching.PollActivityTaskQueue.",
 "service":"frontend","wf-task-queue-name":"<task-queue>",
 "error":"connection error: desc = \"error reading server preface: EOF\"",
 "logging-call-at":".../service/frontend/workflow_handler.go:1168"}
```

### 1.2 History logs (transfer-queue stuck retry loop)

```
{"level":"warn","msg":"Fail to process task","shard-id":417,
 "address":"10.52.6.50:7234","component":"transfer-queue-processor",
 "queue-task-type":"TransferActivityTask",
 "error":"connection error: desc = \"error reading server preface: EOF\"",
 "attempt":2 → 3 → 4 → 5}
```

### 1.3 Worker side (VCP / VLM)

```
# From a VCP customer-worker:
{"severity":"WARNING","message":"Failed to poll for task.",
 "WorkerType":"WorkflowWorker","Error":"no healthy upstream"}

# From a VLM worker (long-poll path):
"Context timeout is too short: 1s. Should be at least 1m"
```

The worker-side `no healthy upstream` is **Envoy outlier detection** ejecting frontend endpoints after frontend keeps returning 503s (because frontend → matching is failing). It is a downstream effect, not the root cause.

---

## 2. Triage Decision Tree

```
EOF errors in temporal logs?
│
├── Are matching pods Running 3/3?
│   ├── No → Fix scheduling (capacity, anti-affinity, nodeSelector) FIRST.
│   │        Section 4.1.
│   └── Yes → continue
│
├── Is Istio mTLS config correct?
│   (PA: STRICT in temporal ns, DRs: ISTIO_MUTUAL on every temporal-* host,
│    callers in vcp / vlm-* have sidecars injected)
│   ├── No  → Fix the misconfigured PA / DR / sidecar injection. Section 3.7.
│   │         Restart the affected workloads. Restarting Temporal alone
│   │         will NOT help.
│   └── Yes → continue
│
├── Does the membership ring (`tctl admin cluster describe`) contain any
│   IPs that are NOT current pod IPs?
│   ├── Yes → Stale persistence-layer entry. Wait for `record_expiry`,
│   │         or DELETE the row in `cluster_membership`. Section 4.2.
│   └── No  → continue
│
├── Does any temporal-frontend / -history / -worker pod's istio-proxy
│   show `PassthroughCluster::<old-IP>:<port>` for old matching pod IPs?
│   ├── Yes → STALE gRPC CLIENT CACHE inside the temporal Go process.
│   │         Rolling restart frontend/history/worker. Section 4.3.
│   └── No  → Different problem. Re-investigate (EDS, network, ingress).
```

---

## 3. Diagnostic Commands

### 3.1 Cluster topology

```bash
# All temporal pods + their IPs
kubectl -n temporal get po -o wide

# Pending pods anywhere (capacity check; blocks rolling restarts)
kubectl get po -A --field-selector=status.phase=Pending -o wide

# Stuck rollouts
kubectl get deploy -A -o json \
  | jq -r '.items[] | select(.status.replicas != .status.readyReplicas) |
           "\(.metadata.namespace)/\(.metadata.name): \(.status.readyReplicas)/\(.status.replicas)"'
```

### 3.2 Membership ring (the ground truth)

```bash
ADMIN=$(kubectl -n temporal get po --field-selector=status.phase=Running \
          -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
        | grep '^temporal-admintools-' | head -1)

kubectl -n temporal exec $ADMIN -- \
  tctl --ad temporal-frontend.temporal.svc.cluster.local:7233 admin cluster describe
```

What we expect for a clean cluster:
```
"rings": [
  { "role": "frontend", "members": [<current frontend pod IPs>:7233] },
  { "role": "history",  "members": [<current history pod IPs>:7234] },
  { "role": "matching", "members": [<current matching pod IPs>:7235] },
  { "role": "worker",   "members": [<current worker pod IPs>:0]    }
]
```

If you see any IP that is NOT in `kubectl -n temporal get po -o wide`, that's a stale persistence-layer entry. It is **different** from a stale in-memory client cache (this section's symptom is the *clean* ring case).

### 3.3 Envoy view from each Temporal client pod (the smoking gun)

```bash
# Pick a frontend pod by name pattern (label selector varies by helm chart)
FE=$(kubectl -n temporal get po --field-selector=status.phase=Running \
       -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
     | grep '^temporal-frontend-' | head -1)
echo "frontend: $FE"

# Cluster endpoints (should match membership ring)
kubectl -n temporal exec $FE -c istio-proxy -- pilot-agent request GET clusters \
  | grep -E 'temporal-matching|7235|6935' \
  | grep -E '::address|::cx_active|::cx_total|::health_flags'

# Stale-IP check: any reference to old pod CIDR
kubectl -n temporal exec $FE -c istio-proxy -- pilot-agent request GET clusters \
  | grep '<OLD-CIDR>'         # e.g. 240\.71\.

# PassthroughCluster activity (rerun 30s apart; growth = active leak)
kubectl -n temporal exec $FE -c istio-proxy -- pilot-agent request GET stats \
  | grep -E 'cluster\.PassthroughCluster\.(upstream_cx_total|upstream_cx_destroy_remote|upstream_cx_connect_fail|upstream_rq_total)'
```

The classic stuck-cache fingerprint:

```
PassthroughCluster::<OLD-IP>:7235::cx_active::1
PassthroughCluster::<OLD-IP>:7235::cx_total::1
PassthroughCluster::<OLD-IP>:7235::rq_active::0
PassthroughCluster::<OLD-IP>:7235::rq_total::0
PassthroughCluster::<OLD-IP>:7235::health_flags::healthy
```

Reading: an open TCP socket to a non-existent IP, never produced a successful HTTP/2 request — exactly what a gRPC client stuck in `TRANSIENT_FAILURE` does.

Why "PassthroughCluster"?
- The dead IP is no longer in any EDS cluster's endpoint list (because the headless service's endpoint slice no longer contains it).
- Envoy's outbound listener falls through to `PassthroughCluster`, which does ORIGINAL_DST plaintext forwarding.
- With STRICT `PeerAuthentication` in the `temporal` namespace, plaintext is rejected by any current pod that may have inherited that IP — connection terminates without a server preface → `EOF`.

### 3.4 Same check on every history and worker pod

```bash
for HX in $(kubectl -n temporal get po --field-selector=status.phase=Running \
              -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
            | grep '^temporal-history-'); do
  echo "== $HX =="
  kubectl -n temporal exec $HX -c istio-proxy -- pilot-agent request GET clusters \
    | grep '<OLD-CIDR>' || echo "  (clean)"
done

for WP in $(kubectl -n temporal get po --field-selector=status.phase=Running \
              -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
            | grep '^temporal-worker-'); do
  echo "== $WP =="
  kubectl -n temporal exec $WP -c istio-proxy -- pilot-agent request GET clusters \
    | grep '<OLD-CIDR>' || echo "  (clean)"
done
```

Mark every pod that prints something other than `(clean)`. Each of those holds a stale client cache and must be cycled.

### 3.5 Worker-side outlier ejection (explains `no healthy upstream`)

```bash
# Pick a complaining worker
WP=<vcp-customer-worker-pod>

# Frontend cluster endpoints + outlier-detection state
kubectl -n vcp exec $WP -c istio-proxy -- pilot-agent request GET clusters \
  | grep -E 'temporal-frontend|7233' \
  | grep -E '::address|::cx_connect_fail|::health_flags|::hostname'

kubectl -n vcp exec $WP -c istio-proxy -- pilot-agent request GET stats \
  | grep -E 'temporal-frontend.*outlier_detection'
```

`FAILED_OUTLIER_CHECK` health flag or `outlier_detection.ejections_active > 0` confirms worker sidecars have ejected all frontend endpoints. This clears on its own once frontend stops returning 5xx.

### 3.6 Persistence and version info (handy for tickets / regressions)

From `tctl admin cluster describe` output:
- `persistenceStore` (e.g. `postgres12`)
- `visibilityStore`
- `serverVersion`
- `clusterId`, `clusterName`

In the 2026-04-26 incident: `serverVersion=1.29.1`, `persistenceStore=postgres12`.

### 3.7 Validate Istio mTLS configuration (rule out a real misconfig)

Before concluding this is a stale-client-cache issue, sanity-check that `PeerAuthentication` and the `DestinationRule` set for the Temporal services are consistent. A mismatch (e.g. one side STRICT mTLS while the other side talks plaintext) will produce identical `error reading server preface: EOF` symptoms but requires a totally different fix.

```bash
echo "==== PeerAuthentication for the temporal namespace ===="
kubectl get peerauthentication -n temporal -o yaml \
  | grep -E 'name:|namespace:|mode:|matchLabels:|app:'

echo
echo "==== DestinationRules in the temporal namespace ===="
kubectl get destinationrule -n temporal -o yaml \
  | grep -E 'name:|host:|mode:'
```

Expected, healthy state (this is what the 2026-04-26 cluster had — and is the recommended pattern):

```
PeerAuthentication
  name:       istio-peer-authentication-temporal
  namespace:  temporal
  mode:       STRICT          # applies to ALL workloads in the namespace (no selector)

DestinationRule (one per Temporal service)
  name:  temporal-frontend-mtls            host: temporal-frontend.temporal.svc.cluster.local            mode: ISTIO_MUTUAL
  name:  temporal-frontend-headless-mtls   host: temporal-frontend-headless.temporal.svc.cluster.local   mode: ISTIO_MUTUAL
  name:  temporal-history-headless-mtls    host: temporal-history-headless.temporal.svc.cluster.local    mode: ISTIO_MUTUAL
  name:  temporal-matching-headless-mtls   host: temporal-matching-headless.temporal.svc.cluster.local   mode: ISTIO_MUTUAL
  name:  temporal-worker-headless-mtls     host: temporal-worker-headless.temporal.svc.cluster.local     mode: ISTIO_MUTUAL
  name:  temporal-internal-frontend-mtls   host: temporal-internal-frontend.temporal.svc.cluster.local   mode: ISTIO_MUTUAL
  name:  temporal-admintools-mtls          host: temporal-admintools.temporal.svc.cluster.local          mode: ISTIO_MUTUAL
  name:  temporal-web-mtls                 host: temporal-web.temporal.svc.cluster.local                 mode: ISTIO_MUTUAL
```

Pass criteria:

1. **Exactly one `PeerAuthentication` in the `temporal` namespace**, with `mode: STRICT` and **no** `selector` (so it applies to every Temporal workload).
2. **A `DestinationRule` for every Temporal service** the client side dials, all using `tls.mode: ISTIO_MUTUAL` and no per-port overrides.
3. **No `DisablePolicyChecks` / `disabled` PA** for any Temporal workload that would create a STRICT/PERMISSIVE asymmetry.
4. **No conflicting `PeerAuthentication`** at workload-selector level inside the `temporal` namespace overriding the namespace-wide STRICT (e.g. a `mode: DISABLE` PA on `app: temporal-matching` would silently break matching mTLS).

What a misconfig would look like (and how to spot it):

```bash
# Anything that says PERMISSIVE or DISABLE inside the temporal namespace is suspicious
kubectl get peerauthentication -n temporal -o yaml | grep -E 'mode:'

# A DR pointing at a temporal-* host with mode != ISTIO_MUTUAL is suspicious
kubectl get destinationrule -A -o yaml \
  | grep -B1 -A6 'host: .*temporal-' | grep -E 'name:|host:|mode:'
```

If you find any of these:
- a `PeerAuthentication` in `temporal` with `mode: PERMISSIVE` or `mode: DISABLE`,
- a `DestinationRule` for a `temporal-*` host with `mode: DISABLE` / `SIMPLE` / `MUTUAL` (note: not `ISTIO_MUTUAL`) / no `tls` block at all,
- two PAs at different scopes contradicting each other,

then the EOFs are an mTLS misconfig, **not** the cache-cache problem this runbook addresses, and you should fix the config first (the rolling restart will not help).

Cross-namespace check (clients of Temporal must speak mTLS):

```bash
echo "==== PeerAuthentication for callers (vcp / vlm-* / etc) ===="
kubectl get peerauthentication -A -o yaml \
  | grep -E 'name:|namespace:|mode:|matchLabels:'

echo
echo "==== Sidecar injection on caller deployments (must be enabled) ===="
for ns in vcp vlm-ontap-9-17-1; do
  echo "-- $ns"
  kubectl -n $ns get deploy -o json \
    | jq -r '.items[] | "\(.metadata.name): istio-injection=\(.spec.template.metadata.labels."sidecar.istio.io/inject" // "default")"'
done
```

Caller workloads (VCP / VLM) need a sidecar injected so they can speak ISTIO_MUTUAL to the Temporal services. If `sidecar.istio.io/inject` is `false` on a caller, that caller will be sending plaintext into a STRICT-mTLS namespace and will produce identical-looking EOF errors.

Quick interpretation cheatsheet:

| What you see | Likely cause | Fix |
|---|---|---|
| Temporal namespace `STRICT` + all DRs `ISTIO_MUTUAL` + EOFs continue | This runbook's scenario (stale client cache) | Section 4.3 |
| Temporal namespace `STRICT` + a DR missing `ISTIO_MUTUAL` | mTLS asymmetry | Add `tls.mode: ISTIO_MUTUAL` to the DR |
| Temporal namespace `STRICT` + caller pod has no sidecar | Plaintext caller into STRICT namespace | Enable sidecar injection on the caller |
| Temporal namespace `PERMISSIVE` (unexpected) + EOFs | Possibly intended, but raises blast radius for any IP-leak case | Tighten to `STRICT`, verify all clients have DRs |
| Temporal namespace `STRICT` + workload-scoped PA with `DISABLE` | Hidden override breaking one service | Remove or correct the workload-scoped PA |

Why this matters specifically for the stale-client-cache scenario: STRICT mTLS is what makes the symptom *visible* as a clean `EOF` (because the dead IP, if it's even reachable, refuses plaintext). Without STRICT mTLS, the plaintext PassthroughCluster traffic to a recycled IP could silently land on a *different workload*, which is much harder to debug. STRICT is doing its job here; do not relax it as a workaround.

---

## 4. Resolution

The fix is two steps in order: **(1) consolidate onto one pod-CIDR**, then **(2) rolling-restart Temporal**. Step 1 alone does not clear the EOFs. Step 2 alone, run while pods are still split across CIDRs and capacity is tight, is unsafe (rollouts stall mid-way and you end up with worse-than-before mixed state).

### 4.1 Step 1 — Consolidate all pods onto a single pod-CIDR

Goal: every Temporal pod, and every caller of Temporal (VCP, VLM, etc.), running on the same pod-CIDR / node pool, with **zero Pending pods anywhere** in the cluster.

Why this matters as a real step (not just hygiene):

- It eliminates cross-pool networking as a confounder for the diagnosis. If EOFs persist after consolidation, the cause is unambiguously in-process state.
- It completes any in-flight node-pool migration so EDS endpoint slices are stable when the rolling restart begins.
- A rolling restart needs surge headroom; if any deployment is already `n/m` Ready or has Pending replicas, restarting more pods will make things worse.

Do **not** proceed to step 2 while any of these are true:

- Any `temporal-*` deployment is `n/m` Ready (e.g. 2/3).
- Any non-Temporal deployment that calls Temporal has Pending pods (typically VCP / VLM workloads on the same cluster).
- Pods still exist on the *old* pod-CIDR (e.g. some on `240.71.x.x` while the rest are on `10.52.x.x`).

If the destination pool is over-committed:
- Re-enable a previously-removed node pool (uncordon, scale up, or remove the nodeSelector that pinned everything onto the smaller pool).
- Or scale up the existing node pool until all Pending pods schedule.

In the 2026-04-26 incident this step was: finish forcing every workload onto `us-c1-new-pool` (CIDR `10.52.x.x`), then scale up that pool until the surge replicas from the in-flight rollout could schedule. Once all pods were `Running` on `10.52.x.x` and zero Pending pods remained, we proceeded to step 2.

Verification before moving on:

```bash
# Every temporal pod on the same CIDR
kubectl -n temporal get po -o wide | awk 'NR==1 || /^temporal-/ {print $1, $6, $7}'

# Zero Pending pods cluster-wide
kubectl get po -A --field-selector=status.phase=Pending -o wide

# Every deployment fully Ready
kubectl get deploy -A -o json \
  | jq -r '.items[] | select(.status.replicas != .status.readyReplicas) |
           "\(.metadata.namespace)/\(.metadata.name): \(.status.readyReplicas)/\(.status.replicas)"'
```

The EOFs will still be present after step 1 — that is expected and **proves** the bug is in-process gRPC client cache rather than networking.

### 4.2 (Optional, rare) Stale persistence-layer entry

Only applies if `tctl admin cluster describe` shows IPs that don't match any current pod. In Temporal 1.29.x with Postgres:

```sql
-- Verify before deleting
SELECT host_id, rpc_address, role, last_heartbeat, record_expiry
FROM cluster_membership
WHERE rpc_address LIKE '<OLD-CIDR>%'
ORDER BY last_heartbeat DESC;

-- If `record_expiry` is in the future for any of those rows, they are still
-- considered live by the cluster. Either wait for expiry or delete:
DELETE FROM cluster_membership
WHERE rpc_address LIKE '<OLD-CIDR>%' AND record_expiry < now();
```

Do **not** delete rows whose `record_expiry` is in the future without confirming the pod is gone (`kubectl -n temporal get po -o wide`). Deleting a live member's row will cause membership churn.

After delete, do Section 4.3 to flush any in-memory caches that loaded the stale entry.

### 4.3 Step 2 — Rolling-restart Temporal to flush stale gRPC client caches

Only run this **after** Section 4.1 has fully completed (single CIDR, zero Pending pods, all deployments fully Ready). This is the step that actually clears the EOFs.

Rolling-restart in this order:

```bash
kubectl -n temporal rollout restart deploy/temporal-matching
kubectl -n temporal rollout status  deploy/temporal-matching --timeout=3m
sleep 30

kubectl -n temporal rollout restart deploy/temporal-history
kubectl -n temporal rollout status  deploy/temporal-history  --timeout=3m
sleep 30

kubectl -n temporal rollout restart deploy/temporal-frontend
kubectl -n temporal rollout status  deploy/temporal-frontend --timeout=3m
sleep 30

kubectl -n temporal rollout restart deploy/temporal-worker
kubectl -n temporal rollout status  deploy/temporal-worker   --timeout=3m
```

Order matters: matching first so its members are fresh in the ring before history/frontend reload caches against it.

If you cannot do a full rolling restart (e.g. capacity-constrained), surgical pod deletion works too:

```bash
# Delete every temporal pod that showed PassthroughCluster::<OLD-IP> entries.
# One at a time, wait for replacement to become Ready before the next.
kubectl -n temporal delete po <pod-name>
kubectl -n temporal get po -w   # Ctrl-C once new pod is Running 2/2
```

### 4.4 (Optional) Clear worker-side outlier ejections

After Temporal stops returning 5xx, worker sidecars' outlier detection re-tests frontend endpoints and unejects them within ~60s. If you want immediate recovery:

```bash
kubectl -n vcp rollout restart deploy/<vcp-customer-worker-deploy>
kubectl -n vlm-ontap-9-17-1 rollout restart deploy/<vlm-worker-deploy>
```

This is rarely necessary; the ejections clear on their own once Temporal is healthy.

---

## 5. Verification

```bash
# 1. Membership ring — should match current pod IPs only
kubectl -n temporal exec $ADMIN -- \
  tctl --ad temporal-frontend.temporal.svc.cluster.local:7233 admin cluster describe \
  | jq '.membershipInfo.rings[] | {role, members: [.members[].identity]}'

# 2. No stale Envoy entries on any temporal pod
for P in $(kubectl -n temporal get po -o name | grep -E 'temporal-(frontend|history|worker|matching)'); do
  echo "== $P =="
  kubectl -n temporal exec ${P#pod/} -c istio-proxy -- pilot-agent request GET clusters \
    | grep '<OLD-CIDR>' || echo "  (clean)"
done

# 3. EOF count in last 200 lines per frontend / history pod
# (Temporal helm chart sets kubectl.kubernetes.io/default-container so the
#  temporal application container is selected without -c. If your chart does
#  not set that annotation, replace with: -c "$(kubectl -n temporal get po
#  "$ns_pod" -o jsonpath='{.spec.containers[?(@.name!="istio-proxy")].name}'
#  | awk '{print $1}')")
for P in $(kubectl -n temporal get po -o name | grep -E 'temporal-(frontend|history)'); do
  ns_pod=${P#pod/}
  ct=$(kubectl -n temporal logs ${ns_pod} --tail=200 \
         | grep -c 'server preface: EOF' || true)
  echo "$ns_pod: $ct"
done

# 4. Worker-side: no `no healthy upstream` and no `Failed to poll for task` (last 200)
kubectl -n vcp logs -l app=vcp-customer-worker --tail=200 \
  | grep -cE 'no healthy upstream|Failed to poll for task'
```

Pass criteria:
- Ring members exactly match pod IPs.
- All temporal pods print `(clean)`.
- EOF count 0 (or single-digit transients during restart).
- Worker logs show no `no healthy upstream`.

---

## 6. Root-Cause Statement

> Migrating Temporal pods to a new node pool changes their pod IPs. Each Temporal Go process maintains an in-memory cache of gRPC `ClientConn` objects keyed by peer pod IP for cross-service calls (especially frontend/history → matching). When a peer's IP disappears, the corresponding `ClientConn` enters `TRANSIENT_FAILURE` and keeps retrying its underlying TCP connection indefinitely. With Istio STRICT `PeerAuthentication` in the `temporal` namespace, the dead IP either no longer responds or rejects the now-plaintext PassthroughCluster traffic, so the gRPC client never reads an HTTP/2 server preface and surfaces `connection error: error reading server preface: EOF` to the application. Membership tables and Envoy EDS recover on their own; the in-memory client cache does not, and is only flushed by restarting the affected Temporal pod.

## 7. Prevention

1. **Drain Temporal cleanly before node-pool migration.** A graceful shutdown sends SIGTERM, the matching client cache is torn down with the process, and replacement pods bootstrap from a clean state. Avoid hard pod deletes during migration.
2. **Use a PDB on every Temporal deployment** so that simultaneous evictions are impossible. See `doc/infrastructure/runbooks/temporal-pdb-test-plan.md`.
3. **Don't migrate to a node pool that is at or near capacity.** If rolling restart can't complete because the surge replica can't schedule, you'll be stuck with two-thirds of a deployment and stale caches you can't refresh. Always keep ≥1 fully empty node-equivalent of headroom in the destination pool.
4. **After any Temporal pod-IP migration, run Section 5 verification.** This entire incident would have been caught in <2 minutes by step 2 of verification.

## 8. Cross-References

- `doc/infrastructure/runbooks/temporal-pdb-test-plan.md` — preventive PDB configuration
- `doc/guides/temporal-helm-customizations.md` — helm chart values
- https://github.com/temporalio/temporal/issues/8719

---

## Appendix A — Reference Snapshots from 2026-04-26

Real evidence captured during this incident, kept here so future investigators have a concrete picture of what each diagnostic looks like in practice.

### A.1 Initial topology — pods spread across two GKE node pools

The cluster had two GKE node pools with **different pod CIDRs**, and Temporal/VCP/VLM workloads were scheduled across both:

| Node pool | Pod CIDR | Example node names |
|---|---|---|
| `us-c1-new-pool <old-pool>` | `10.52.x.x` | `gke-cv-g1p-stage-us-ce-us-c1-new-pool-faf47f7b-fppi`, `...-0fe65891-0vfn`, `...-ad770000-rh4r` |
| `npl-sde-new <new-pool>` | `240.71.x.x` | `gke-cv-g1p-stage-us-centr-npl-sde-new-4f3970ae-uj1j`, `...-8ba4cfbe-phg0`, `...-adcb55da-butc` |

#### A.1.1 `temporal` namespace (pre-migration excerpt)

Temporal services had pods on **both** pod CIDRs at different points (collected from `kubectl logs` of an older frontend pod showing `Current reachable members`):

```
frontend ring contained:
  10.52.2.31:7233   (us-c1-new-pool)
  240.71.193.x:7233 × 3   (npl-sde-new)

matching ring contained:
  10.52.0.38:7235, 10.52.6.65:7235   (us-c1-new-pool)
  240.71.194.199:7235                 (npl-sde-new)  ← became the stale entry
```

An important note about this topology: the fact that pods had been scheduled across both pools for >47 hours and were `Running 2/2` is **not** by itself proof that cross-pool networking was healthy. With workloads on both CIDRs at the source AND destination, requests that happened to land same-CIDR (e.g. frontend on `10.52` calling matching on `10.52`) would succeed, while requests that crossed CIDRs would silently fail or time out. Aggregate behaviour ("workloads stay alive, some workflows finish") would still look mostly OK, just with elevated error rates and tail-latency that's easy to mistake for normal flake.

What actually ruled out cross-pool networking as a cause was post-migration evidence (Sections A.4 and A.5):

1. After everything was forced onto `us-c1-new-pool` (single CIDR `10.52.x.x`), there was **no cross-pool path left** — every Temporal pod, every VCP/VLM worker, was on the same CIDR.
2. EOF errors continued at the same rate.
3. Frontend Envoy on `10.52.5.8` was still dialing `240.71.194.199:7235` (a dead IP no current pod owns), via `PassthroughCluster`.
4. The Temporal membership ring (Section A.5) was clean — no `240.71.x.x` anywhere — yet the in-memory client cache inside the frontend Go process still had a `ClientConn` to the dead IP.

That sequence — same-CIDR-only topology + clean ring + stale PassthroughCluster entry + persistent EOFs — is what definitively points at the in-memory client cache and rules out networking. The pre-migration "it worked for 47h" observation is *consistent with* cross-pool networking being fine, but is not on its own conclusive.

#### A.1.2 `vcp` namespace (mixed across pools)

```
core-service-7d5fcb679d-fkg94                     10.52.6.52       us-c1-new-pool
google-proxy-6b6f785ddd-9zd29                     240.71.193.83    npl-sde-new
google-proxy-6b6f785ddd-q2dqj                     240.71.192.93    npl-sde-new
google-proxy-6b6f785ddd-w8kkq                     240.71.194.21    npl-sde-new
vcp-background-worker-...-rc-44-...-6r8ww         10.52.6.28       us-c1-new-pool
vcp-background-worker-...-rc-44-...-gmnbk         240.71.193.147   npl-sde-new
vcp-background-worker-...-rc-44-...-n4krj         240.71.193.21    npl-sde-new
vcp-customer-worker-...-rc-47-...-mwn2m           240.71.194.22    npl-sde-new
vcp-customer-worker-...-rc-47-...-qd5wh           240.71.193.87    npl-sde-new   ← logged "no healthy upstream"
vcp-customer-worker-...-rc-47-...-rtzcg           10.52.4.32       us-c1-new-pool
vsa-harvest-otel-bbb94997-4mrjs                   10.52.1.33       us-c1-new-pool
vsa-harvest-otel-bbb94997-8kdcx                   240.71.194.19    npl-sde-new
```

Because both VCP-on-`240.71` and VCP-on-`10.52` worker pods produced the same `no healthy upstream` errors against frontend pods that were also split across pools, we could rule out cross-pool networking — the failure was symmetric.

#### A.1.3 `vlm-ontap-9-17-1` namespace (also mixed)

```
vlm-worker-9-17-1-...-5vctt            10.52.1.31       us-c1-new-pool
vlm-worker-9-17-1-...-bh8rv            240.71.194.18    npl-sde-new
vlm-worker-9-17-1-...-tm5gw            240.71.193.163   npl-sde-new
vlm-worker-9-17-1p1-...-2zzx9          10.52.4.31       us-c1-new-pool
vlm-worker-9-17-1p1-...-btmx8          10.52.3.41       us-c1-new-pool
vlm-worker-9-17-1p1-...-ls6j9          10.52.1.32       us-c1-new-pool
vlm-worker-9-18-1p1-...-5zk8f          10.52.6.56       us-c1-new-pool
vlm-worker-9-18-1p1-...-mwcdd          240.71.192.221   npl-sde-new
vlm-worker-9-18-1p1-...-x959t          10.52.2.33       us-c1-new-pool
```

Same symmetry: VLM workers on both CIDRs all hit the same Temporal failure modes.

### A.2 Ringpop view from the original (pre-restart) frontend pod

`kubectl -n temporal logs deploy/temporal-frontend --tail=5000 | grep -i 'reachable members'` showed (excerpt):

```
{"level":"info","msg":"Current reachable members","service":"frontend",
 "component":"service-resolver","service":"matching",
 "addresses":["10.52.0.38:7235","10.52.6.65:7235","240.71.194.199:7235"]}
```

This proved the matching ring had 3 members, of which **`240.71.194.199`** was on the npl-sde-new pool. After we forced the migration to `10.52.x.x`, this pod IP went away — but the in-memory `ClientConn` for it inside frontend/history processes did not close, generating the steady-state EOFs.

### A.3 After the migration to a single node pool — Pending pods

Forcing every workload onto `us-c1-new-pool` removed the cross-pool topology but introduced capacity pressure. Multiple deployments ended up with a Pending surge replica that couldn't schedule:

```
NAMESPACE          POD                                                       STATUS    AGE
temporal           temporal-frontend-549d4cb6b5-rmktg                        Pending   3m35s
temporal           temporal-worker-5b56c56d74-dhlz8                          Pending   3m35s
vcp                google-proxy-6b6f785ddd-jf4cd                             Pending   3m54s
vcp                vcp-background-worker-26031-0-0-rc-46-66c8b694bf-lmcqf    Pending   3m54s
vcp                vcp-customer-worker-26031-0-0-rc-47-545dc6677-z4xvw       Pending   3m54s
vlm-ontap-9-17-1   vlm-worker-9-18-1p1-6f49bfd69-29fz9                       Pending   4m1s
```

These all corresponded to the surge pod from a `rollout restart` that the destination pool couldn't fit. Lesson: re-pinning to a single pool only works if that pool has surge headroom (~`maxSurge × replicas` of CPU/memory across distinct nodes).

### A.4 The smoking-gun Envoy snapshot

After the migration but before the restart fix, `pilot-agent clusters` on a frontend pod (`temporal-frontend-549d4cb6b5-79b2t`, age 12m) showed:

```
# Healthy live endpoints (3 matching pods, all 10.52.x.x):
outbound|7235||temporal-matching-headless...::10.52.0.38:7235  cx_active=1  health_flags=healthy
outbound|7235||temporal-matching-headless...::10.52.3.57:7235  cx_active=1  health_flags=healthy
outbound|7235||temporal-matching-headless...::10.52.6.65:7235  cx_active=1  health_flags=healthy

# The leak — a TCP socket to a dead old IP, never produced a single HTTP/2 request:
PassthroughCluster::240.71.194.199:7235  cx_active=1  cx_total=1
                                         rq_total=0   rq_success=0
                                         cx_connect_fail=0
```

This is the canonical fingerprint of a stuck gRPC `ClientConn` cached inside the temporal-frontend Go process.

### A.5 `tctl admin cluster describe` after rolling restart of matching

This is what a **clean** ring looks like — no `240.71.x.x` anywhere, all members match current pod IPs:

```json
{
  "membershipInfo": {
    "rings": [
      { "role": "frontend", "memberCount": 2,
        "members": [{"identity":"10.52.5.8:7233"},{"identity":"10.52.3.50:7233"}] },
      { "role": "history",  "memberCount": 3,
        "members": [{"identity":"10.52.0.32:7234"},{"identity":"10.52.6.50:7234"},{"identity":"10.52.4.30:7234"}] },
      { "role": "matching", "memberCount": 3,
        "members": [{"identity":"10.52.6.65:7235"},{"identity":"10.52.0.38:7235"},{"identity":"10.52.3.57:7235"}] },
      { "role": "worker",   "memberCount": 2,
        "members": [{"identity":"10.52.6.67:0"},{"identity":"10.52.2.30:0"}] }
    ]
  },
  "clusterId": "7b9a7658-9327-4914-bdb3-883482f0d776",
  "clusterName": "active",
  "historyShardCount": 512,
  "persistenceStore": "postgres12",
  "visibilityStore": "postgres12",
  "serverVersion": "1.29.1"
}
```

The critical observation: even with this perfectly clean ring, frontend Envoy still showed the `PassthroughCluster::240.71.194.199:7235` entry from A.4 — proving the bug was not in the persistence layer or the ring, but in the **in-memory matching client cache** held by the frontend Go process. This is what mandated the rolling restart in Section 4.3.

### A.6 Istio security context (relevant to why we see `EOF` and not silent reroute)

```
PeerAuthentication (namespace=temporal): mtls.mode = STRICT
DestinationRule for every temporal-*.svc:  trafficPolicy.tls.mode = ISTIO_MUTUAL
```

With STRICT mTLS in the `temporal` namespace, the PassthroughCluster's plaintext fallback to a non-existent (or recycled) IP is rejected — which is what produces the clean `error reading server preface: EOF` rather than silent misrouting. STRICT mTLS is **doing its job** here; it is not the bug.