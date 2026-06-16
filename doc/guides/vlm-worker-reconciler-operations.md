# VLM Worker Reconciler — Operations Guide

## Overview

The `vlm-worker-reconciler` is a post-upgrade Helm hook Job that scales inactive `vlm-worker-*` Deployments to 0 after every `helm upgrade`. A failed run is non-critical — workers stay at `replicas=1` (safe default) and retry on the next upgrade. The deployment always succeeds regardless of reconciler outcome.

See the design doc: [0029-vlm-worker-reconciler-design.md](../architecture/designs/0029-vlm-worker-reconciler-design.md)

---

## Checking Job Logs

```bash
# Get the most recent reconciler pod logs
kubectl logs -n <namespace> job/vlm-worker-reconciler

# If the pod is gone (deleted by before-hook-creation policy before next upgrade)
# check Cloud Logging with this filter:
# resource.type="k8s_container"
# resource.labels.namespace_name="<namespace>"
# resource.labels.container_name="reconciler"
```

Common log patterns (structured `key=value` format from Go `slog`):

| Log line | Meaning |
|---|---|
| `WARN DB credentials missing or secret not synced` | ExternalSecret has not synced yet — see ExternalSecret section below |
| `ERROR database query failed` | DB connectivity or auth issue — see DB Connectivity section |
| `WARN G4: active version set is empty` | DB returned no active versions — no action taken (safe) |
| `ERROR G5: hierarchy logic would scale ALL workers to 0` | Logic guardrail fired — no scaling done |
| `WARN reconciler complete with partial failures scale_failures=N` | Some scale patches failed — check preceding ERROR lines for which deployments |

---

## Rollback / Disabling the Reconciler

**Option A — Disable entirely** (Job will not run on next upgrade):
```yaml
# In cloud-deploy overrides for the affected region
vlmWorkerReconciler:
  enabled: false
```

**Option B — Switch to dry-run** (Job runs and logs but makes no replica changes):
```yaml
vlmWorkerReconciler:
  dryRun: true
```

Deploy the override change via the normal release pipeline. The next `helm upgrade` will use the updated setting.

---

## ExternalSecret / DB Secret Not Synced

```bash
# Check ExternalSecret status
kubectl get externalsecret vlm-worker-reconciler-db-secret -n <namespace>

# If SecretSyncedError, describe for the exact error
kubectl describe externalsecret vlm-worker-reconciler-db-secret -n <namespace> | tail -20

# Force ESO to re-sync immediately without waiting for the refresh interval
kubectl annotate externalsecret vlm-worker-reconciler-db-secret \
  -n <namespace> force-sync=$(date +%s) --overwrite

# Verify the K8s secret was created
kubectl get secret vlm-worker-reconciler-db-secret -n <namespace>
```

---

## DB Connectivity

The reconciler image is distroless (no shell). Use `busybox` for connectivity tests:

```bash
# Test TCP connectivity to the DB host from within the namespace
kubectl run -n <namespace> dbtest --rm -it --restart=Never \
  --image=busybox:1.36 \
  --overrides='{"metadata":{"annotations":{"sidecar.istio.io/inject":"false"}}}' \
  -- sh -c "nc -zv <DB_HOST> 5432 && echo OPEN || echo FAILED"
```

For au-se1 staging: `DB_HOST = cloud-sql-proxy.sde.svc.cluster.local`

---

## Manually Restoring Worker Replicas

If inactive workers were incorrectly scaled to 0 and need immediate restoration:

```bash
# Scale a specific worker back to 1
kubectl scale deployment vlm-worker-<version> -n <namespace> --replicas=1

# Example
kubectl scale deployment vlm-worker-9-18-1p2 -n vlm-ontap-9-17-1 --replicas=1
```

The next `helm upgrade` automatically resets all workers to `replicas=1` from chart values, so manual restore is only needed when an immediate fix is required before the next deployment.

---

## Checking RBAC Permissions

The reconciler runs as `vlm-worker-ksa` (the existing vlm-worker service account) and requires `get/list` on Deployments and `patch` on `deployments/scale`:

```bash
kubectl auth can-i list deployments \
  --as=system:serviceaccount:<namespace>:vlm-worker-ksa -n <namespace>

kubectl auth can-i patch deployments/scale \
  --as=system:serviceaccount:<namespace>:vlm-worker-ksa -n <namespace>
```

Both should return `yes`. If not, inspect the Role and RoleBinding:

```bash
kubectl get role vlm-worker-reconciler-role -n <namespace> -o yaml
kubectl get rolebinding vlm-worker-reconciler-rolebinding -n <namespace> -o yaml
```

---

## Failure Reference

| Failure | Log pattern | Safe | Action |
|---|---|---|---|
| Missing DB credentials | `WARN DB credentials missing or secret not synced` | Yes | Fix ExternalSecret sync |
| DB connection failure | `ERROR database query failed` | Yes | Check DB connectivity |
| Empty active pool set (G4) | `WARN G4: active version set is empty` | Yes | Investigate pools table |
| All-zero result guardrail (G5) | `ERROR G5: hierarchy logic would scale ALL workers to 0` | Yes | Logic issue — raise with VCP Platform |
| Partial scale failures | `WARN reconciler complete with partial failures scale_failures=N` | Partial | Check preceding ERROR lines for affected deployments |
| Job deadline exceeded | `DeadlineExceeded` in Cloud Deploy | Check | Increase `activeDeadlineSeconds` or investigate hang |
