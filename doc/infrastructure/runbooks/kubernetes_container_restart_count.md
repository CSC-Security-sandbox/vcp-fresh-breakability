# Kubernetes Container Restart Count Runbook

This runbook provides a step-by-step guide to debug Kubernetes container restart counts. A high restart count can indicate underlying issues such as resource constraints, application errors, or misconfigurations.

## Dashboard

**Link to the Dashboard:** [Attention Dashboard](https://console.cloud.google.com/monitoring/dashboards/builder/45a945a3-e40f-4181-9455-840a7e11b200;duration=P14D;filters=type:rlabel,key:location?invt=Ab6I-A&project=netapp-prod-monitoring)

## Summary of Past Findings

### Restart during Upgrades
Container restarts can occur during upgrades due to rolling updates of deployments or StatefulSets. During a rolling update, new versions of pods are gradually brought up, and old versions are terminated. This process inherently involves container restarts as old containers are replaced by new ones. This is expected behavior and typically doesn't indicate a problem unless the restarts are excessive or the new pods fail to come up.

### Misconfigured Liveness Probes
Liveness probes that are too aggressive (e.g., short `periodSeconds` or `failureThreshold`) or point to an endpoint that temporarily becomes unresponsive can falsely report the container as unhealthy, leading to unnecessary restarts by Kubernetes.

### Memory Leaks
Applications exhibiting memory leaks gradually consume more memory over time, eventually hitting their configured memory limits and causing the container to be OOMKilled (Out Of Memory Killed) by Kubernetes, leading to a restart. We have an alert configured for OOMs. 

## 1. Initial Triage

Before diving into detailed debugging, gather initial information to narrow down the possible causes.

### Check Pod Status

```bash
kubectl get pods -n <namespace>
```

Look for pods with a `CrashLoopBackOff` or `Error` status.

### Examine Pod Description

```bash
kubectl describe pod <pod-name> -n <namespace>
```

Pay attention to the **Events** section for any error messages or warnings that might explain the restarts.

### View Pod Logs

```bash
kubectl logs <pod-name> -n <namespace>
kubectl logs <pod-name> -n <namespace> -p # For previous container logs
```

Analyze the logs for application-specific errors, exceptions, or startup failures.

## 2. Common Causes and Solutions

### 2.1. Resource Constraints

Containers might restart if they exceed their allocated CPU or memory limits.

#### Identify Resource Usage

```bash
kubectl top pod <pod-name> -n <namespace>
```

Compare current usage with the limits and requests defined in the pod's manifest.

#### Adjust Resource Limits

Update the deployment definition to increase CPU or memory limits.

```yaml
resources:
  limits:
    cpu: "500m"
    memory: "512Mi"
  requests:
    cpu: "200m"
    memory: "256Mi"
```

**Action:** Apply the updated manifest.

### 2.2. Application Errors

Bugs or misconfigurations within the application itself can lead to crashes and restarts.

#### Review Application Logs (Detailed)

Utilize `kubectl logs` with the `-f` flag to stream logs in real-time during a restart cycle.

```bash
kubectl logs -f <pod-name> -n <namespace>
```
### 2.3. Liveness and Readiness Probes

Incorrectly configured liveness or readiness probes can cause healthy containers to be prematurely killed or marked as unready.

#### Examine Probe Configuration

Inspect the pod's manifest for `livenessProbe` and `readinessProbe` definitions.

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 15
  periodSeconds: 20

readinessProbe:
  httpGet:
    path: /readiness
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
```

#### Verify Probe Endpoint Reachability

If using HTTP probes, ensure the endpoint path and port are correct and the application exposes a healthy response. Use `curl` from within another pod in the same network to test the probe endpoint.

#### Adjust Probe Settings

Increase `initialDelaySeconds` to give the application more time to start up. Adjust `periodSeconds` or `timeoutSeconds` if the application takes longer to respond.

### 2.4. Image Pull Issues

The container might be restarting because it cannot pull the specified image.

#### Check Image Pull Errors

Look for events like `ErrImagePull` or `ImagePullBackOff` in `kubectl describe pod`.

#### Verify Image Name and Tag

Ensure the image name and tag in the deployment/pod manifest are correct and exist in the image registry.

#### Check Image Pull Secrets

If pulling from a private registry, ensure `imagePullSecrets` are correctly configured and have the necessary permissions.
### 2.5. Storage Issues

Problems with persistent storage can prevent applications from starting correctly.

#### Check Persistent Volume Claims (PVC) and Persistent Volumes (PV)

```bash
kubectl get pvc -n <namespace>
kubectl describe pvc <pvc-name> -n <namespace>
kubectl get pv
kubectl describe pv <pv-name>
```

Look for `Pending` or `Failed` status for PVCs or PVs.

#### Review Storage Class

Ensure the `storageClassName` in your PVC is correct and the underlying storage provisioner is functioning.

## 3. Advanced Debugging

### 3.1. Kernel and System Logs (Node Level)

Sometimes, restarts are due to issues on the underlying Kubernetes node.

#### Access Node Logs

SSH into the node where the problematic pod is scheduled.

```bash
journalctl -u kubelet -f # Kubelet logs
dmesg # Kernel messages
```

### 3.2. Pod Disruption Budgets (PDB)

In some cases, PDBs can cause pods to be evicted more aggressively during node maintenance.

#### Check PDBs

```bash
kubectl get pdb -n <namespace>
```

Ensure your application's PDBs are not overly restrictive.

## 4. Escalation and Monitoring

**Escalate to Team/SRE:** If the issue persists after following these steps, escalate to the appropriate team (e.g., SRE, development team) with all gathered information.

## 5. Post-Mortem and Documentation

### Document Findings
After resolving the issue, document the root cause, troubleshooting steps, and resolution in a shared knowledge base.

### Implement Preventative Measures
Based on the post-mortem, identify and implement measures to prevent similar issues in the future (e.g., improved resource requests, better probe configurations, code fixes).