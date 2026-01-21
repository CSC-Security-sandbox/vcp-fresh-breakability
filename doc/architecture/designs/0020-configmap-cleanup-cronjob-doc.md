# ConfigMap Cleanup CronJob - Detailed Documentation

## Overview

The ConfigMap Cleanup CronJob is an automated Kubernetes CronJob that runs on a scheduled basis to identify and remove unused vcp-worker ConfigMaps from the cluster. This job helps maintain cluster hygiene by automatically cleaning up orphaned ConfigMaps that are no longer referenced by any active workloads, while implementing multiple safety mechanisms to prevent accidental deletion of active resources.

The CronJob operates independently of Helm install/upgrade operations and runs on a configurable schedule (default: weekly on Sundays at 2:00 AM). It performs comprehensive checks across all workload types and running pods to ensure that only truly unused ConfigMaps are deleted.

## Architecture and Components

### Service Account

The CronJob uses a Kubernetes ServiceAccount to authenticate and authorize its operations. We are using the existing service account vcp-worker-ksa.

### RBAC (Role-Based Access Control)

The CronJob requires specific Kubernetes API permissions to perform its cleanup operations. These permissions are granted through a Kubernetes Role and RoleBinding that are conditionally created based on the `configMapCleanup.rbac.enabled` setting in `values.yaml`.

#### Role: `configmap-cleanup-role`

The Role defines the following permissions:

1. **ConfigMaps** (core API group):
   - `get`: Retrieve individual ConfigMap details
   - `list`: List all ConfigMaps in the namespace
   - `delete`: Delete ConfigMaps that are identified as unused

2. **Pods** (core API group):
   - `get`: Retrieve individual Pod details
   - `list`: List all Pods in the namespace
   - These permissions are required to check which ConfigMaps are currently in use by running Pods

3. **Deployments, StatefulSets, DaemonSets** (apps API group):
   - `get`: Retrieve individual workload resource details
   - `list`: List all workload resources in the namespace
   - These permissions are required to check which ConfigMaps are referenced by active workloads

4. **Jobs, CronJobs** (batch API group):
   - `get`: Retrieve individual Job/CronJob details
   - `list`: List all Jobs and CronJobs in the namespace
   - These permissions are required to check which ConfigMaps are referenced by batch workloads

The Role is defined in `templates/rbac.yaml` (lines 24-43) and is only created when `configMapCleanup.rbac.enabled` is set to `true`.

#### RoleBinding: `configmap-cleanup-rolebinding`

The RoleBinding associates the `configmap-cleanup-role` Role with the service account specified in `serviceAccount.name`. This binding grants the CronJob pod the necessary permissions to perform cleanup operations within the namespace scope.

The RoleBinding is defined in `templates/rbac.yaml` (lines 45-57) and is conditionally created alongside the Role.

## Configuration (values.yaml)

All configuration for the ConfigMap cleanup CronJob is defined in the `configMapCleanup` section of `values.yaml`. The configuration is organized into two main sections: general cleanup settings and CronJob-specific settings.

### General Configuration

```yaml
configMapCleanup:
  rbac:
    enabled: true  # Controls creation of Role and RoleBinding
  pattern: "^(vcp-background-worker|vcp-customer-worker).*-config$"
  backoffLimit: 2
  activeDeadlineSeconds: 300
```

- **`rbac.enabled`**: Boolean flag that controls whether the RBAC Role and RoleBinding are created. When set to `false`, the RBAC resources are not created, and the CronJob will fail if it doesn't have permissions from another source. Default: `true`

- **`pattern`**: Regular expression pattern used to identify which ConfigMaps should be considered for cleanup. The default pattern `^(vcp-background-worker|vcp-customer-worker).*-config$` matches ConfigMaps that:
  - Start with either `vcp-background-worker` or `vcp-customer-worker`
  - End with `-config`
  - Have any characters in between (typically version information)
  - Examples of matching ConfigMaps:
    - `vcp-customer-worker-v0-1-0-config`
    - `vcp-background-worker-v0-2-0-config`
    - `vcp-customer-worker-v1-5-3-config`

- **`backoffLimit`**: Number of times Kubernetes will retry the job if it fails. This applies to both the CronJob's job template and any retry logic. Default: `2`

- **`activeDeadlineSeconds`**: Maximum time in seconds that the job is allowed to run before being terminated. This prevents jobs from running indefinitely. Default: `300` (5 minutes)

### CronJob-Specific Configuration

```yaml
configMapCleanup:
  cronJob:
    enabled: true
    schedule: "0 2 * * 0"
    successfulJobsHistoryLimit: 3
    failedJobsHistoryLimit: 3
    concurrencyPolicy: "Forbid"
    backoffLimit: 2
    activeDeadlineSeconds: 300
    imageSHA: "sha256:ae4bb851c280dae99fc2f0ca15eb7719a92721f2c13d757e63bf6e2e557a8cfc"
```

- **`enabled`**: Boolean flag that controls whether the CronJob is created. When set to `false`, the CronJob resource is not deployed. Default: `true`

- **`schedule`**: Cron expression defining when the job should run. The format follows standard cron syntax: `minute hour day-of-month month day-of-week`. The default `"0 2 * * 0"` means:
  - Minute: `0` (at the top of the hour)
  - Hour: `2` (at 2:00 AM)
  - Day of month: `*` (every day)
  - Month: `*` (every month)
  - Day of week: `0` (Sunday)
  - Result: Every Sunday at 2:00 AM

- **`successfulJobsHistoryLimit`**: Number of successful job execution records to retain in the CronJob's history. This helps with debugging and monitoring. Default: `3`

- **`failedJobsHistoryLimit`**: Number of failed job execution records to retain in the CronJob's history. This helps with troubleshooting. Default: `3`

- **`concurrencyPolicy`**: Defines how the CronJob handles concurrent executions. Options:
  - `"Forbid"` (default): If a previous job is still running, skip the new scheduled execution
  - `"Allow"`: Allow multiple jobs to run concurrently
  - `"Replace"`: Cancel the running job and start a new one
  - The default `"Forbid"` policy prevents overlapping executions and potential race conditions

- **`backoffLimit`**: Same as the general `backoffLimit`, but specific to the CronJob's job template. Default: `2`

- **`activeDeadlineSeconds`**: Same as the general `activeDeadlineSeconds`, but specific to the CronJob's job template. Default: `300` (5 minutes)

- **`imageSHA`**: The SHA256 digest of the container image to use for the cleanup job. The image used is a kubectl-based image that provides the `kubectl` command-line tool for interacting with the Kubernetes API. Default: `"sha256:ae4bb851c280dae99fc2f0ca15eb7719a92721f2c13d757e63bf6e2e557a8cfc"`

- **`startingDeadlineSeconds`** (optional): If a CronJob misses its scheduled time (e.g., due to cluster maintenance), this setting defines how long after the scheduled time the job can still start. If not specified, the job will not start if it misses its scheduled window.

## How ConfigMaps Are Collected

The cleanup process begins by identifying all ConfigMaps in the target namespace that match the configured pattern. This is accomplished through a multi-step process:

### Step 1: Pattern Matching

The CronJob uses `kubectl get configmaps` to retrieve all ConfigMaps in the namespace, then filters them using the regex pattern specified in `configMapCleanup.pattern`. The command executed is:

```bash
kubectl get configmaps -n <namespace> -o jsonpath='{.items[*].metadata.name}' | \
  tr ' ' '\n' | \
  grep -E '<pattern>' | \
  sort -u
```

This process:
1. Retrieves all ConfigMaps as JSON and extracts their names
2. Converts the space-separated list into newline-separated entries
3. Filters using grep with extended regex (`-E`) to match the pattern
4. Sorts and removes duplicates (`sort -u`)

If no ConfigMaps match the pattern, the job exits successfully with a message indicating no matching ConfigMaps were found.

### Step 2: Active ConfigMap Detection

After identifying candidate ConfigMaps, the job determines which ones are actively in use by examining all workload resources and running pods. This is the most critical safety mechanism to prevent deletion of active ConfigMaps.

#### Workload Resource Checking

The job queries all workload resources in the namespace:
- Deployments
- StatefulSets
- DaemonSets
- Jobs
- CronJobs

For each workload resource, the job extracts ConfigMap references from multiple locations:

1. **Container `envFrom.configMapRef`**: ConfigMaps referenced via `envFrom` in container specifications
2. **Container `env.valueFrom.configMapKeyRef`**: ConfigMaps referenced via individual environment variables
3. **InitContainer `envFrom.configMapRef`**: ConfigMaps referenced in init containers
4. **Volumes `configMap`**: ConfigMaps mounted as volumes

The extraction is performed using `jq` to parse the JSON output from `kubectl get`:

```bash
kubectl get deployments,statefulsets,daemonsets,jobs,cronjobs -n <namespace> -o json | \
  jq -r '.items[]? | .spec.template.spec.containers[]? | .envFrom[]? | .configMapRef.name // empty'
```

Similar queries are executed for each reference type (envFrom, env.valueFrom, initContainers, volumes) and the results are combined into a single list of active ConfigMap names.

#### Running Pod Checking

In addition to workload resources, the job also checks currently running Pods to catch ConfigMaps that might be in use but not yet reflected in workload specifications (e.g., during rolling updates or pod restarts). The same extraction logic is applied to Pod resources:

```bash
kubectl get pods -n <namespace> -o json | \
  jq -r '.items[]? | .spec.containers[]? | .envFrom[]? | .configMapRef.name // empty'
```

The results from both workload resources and running pods are merged, deduplicated, and sorted to create a comprehensive list of actively referenced ConfigMaps.

#### Safety Verification

Before proceeding with deletions, the job performs a critical safety check: if ConfigMaps are found but no active references are detected, and there are workloads or pods present in the namespace, the job aborts with an error. This prevents accidental deletion in cases where the parsing logic might have failed silently.

## Grace Period Mechanism

To prevent deletion of recently created ConfigMaps that might not yet be referenced by workloads (e.g., during deployment transitions), the job implements a grace period of 24 hours (86,400 seconds).

### How It Works

For each ConfigMap identified as unused (not in the active list), the job:

1. Retrieves the ConfigMap's `metadata.creationTimestamp` field
2. Converts the RFC3339 timestamp to a Unix timestamp
3. Calculates the age of the ConfigMap in seconds
4. Compares the age against the grace period threshold

If the ConfigMap was created within the last 24 hours, it is skipped with a message indicating it's within the grace period. The age is displayed in a human-readable format (hours and minutes) for logging purposes.

### Grace Period Logic

```bash
GRACE_PERIOD_SECONDS=86400  # 24 hours
CURRENT_TIMESTAMP=$(date +%s)
CREATION_TIMESTAMP=$(kubectl get configmap <name> -n <namespace> -o jsonpath='{.metadata.creationTimestamp}')
CREATION_UNIX=$(date -d "$CREATION_TIMESTAMP" +%s)
AGE_SECONDS=$((CURRENT_TIMESTAMP - CREATION_UNIX))

if [ "$AGE_SECONDS" -lt "$GRACE_PERIOD_SECONDS" ]; then
  # Skip deletion - within grace period
fi
```

### Safety Considerations

If the creation timestamp cannot be parsed or is missing, the job takes a conservative approach and skips deletion of that ConfigMap. This ensures that ConfigMaps with unparseable timestamps are never accidentally deleted.

## Deletion Process

Once a ConfigMap has been identified as:
1. Matching the pattern
2. Not referenced by any active workload or pod
3. Older than the grace period (24 hours)

The job proceeds with deletion using `kubectl delete configmap` with the `--ignore-not-found=true` flag to handle cases where the ConfigMap might have been deleted by another process.

### Verification

After attempting deletion, the job:
1. Waits 1 second for the deletion to propagate
2. Verifies the ConfigMap no longer exists
3. Logs the result (success or failure)

If a ConfigMap still exists after deletion (e.g., due to finalizers), it is logged as a warning and tracked in an error file, but the job continues processing other ConfigMaps.

## Safety Mechanisms

The CronJob implements multiple layers of safety to prevent accidental deletion of active ConfigMaps:

### 1. Active Reference Checking

The most important safety mechanism is the comprehensive checking of all workload types and running pods. A ConfigMap is only considered for deletion if it is not referenced anywhere in:
- Deployment specifications
- StatefulSet specifications
- DaemonSet specifications
- Job specifications
- CronJob specifications
- Running Pod specifications

This check covers all reference types:
- `envFrom.configMapRef`
- `env.valueFrom.configMapKeyRef`
- `volumes.configMap`
- References in both regular containers and init containers

### 2. Grace Period

The 24-hour grace period ensures that recently created ConfigMaps are never deleted, even if they appear unused. This protects ConfigMaps during:
- Deployment transitions
- Rolling updates
- Temporary workload unavailability

### 3. Critical Safety Check

If ConfigMaps are found but no active references are detected, and workloads/pods exist in the namespace, the job aborts with an error. This prevents deletion in cases where the parsing logic might have failed.

### 4. Triple Verification

Before deleting a ConfigMap, the job performs three checks:
1. Verifies the ConfigMap still exists (might have been deleted by another process)
2. Re-checks that it's not in the active list (double verification)
3. Confirms it's outside the grace period

### 5. Error Handling

The job is designed to be non-blocking. If some ConfigMaps cannot be deleted (e.g., due to finalizers), the job logs warnings but continues processing other ConfigMaps and exits with success. This ensures the CronJob continues to run on schedule even if some deletions fail.

### 6. Concurrency Policy

The default `concurrencyPolicy: "Forbid"` ensures that only one instance of the cleanup job runs at a time, preventing race conditions and overlapping operations.

## Execution Workflow

The cleanup job follows this step-by-step workflow:

1. **Initialization**: Logs job start with namespace and schedule information

2. **ConfigMap Discovery**: Finds all ConfigMaps matching the pattern and logs the count

3. **Active Reference Detection**: 
   - Queries all workload resources (Deployments, StatefulSets, DaemonSets, Jobs, CronJobs)
   - Queries all running Pods
   - Extracts ConfigMap references from all sources
   - Merges and deduplicates the active ConfigMap list

4. **Safety Verification**: Performs critical safety check to ensure parsing succeeded

5. **Deletion Processing**: For each candidate ConfigMap:
   - Checks if it's in the active list (skip if active)
   - Verifies it still exists
   - Checks grace period (skip if within 24 hours)
   - Attempts deletion
   - Verifies deletion succeeded

6. **Summary Reporting**: Calculates and logs:
   - Total ConfigMaps found
   - Active ConfigMaps (kept)
   - ConfigMaps deleted
   - ConfigMaps skipped
   - ConfigMaps with errors

7. **Final Verification**: Lists remaining ConfigMaps matching the pattern to confirm cleanup results

8. **Completion**: Logs completion status and exits

## Resource Limits

The CronJob container has resource limits configured to prevent excessive resource consumption:

- **CPU Limits**: `200m` (0.2 CPU cores)
- **CPU Requests**: `100m` (0.1 CPU cores)
- **Memory Limits**: `512Mi` (512 megabytes)
- **Memory Requests**: `256Mi` (256 megabytes)

These limits ensure the cleanup job doesn't impact other workloads in the cluster.



