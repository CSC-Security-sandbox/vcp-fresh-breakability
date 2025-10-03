# Temporal Debugging Guide

This guide provides concise operational guidance for debugging Temporal workflows used by the VSA Control Plane. It covers common tctl commands, using the Temporal Web UI, typical troubleshooting patterns, and safe operator actions.

## Prerequisites
- kubectl configured for the target cluster and namespace
- tctl available locally or accessible from the admintools pod
- The control plane's Temporal namespace (placeholder: `<vcp-namespace>`)

## Common Concepts
- Workflow ID == job.job_id in the control plane database
- Run ID identifies a specific workflow execution run

## Inspecting Workflows with tctl
- List running workflows (filter by type):

  ```bash
  tctl --namespace <vcp-namespace> workflow list --query 'WorkflowType="CreateVolumeWorkflow"'
  ```

- Describe a workflow execution:

  ```bash
  tctl --namespace <vcp-namespace> workflow describe --workflow-id <workflow-id> [--run-id <run-id>]
  ```

- Show workflow history (events, failures):

  ```bash
  tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow-id> [--run-id <run-id>]
  ```

- Query a workflow (if a query handler exists):

  ```bash
  tctl --namespace <vcp-namespace> workflow query --workflow-id <workflow-id> --query-type "status"
  ```

- Signal a workflow:

  ```bash
  tctl --namespace <vcp-namespace> workflow signal --workflow-id <workflow-id> --signal-name <SignalName> --input '"payload"'
  ```

- Terminate / cancel (operator-only, use caution):

  ```bash
  tctl --namespace <vcp-namespace> workflow terminate --workflow-id <workflow-id> --reason "reason"
  tctl --namespace <vcp-namespace> workflow cancel --workflow-id <workflow-id>
  ```

## Temporal Web UI
- Port-forward locally:

  ```bash
  kubectl -n <temporal-namespace> port-forward svc/temporal-web 8088:8088
  ```
  Open http://localhost:8088 and select the `<vcp-namespace>` namespace.

- Use the UI to step through history events and inspect activity failures and inputs.

## Using admintools in Restricted Clusters
In restricted environments, run tctl from the admintools pod to avoid granting broad kubectl access:

  ```bash
  kubectl -n <ops-namespace> exec -it deploy/admintools -- /bin/sh -c "tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow-id>"
  ```

## Troubleshooting Patterns
- Activity retries but keeps failing:
  1. Inspect the last error in workflow history.  
  2. Check worker logs for stack traces.  
  3. Verify idempotency of side effects.

- Workflow stuck on heartbeat timeout:
  - Confirm activity heartbeats and worker clocks (NTP).  
  - Increase heartbeat timeout if appropriate.

- Duplicate operations / double side-effects:
  - Ensure activities are idempotent and dedup tokens are used.  
  - Inspect DB job rows for duplicate job ids.

- Workflow not found but Job row exists:
  - Check orchestrator logs for startup failures.  
  - Search Temporal history across run IDs.

## Logs and Correlation
- Worker logs: `kubectl -n <ns> logs deploy/vcp-worker -c worker`  
- Orchestrator logs: `kubectl -n <ns> logs deploy/core-api -c core`  
- Correlate logs with workflow history using workflow-id/job-id and timestamps.

## Safe Operator Actions
- Prefer `describe` and `show` before terminating or resetting workflows.  
- Capture history and logs before performing destructive actions.  
- Document reasons for resets/terminations in incident notes.

## Quick Runbook Snippets
- Show workflow history from admintools:

  ```bash
  kubectl -n <ops-namespace> exec -it deploy/admintools -- /bin/sh -c "tctl --namespace <vcp-namespace> workflow show --workflow-id <workflow-id>"
  ```

- Port-forward Temporal UI:

  ```bash
  kubectl -n <temporal-namespace> port-forward svc/temporal-web 8088:8088
  ```

- Tail worker logs for a given timeframe:

  ```bash
  kubectl -n <ns> logs deploy/vcp-worker --since=2h | grep <workflow-id> -C 5
  ```

## Further Reading
- Temporal docs: https://docs.temporal.io
- tctl reference: https://docs.temporal.io/docs/system-tools/tctl

---

(If you want, I can expand this guide with automated helper scripts or an admintools wrapper to simplify common tctl commands.)
