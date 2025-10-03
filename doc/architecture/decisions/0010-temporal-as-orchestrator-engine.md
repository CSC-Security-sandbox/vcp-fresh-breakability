# 6. Temporal as orchestrator engine

Date: 2025-08-06

## Status

Accepted

## Context

The VCP Control-Plane orchestrates customer-facing and internal workflows (Pool creation, Snapshot, Backup, Replication, etc.) that can run from seconds to several hours and must survive worker restarts, pod rescheduling, and regional fail-over. Rather than investing engineering resources in building and maintaining our own job orchestration framework, which would require significant ongoing maintenance and feature development, we decided to evaluate battle-tested third-party solutions. The platform therefore requires:

* Exactly-once execution semantics with durable state persistence
* Built-in retry / back-off and activity heart-beating
* Deterministic workflow re-play for safe upgrades
* First-class visibility (UI, CLI, metrics)
* Go SDK with type-safe APIs and context propagation
* Deployment under our own GKE clusters to avoid cloud lock-in

An engineering spike evaluated several workflow orchestration options:

| Solution | Key Features | Limitations | 
|----------|--------------|-------------|
| **Temporal** | • Exactly-once execution guarantee<br/>• Durable state persistence<br/>• Native retry/backoff/heartbeat<br/>• Deterministic replay for versioning<br/>• Rich UI, CLI, metrics<br/>• Strong Go SDK support<br/>• Self-hosted with PostgreSQL | • Operational complexity (server + DB)<br/>• Learning curve for versioning patterns<br/>• Requires careful retry configuration |
| Uber Cadence | • Similar feature set to Temporal<br/>• Production-proven at Uber<br/>• Strong workflow versioning | • Smaller community<br/>• Less active development<br/>• Limited cloud-native features |
| Netflix Conductor | • Proven at Netflix scale<br/>• JSON-based workflow DSL<br/>• Good documentation | • Limited Go SDK maturity<br/>• Complex server deployment<br/>• Smaller community vs Temporal |
| Apache Airflow | • Rich ecosystem of operators<br/>• Strong scheduling capabilities<br/>• Large community | • DAG-focused vs workflow-focused<br/>• Python-centric architecture<br/>• High resource requirements |
| Dapr Workflow | • Cloud-native design<br/>• Multi-runtime support<br/>• Kubernetes-native | • Early project maturity<br/>• Limited production usage<br/>• Basic workflow patterns only |
| Argo Workflows | • Native Kubernetes integration<br/>• YAML-based workflow spec<br/>• Container-per-task model | • No durable execution guarantee<br/>• Limited state persistence<br/>• Container startup overhead |
| Google Cloud Workflows | • Fully managed service<br/>• Native GCP service auth<br/>• Pay-per-execution | • GCP platform lock-in<br/>• Basic retry mechanics only<br/>• Limited observability |
| Custom Scheduler | • Full control over implementation<br/>• No external dependencies<br/>• Simplest deployment | • High development/maintenance cost<br/>• Must build retry/state/monitoring<br/>• Risk of bugs in critical paths |

Temporal was selected as it uniquely satisfied all reliability and determinism requirements while enabling self-hosting.

## Decision

Adopt **Temporal** as the orchestration engine for all long-running control-plane workflows. This open-source durable execution platform will simplify building scalable and reliable distributed systems by handling workflow execution, state management, and worker coordination.

Key points of the adoption:

1. **Workflow Implementation**:
    - Implement all VSA Control Plane APIs as Temporal workflows
    - Delegate execution steps to Temporal engine, allowing control plane to focus on core business logic
    - Ensure reliable execution with built-in state persistence and failure handling

2. **Queue Management**:
    - Define priority-based task queues for different workflow types
    - Separate queues for CRUD operations vs scheduled operations (e.g., backups)
    - Enable efficient execution and prioritization of operations

3. **Worker Architecture**:
    - Deploy scalable worker pools per task queue
    - Dynamic worker scaling based on queue depth and performance requirements
    - Package workers in independent images (`vcp-worker`, `vlm-worker`) using Temporal Go SDK

4. **Infrastructure Setup**:
    - Deploy Temporal server via official `temporal-helm` chart in `vsa-control-plane` GKE namespace
    - Use PostgreSQL for persistence and optional Elasticsearch for visibility
    - Create dedicated namespace `vcp` for all workflow executions

5. **Operational Standards**:
    - Standardize timeouts and retry policies (`StartToCloseTimeout` 10m, heartbeat 1m)
    - Manage versioning through task-queue "rainbow" deployments
    - Plan migration to Build-ID based versioning with Temporal ≥ 1.21

## Consequences

**Pros**

* Durable, exactly-once workflow execution with automatic state recovery.
* Built-in back-off, cron, heart-beating and signal/query APIs remove custom scheduler code.
* Deterministic replay enables safe zero-downtime upgrades.
* Go SDK aligns with existing stack and supports local unit-testing.
* Comprehensive observability features:
    - Built-in Prometheus metrics for workflow/activity latency and queue depth
    - OpenTelemetry integration for distributed tracing
    - Web UI for workflow history visualization and debugging
    - Structured logging with correlation IDs
* Strong operational capabilities:
    - Helm charts for automated deployment and upgrades
    - Build-ID based versioning for safe rollouts
    - Automated backup and recovery procedures
    - Monitoring and alerting on key SLIs/SLOs

**Cons / Risks**

* Additional operational surface area: we must operate Temporal server, PostgreSQL, and (optionally) Elasticsearch.
* Workflow code must remain backwards-compatible with in-flight histories; engineers require training on `GetVersion`, queue versioning and Build-ID routing.
* Mis-configured retry policies can amplify downstream outages; guardrails and alerts are needed.
* Potential task-queue sprawl if deprecated queues are not pruned after migrations.

