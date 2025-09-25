# ADR 6: Backup Scheduler Architecture for VCP

## Context

The Volume Control Plane (VCP) is a critical component of our backup and recovery system. It requires a robust and scalable backup scheduling mechanism to ensure data integrity, operational efficiency, and compliance with SLAs (Service-Level Agreements). After evaluating the requirements and constraints, we have identified three potential options for implementing the scheduler:

## 1. Self-Managed Scheduler

### Implementation Details:
VCP will include an internal scheduler for managing backup operations. This scheduler will leverage Temporal, a workflow orchestration platform, to implement backup workflows with precise control over scheduling logic. Temporal provides features like retries, long-running workflows, and state persistence, making it ideal for complex scheduling needs.

### Advantages:
- **Control**: Full control over scheduling logic, enabling customization for business requirements, serialization for restore/delete operations, and granular state management.
- **No Hydration Needed**: Scheduled backups do not require hydration, reducing implementation complexity and saving costs.
- **Immutability**: Easy to implement immutability for ad-hoc backups with a minimum keep time of 2 days.

### Challenges:
- Higher resource usage due to the need to manage the scheduler within VCP.
- Increased implementation cost compared to other options.

## 2. ONTAP Policy Scheduler

### Implementation Details:
This approach relies on NetApp's ONTAP software for scheduling logic. The VSA (Volume Storage Appliance) team will manage the scheduling policies and push events to VCP.

### Advantages:
- Lower implementation cost due to reliance on existing ONTAP capabilities.
- Minimal development efforts required for scheduling logic.

### Challenges:
- Limited control over scheduling logic, requiring dependency on the VSA team for new features or bug fixes.
- Serialization on restore/delete operations remains unsolved.
- Hydration of scheduled backups is required, increasing complexity.

## 3. Google Managed Scheduler

### Implementation Details:
This approach leverages Google Cloud Scheduler, a fully managed service for scheduling jobs and automating backup workflows. Scheduling logic will be offloaded to Google's infrastructure, simplifying implementation.

### Advantages:
- Zero implementation cost; Google handles the scheduling infrastructure.
- No hydration of scheduled backups is required.
- Supports granular state management and immutability requirements.

### Challenges:
- No control over scheduling logic. Feature requests or bug fixes depend on Google Cloud's roadmap.
- Serialization on restore/delete operations requires Google to implement additional features.

## Comparison Table

| Feature | Self-Managed Scheduler | ONTAP Policy Scheduler | Google Managed Scheduler |
|---------|------------------------|------------------------|--------------------------|
| Management Control | Best control, easy to add features or fix bugs | Less control, VSA team dependency | No control |
| Serialization on Restore/Delete | Easy to implement in VCP | Unsolved, requires a solution | Google needs to implement |
| Implementation Cost | Highest cost, but no hydration needed | Low cost | Zero cost |
| Hydration of Scheduled Backups | No hydration needed | Hydration needed | No hydration needed |
| Granular State Management | Can be implemented | Not possible | Can be implemented |
| Immutability | Easy for ad-hoc backups (min keep time: 2 days) | Complicated (requires two policies) | Needs bucket-level changes (min keep time: 30 days) |

## Hydration of Scheduled Backups

Hydration ensures that the scheduled backups are correctly populated and executed. Two approaches are considered:

### Push from VSA to VCP:
- Event-based approach where VSA pushes backup events to VCP.
- Suitable for infrequent backup operations.

### Polling VSA from VCP:
- VCP continuously polls VSA for backup status.
- Less scalable and resource-intensive.

| Feature | Push from VSA to VCP | Polling VSA from VCP |
|---------|---------------------|---------------------|
| Scalability | Highly scalable | Scalable only with better design |
| Resource Usage | Low resource usage | High resource usage (polling 10,000+ VSAs) |
| Ease of Management | Less control, VSA team dependency | Full control on VCP side |
| Implementation Cost | Low cost | High cost |

## Decision

We will implement the **Self-Managed Scheduler** for VCP. This decision is based on the following factors:

- Provides the highest level of control over scheduling logic.
- Avoids the need for hydration of scheduled backups, reducing implementation complexity and cost.
- Enables easier implementation of features like serialization on restore/delete and granular state management.
- Supports immutability for ad-hoc backups with a minimum keep time of 2 days.

## Implementation Status

**Fully Implemented and Active** - The self-managed scheduler has been successfully implemented using Temporal workflows with comprehensive backup scheduling capabilities.

### Current Implementation Details

#### 1. Scheduled Backup Workflows
- **CreateScheduledBackupInitWorkflow**: Initializes scheduled backup workflows for backup policies with job tracking
- **CreateScheduledBackupWorkflow**: Handles individual scheduled backup creation with comprehensive error handling
- **DeleteScheduledBackupWorkflow**: Manages scheduled backup deletion with rollback capabilities

#### 2. Scheduled Backup Activities
- **ScheduledBackupActivity**: Core activities for scheduled backup operations
- **CreateScheduledBackup**: Creates scheduled backup records in the database with unique naming
- **GenerateScheduledSnapshotName**: Generates unique snapshot names with timestamps and random strings
- **HydrateCreatedBackupsToCCFE**: Sends backup information to CCFE for hydration (configurable via `GCP_HYDRATE_ENABLED`)
- **HydrateDeletedBackupsToCCFE**: Notifies CCFE of deleted backups
- **GetVolumesByBackupPolicyUUID**: Retrieves volumes associated with backup policies
- **FetchScheduledBackupForDeletion**: Fetches backups scheduled for deletion based on retention policies

#### 3. Backup Policy Integration
- Support for daily, weekly, and monthly backup schedules with configurable days
- Configurable backup retention policies through environment variables
- Automatic backup creation based on policy schedules with child workflow execution
- Integration with backup vault management and account-based policies

#### 4. Database Schema and Job Management
- **BackupPolicy**: Manages backup scheduling policies with account association
- **Backup**: Enhanced with `ScheduleTag` field for scheduled backups (daily/weekly/monthly)
- **Job**: Comprehensive job tracking for all scheduled backup operations
- **AdminJobSpec**: Manages cron expressions for scheduled jobs

#### 5. Advanced Features
- **Child Workflow Execution**: Uses Temporal child workflows for parallel backup creation
- **Rollback Management**: Comprehensive rollback capabilities with disconnected context execution
- **Error Handling**: Robust error handling with custom VSA error types
- **Retry Policies**: Configurable retry policies with non-retryable error types
- **CCFE Hydration**: Optional hydration to Cloud Control Frontend with token-based authentication

### Key Implementation Features

1. **Temporal Workflow Orchestration**: All scheduled backup operations are managed through Temporal workflows with proper job tracking
2. **CCFE Hydration**: Configurable hydration of scheduled backups to CCFE with authentication tokens
3. **Retry Policies**: Built-in retry mechanisms with configurable parameters and non-retryable error handling
4. **Rollback Management**: Comprehensive rollback capabilities with disconnected context execution for cleanup
5. **Multi-tier Backup Support**: Daily, weekly, and monthly backup tiers with configurable retention and scheduling
6. **Job Tracking**: Full job lifecycle management with status tracking and error reporting
7. **Child Workflow Pattern**: Parallel execution of backup operations using Temporal child workflows

## Status

**Accepted and Fully Implemented** - The self-managed scheduler is fully operational and handling scheduled backup operations in production with comprehensive monitoring and error handling.

## Consequences

### Positive
- Full control over scheduling logic, enabling faster feature development and bug fixes.
- No dependency on the VSA team or Google infrastructure for scheduling.
- No hydration of scheduled backups is required, reducing resource usage and costs.
- Supports granular state management and immutability requirements.
- **Achieved**: Temporal-based workflow orchestration provides robust scheduling capabilities with job tracking

### Negative
- **Realized**: Additional complexity in managing Temporal workflows and activities
- **Realized**: Need for comprehensive monitoring of scheduled backup operations
- **Realized**: Job tracking overhead for all scheduled backup operations

## Consequences for Future ADRs

This decision has been successfully implemented and establishes a solid foundation for:
- Advanced backup scheduling features (custom schedules, backup windows, timezone support)
- Integration with additional cloud providers and hyperscalers
- Enhanced monitoring and alerting capabilities for scheduled backups
- Backup policy automation and optimization based on usage patterns
- Integration with disaster recovery workflows and cross-region backup strategies
- Advanced retention policies with tiered storage and lifecycle management

The implementation has proven the viability of the self-managed scheduler approach and provides a scalable, robust platform for future backup-related features with comprehensive job tracking and error handling.