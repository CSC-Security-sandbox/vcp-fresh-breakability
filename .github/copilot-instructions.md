# VSA Control Plane Development Guide

## Architecture Overview

This is a **VSA (Virtual Storage Appliance) Control Plane** - a microservices-based system managing the lifecycle of NetApp ONTAP storage systems on cloud hyperscalers (primarily Google Cloud). The architecture follows a **workflow-driven approach** using Temporal for orchestrating complex, long-running storage operations.

### System Context
The VSA Control Plane (VCP) is NetApp's **lightweight regional control plane** that integrates with Google Cloud NetApp Volumes (GCNV) to deliver unified storage support (block + file protocols) using VSA backend clusters. It coexists with the legacy SDE (Service Delivery Engine) but provides a cleaner architecture for VSA-based resources without the complexities of multi-tenant hardware cluster management.

### Key Components
- **Core API** (`core/`): Hyperscaler-agnostic business logic and orchestration
- **Google Proxy** (`google-proxy/`): GCP-specific operations and resource management  
- **Telemetry** (`telemetry/`): Metrics collection and performance monitoring
- **Worker** (`worker/`): Temporal workflow worker executing background tasks
- **ONTAP Proxy** (`ontap-proxy/`): NetApp ONTAP API interactions

### Data Flow & Integration
VCP operates as a **regional entity** that receives requests from Google's CCFE/CLH through a forwarding proxy. It manages VSA clusters deployed in regional tenant projects using GCE VMs and Hyperdisk storage running NetApp ONTAP. The control plane handles both synchronous operations and long-running workflows (LROs) with polling mechanisms.

## Critical Architectural Patterns

### Workflow-First Design
All complex operations are implemented as **Temporal workflows** in `core/orchestrator/workflows/`. Each workflow is paired with activities in `core/orchestrator/activities/`.

```go
// Workflow pattern: workflows define the business logic flow
func (wf *BackupCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
    err := workflow.ExecuteActivity(ctx, backupActivity.PrepareObjectStoreActivity, context).Get(ctx, &context)
    // Activities are the actual work units
}
```

### Cloud Abstraction Boundary
**Critical Rule**: The `core/` module MUST NOT import cloud-specific packages. Cloud operations go through the hyperscaler abstraction layer.

```go
// ❌ Never in core/
import "cloud.google.com/go/storage"

// ✅ Use abstraction
import "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
```

### Database Architecture
Uses **PostgreSQL** as the primary database with a hybrid data model:
- **Relational data** for hierarchical resources (Account → Pool → Volume → Snapshot)
- **JSONB columns** for semi-structured data (VolumeAttributes, PoolAttributes)
- **Strong ACID properties** for data consistency in concurrent environments
- **Temporal workflow persistence** for workflow state and history

### Custom Error System
Use the structured error system from `core/errors/` with categorized error codes:
- 1000-1999: Workflow errors
- 2000-2999: Database errors  
- 3000-3999: GCP/Cloud errors
- 4000-4999: VSA Cluster errors
- 5000-5999: ONTAP errors

```go
return vsaerrors.NewVCPError(vsaerrors.ErrVolumeCreationFailed, err)
```

## Resource Management Patterns

### VSA Cluster Lifecycle
- **Storage Pool → VSA Cluster mapping**: Each GCNV Storage Pool is backed by a VSA cluster (2-node HA pair)
- **Serial Number Generation**: 20-digit globally unique serial numbers with region-specific counters
- **VMRS (Virtual Machine Right Sizing)**: Performance-based VM selection from configuration tables

### Networking Architecture
VSA clusters use **multiple vNICs** for different purposes:
- **vNIC0**: Management interface, cluster communication, GCS traffic (internal IPs: 198.18.0.0/x)
- **vNIC1**: HA/Interconnect interface (internal IPs: 198.18.128.0/x)
- **vNIC2**: Health checks, RSM for regional clusters (internal IPs: 198.18.192.0/x)
- **vNIC3**: Data LIFs for customer protocols (customer-provided PSA IP range)

### Storage Integration
- **Backup**: GCS buckets per BackupVault with object store configuration and SnapMirror-to-cloud
- **Auto-tiering**: Per-pool GCS buckets for cold data with workload identity authentication
- **Replication**: Inter-cluster LIFs for SnapMirror traffic between regions

## Architectural Decision Records (ADRs)

### Database Technology Choice
**PostgreSQL** was selected over other database options for specific architectural reasons:
- **ACID Compliance**: Strong consistency guarantees essential for storage operations
- **JSONB Support**: Hybrid data model supporting both relational and semi-structured data
- **Hyperscaler Agnostic**: Available across all cloud providers without vendor lock-in
- **Temporal Compatibility**: Native support for Temporal workflow persistence
- **Operational Maturity**: Well-understood operational characteristics for distributed systems

### VMRS (Virtual Machine Right Sizing)
**Single HA-Pair Design**: Each GCNV Storage Pool maps to exactly one VSA cluster (2-node HA pair):
- **Cost Optimization**: Eliminates over-provisioning of large multi-pool clusters
- **Resource Efficiency**: VM size selection based on performance requirements from VMRS tables
- **Simplified Management**: Reduces operational complexity compared to multi-tenant clusters
- **Configuration**: VMRS tables in `/config/vmrs_gcp.yaml` define VM-to-performance mappings

### Serial Number Generation Strategy
**20-Digit Globally Unique Identifiers**:
- **Format**: `{region_code}{4-digit-counter}{14-digit-timestamp}`
- **Global Uniqueness**: Region-specific counters prevent collisions across deployments
- **Traceability**: Embedded timestamp and region for operational tracking
- **Implementation**: Counter management through database sequences with region isolation

## Development Workflows

## Documentation Standards

### PlantUML Diagrams
Architecture diagrams use **PlantUML** with specific conventions:
- **C4 Model**: Context, Container, Component, and Code diagrams
- **File Location**: All diagrams in `doc/architecture/` with `.puml` extensions
- **Rendering**: Use PlantUML online service or local installation for viewing
- **Standards**: Follow C4 model conventions for system boundaries and relationships

```plantuml
@startuml
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml

Person(customer, "Storage Customer", "Requires persistent storage")
System(gcnv, "Google Cloud NetApp Volumes", "Managed storage service")
System(vcp, "VSA Control Plane", "Storage lifecycle management")

Rel(customer, gcnv, "Provisions storage")
Rel(gcnv, vcp, "Delegates VSA operations")
@enduml
```

### Local Development Setup
```bash
# Required environment variables
export GHVSA_PAT=$(gh auth token)
export DB_PASSWORD=<password>
export VSA_NODE_PASSWORD=<ontap-password>

# Build and run locally with Skaffold
make build-all-binaries-dev skaffold-dev
```

### Testing Patterns
- **Workflow Tests**: Use `testsuite.WorkflowTestSuite` with extensive mocking of activities
- **Mock Setup**: Always provide fully populated `BackupActivitiesContext` to avoid nil pointer issues
- **Activity Tests**: Mock database (`database.NewMockStorage(t)`) and hyperscaler providers

```go
// Critical: Populate context completely in tests
env.OnActivity("SomeActivity", mock.Anything, mock.Anything).Return(&activities.BackupActivitiesContext{
    BackupWorkflowInit: &activities.BackupWorkflowInput{
        Backup: backup, BackupVault: backupVault, Volume: volume,
    },
    Node: &models.Node{EndpointAddress: "127.0.0.1"},
}, nil)
```

### Code Generation Commands
```bash
make generate-mocks          # Generate all mocks using mockery
make generate-core-api       # Generate OpenAPI server from core/core-api/api.yaml
make generate-google-proxy   # Generate from google-proxy/api/gcp-api.yaml
make test                    # Run tests with coverage
```

## Data Model Patterns

### GORM Patterns
Models extend `BaseModel` with standard fields (ID, UUID, CreatedAt, UpdatedAt, DeletedAt):

```go
type Volume struct {
    BaseModel
    VolumeAttributes *VolumeAttributes `gorm:"column:volume_attributes;type:jsonb"`
    Pool             *Pool             `gorm:"ForeignKey:PoolID"`
}
```

### Activity Context Pattern
Workflows pass context between activities using `BackupActivitiesContext` structures:

```go
type BackupActivitiesContext struct {
    BackupWorkflowInit     *BackupWorkflowInput
    Node                   *models.Node
    SnapshotName          string
    TransferStatus        SmStatus
    // ... other fields accumulated during workflow
}
```

## Integration Points

### Temporal Integration
- Workflows are registered in `workflow_engine/temporal/`
- Use `workflow.ExecuteActivity()` for all external operations
- Activities must be idempotent and retryable
- Use `ConvertToVSAError()` to wrap errors for Temporal

### Database Layer
- Use `database.Storage` interface for all DB operations
- GORM with PostgreSQL backend
- Migrations in `database/vcp/migrations/`
- Always use context for cancellation and tracing

### ONTAP Integration
- NetApp ONTAP operations through `core/vsa/` interfaces
- Provider pattern in `hyperscaler/` for cloud-specific implementations
- Mock providers available for testing

## Project-Specific Conventions

### Import Organization
```go
import (
    // Standard library first
    "context"
    "fmt"
    
    // Third-party and local second (alphabetical)
    "github.com/stretchr/testify/mock"
    "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)
```

### Naming Conventions
- Workflows: `CreateVolumeWorkflow`, `BackupCreateWorkflow`
- Activities: `PrepareObjectStoreActivity`, `UpdateSnapshotActivity`  
- Test mocks: `TestBackupActivity` with overridden methods
- Error codes: `ErrVolumeCreationFailed`, `ErrDatabaseConnectionError`

### Key Files for Reference
- `core/orchestrator/workflows/backup_workflow.go` - Complex workflow example
- `core/orchestrator/activities/backup_activities.go` - Activity patterns
- `core/datamodel/models.go` - Data model patterns
- `CODING_GUIDELINES.md` - Comprehensive coding standards
- `core/errors/README.md` - Error handling system documentation

## Common Pitfalls
- Never use empty `BackupActivitiesContext{}` in tests - always populate required fields
- Temporal activity mocks need exact parameter counts (including context)
- Cloud-specific code belongs in `hyperscaler/` or service-specific directories, not `core/`
- Use `mock.Anything` parameters matching actual function signatures including context
