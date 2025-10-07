# Pool ZI/ZS Compliance Design

## Context

The VSA Control Plane needs to track and synchronize Zone Independence (ZI) and Zone Separation (ZS) compliance metrics for pools. These compliance requirements ensure that cloud resources in pools meet specific architectural requirements for high availability, fault tolerance, and security isolation. The system needs to:

- Track compliance status for each pool (satisfy_zi, satisfy_zs)
- Store asset metadata about child resources associated with pools
- Sync compliance data periodically from VLM (Virtual Lifecycle Management) service
- Trigger compliance checks during pool creation
- Provide REST API endpoints to retrieve compliance information

## Decision

We implement a comprehensive ZI/ZS compliance tracking system with the following components:

### 1. Data Model Extensions

**Pool Model Enhancements:**
- Add `SatisfiesPzi` (boolean) field to track Zone Independence compliance
- Add `SatisfiesPzs` (boolean) field to track Zone Separation compliance 
- Add `AssetMetadata` (JSONB) field to store child asset information

**Database Schema Changes:**
```sql
-- Migration 0008: Add compliance columns
ALTER TABLE pools ADD COLUMN satisfy_zi BOOLEAN DEFAULT false;
ALTER TABLE pools ADD COLUMN satisfy_zs BOOLEAN DEFAULT false;

-- Migration 0009: Add asset metadata column
ALTER TABLE pools ADD COLUMN asset_metadata JSONB;
CREATE INDEX idx_pools_asset_metadata ON pools USING GIN (asset_metadata);
```

### 2. API Integration Design

**VLM Workflow Integration:**
- New VLM workflow `GetClusterZiZsDetails` for compliance data retrieval
- Request/Response structures defined for GCP resource information:
```go
type GetResourceInfoReq struct {
    ProjectID    string `json:"project_id"`
    DeploymentID string `json:"deployment_id"`
}

type GetResourceInfoResp struct {
    ProjectID    string              `json:"project_id"`
    DeploymentID string              `json:"deployment_id"`
    ResourceInfo ResourceInformation `json:"resource_info"`
}
```

### 3. Workflow Architecture

**Temporal Workflow Hierarchy:**
1. **SyncPoolZIZSDetailsWorkflow** - Main background workflow for all pools
2. **SyncPoolComplianceForPoolWorkflow** - Per-pool compliance sync workflow
3. **Background Job Scheduler** - Periodic execution via cron

**Workflow Execution Flow:**
- Background scheduler triggers every hour (`0 * * * *`)
- Fetches all undeleted pools from database
- Spawns individual compliance sync workflows per pool
- Each pool workflow calls VLM service and updates compliance data

### 4. Pool Creation Integration

During pool creation workflow:
- After successful pool creation, triggers compliance sync as child workflow
- Runs asynchronously with `PARENT_CLOSE_POLICY_ABANDON`
- Non-blocking - failures don't affect pool creation success

## Implementation Details

### Core Components

**1. Pool Activities**
```go
// FetchPoolDataActivity - Retrieves pool configuration for VLM call
type FetchPoolDataActivityInput struct {
    PoolUUID  string `json:"pool_uuid"`
    AccountID int64  `json:"account_id"`
}

// UpdatePoolComplianceActivity - Persists compliance data
type UpdatePoolComplianceActivityInput struct {
    PoolUUID      string                   `json:"pool_uuid"`
    SatisfyZI     bool                     `json:"satisfy_zi"`
    SatisfyZS     bool                     `json:"satisfy_zs"`
    AssetMetadata *datamodel.AssetMetadata `json:"asset_metadata,omitempty"`
}
```

**2. Asset Metadata Structure**
```go
type AssetMetadata struct {
    ChildAssets []ChildAsset
}

type ChildAsset struct {
    AssetNames []string  // List of asset names for this type
    AssetType  string    // Type of asset (e.g., "compute_instance", "compute_disk")
}
```

**3. VLM Client Configuration**
- Workflow timeout: configurable via `VLM_GET_CLUSTER_ZIZS_DETAILS_WF_TIMEOUT_MINUTES` (default: 10 minutes)
- Retry policy with exponential backoff
- Correlation ID tracking for request tracing

### Background Job Scheduling

**Admin Background Jobs Configuration:**
```json
{
  "SYNC_POOL_COMPLIANCE": {
    "jobType": "SYNC_POOL_COMPLIANCE", 
    "cronExpression": "0 * * * *",
    "state": "CREATING"
  }
}
```

### Error Handling Strategy

**Retry Policies:**
- Activity-level retries with exponential backoff
- Non-retryable errors: PanicError
- Maximum attempts configurable via environment variables

**Failure Recovery:**
- Individual pool sync failures don't stop other pools from processing
- Background sync failures are logged but don't affect API responses
- Graceful degradation - pools serve cached compliance data during VLM outages

## API Changes

### Pool Response Enhancement

**Enhanced GetPool API Response:**
```yaml
pool:
  type: object
  properties:
    # Existing fields...
    satisfies_pzi:
      type: boolean
      description: "Whether pool satisfies Zone Independence requirements"
    satisfies_pzs: 
      type: boolean
      description: "Whether pool satisfies Zone Separation requirements"
    asset_metadata:
      type: object
      description: "Metadata about child assets associated with the pool"

asset_metadata:
  type: object
  properties:
    child_assets:
      type: array
      items:
        type: object
        properties:
          asset_type:
            type: string
            example: "compute_instance"
          asset_names:
            type: array
            items:
              type: string
```

## Consequences

### **Positive Impacts:**
- **Visibility**: Clear compliance status for all pools via REST API
- **Automation**: Automated periodic sync ensures data freshness
- **Integrating**: Seamless integration with pool creation workflow
- **Scalability**: Individual pool processing enables parallel execution
- **Reliability**: Retry mechanisms and error isolation prevent cascading failures

### **Operational Considerations:**
- **VLM Dependency**: System depends on VLM service availability for fresh data
- **Performance Impact**: Background workflows run every hour - minimal resource overhead
- **Storage Growth**: Asset metadata JSONB column will grow with complex deployments
- **Monitoring**: New metrics required for sync workflow success/failure rates

### **Technical Debt:**
- **VLM Integration**: Single-vendor dependency on VLM workflow APIs
- **Hard-coded Timeouts**: Environment-based configuration could be more flexible
- **Asset Metadata Evolution**: Structure may need updates as cloud resource types expand

## Migration Strategy

### **Database Migration Path:**
1. **Migrate existing pools**: Set default compliance values (false)
2. **Add constraints**: Ensure future pools always have compliance data
3. **Create indexes**: Optimize asset metadata queries with GIN indexes

### **Deployment Considerations:**
- Zero-downtime migration (nullable → non-nullable with defaults)
- Background sync starts immediately after deployment
- API changes are backward compatible

## Monitoring and Observability

### **Key Metrics to Track:**
- Pool compliance sync success/failure rates
- VML workflow execution latency
- Asset metadata query performance
- API response times for enhanced pool data

### **Alerting Requirements:**
- VLM workflow timeout/retry failures
- Database constraint violations on compliance fields
- High API latency for pool retrieval with compliance data

## Future Enhancements

### **Planned Improvements:**
- **Multi-cloud Support**: Extend beyond GCP for Azure/AWS compliance
- **Real-time Updates**: WebSocket-based compliance status updates
- **Compliance History**: Track compliance changes over time
- **Batch Processing**: Optimize multi-pool compliance checks

### **API Evolution:**
- Compliance-specific endpoints for bulk operations
- Compliance trend analysis endpoints
- Export capabilities for compliance reporting

## References

**Implementation Files:**
- `core/models/pool.go` - Pool model with compliance fields
- `core/orchestrator/workflows/pool_workflows.go` - Pool creation integration
- `core/orchestrator/workflows/backgroundworkflows/sync_pool_compliance_workflow.go` - Background sync workflow
- `core/orchestrator/activities/pool_activities.go` - Compliance sync activities
- `clients/vlm/vlm_workflow_client.go` - VLM integration client
- `database/vcp/migrations/post/0009_add_satisfy_zi_zs_columns.up.sql` - Database schema

**Configuration:**
- `clients/vlm/config.go` - VLM workflow configuration
- `core/scheduler/adminbackgroundjobs/admin_background_jobs.json` - Background job scheduling
