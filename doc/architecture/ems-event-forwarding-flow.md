# EMS Event Forwarding Code Flow

This document explains the complete code flow for EMS event forwarding, starting from when a Google proxy request is received to when EMS events are configured on ONTAP.

## Overview

When a pool is created, the system automatically configures EMS event forwarding to send ONTAP EMS events to a PSC (Private Service Connect) endpoint. This flow is integrated into the pool creation workflow.

## Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. GOOGLE PROXY REQUEST                                                 │
│    Endpoint: POST /v1beta/projects/{project}/locations/{location}/pools │
│    File: google-proxy/api/endpoints/pool_endpoints.go                   │
│    Function: V1betaCreatePool()                                        │
└────────────────────┬──────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. GOOGLE PROXY HANDLER                                                 │
│    - Validates request parameters                                       │
│    - Parses region/zone                                                 │
│    - Resolves performance parameters                                    │
│    - Prepares CreatePoolParams                                          │
│    - Calls: h.Orchestrator.CreatePool(ctx, createPoolParams)            │
│    File: google-proxy/api/endpoints/pool_endpoints.go:221              │
└────────────────────┬──────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. ORCHESTRATOR (GCP Factory)                                          │
│    File: core/orchestrator/factory/gcp/pool.go                         │
│    Function: CreatePool()                                              │
│    - Creates pool record in database                                   │
│    - Creates job record                                                │
│    - Executes Temporal workflow: workflows.CreatePoolWorkflow          │
│    File: core/orchestrator/factory/gcp/pool.go:144-153                 │
└────────────────────┬──────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. POOL CREATION WORKFLOW                                               │
│    File: core/orchestrator/workflows/pool_workflows.go                 │
│    Function: CreatePoolWorkflow()                                       │
│    - Sets up workflow context                                           │
│    - Executes pool creation activities                                 │
│    - Deploys VSA instances                                              │
│    - Configures network                                                 │
│    - Sets up PSC endpoint (if logging enabled)                         │
│    File: core/orchestrator/workflows/pool_workflows.go:107              │
└────────────────────┬──────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 5. PSC ENDPOINT CONFIGURATION WORKFLOW                                   │
│    File: core/orchestrator/workflows/pool_workflows.go:2134             │
│    Function: ConfigurePSCEndpointWorkflow()                             │
│    - Creates internal infra subnet                                     │
│    - Creates PSC endpoint address                                       │
│    - Creates forwarding rule                                            │
│    - Gets forwarding rule IP address                                    │
│    - Updates security audit                                             │
│    - Creates cluster log forwarding                                    │
│    - Creates EMS event forwarding ⭐                                    │
│    File: core/orchestrator/workflows/pool_workflows.go:2211             │
└────────────────────┬──────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 6. EMS EVENT FORWARDING ACTIVITY                                         │
│    File: core/orchestrator/activities/psc_activities.go:160             │
│    Function: CreateEMSEventForwarding()                                │
│    - Records heartbeat                                                  │
│    - Gets VSA provider by node                                          │
│    - Prepares EMS forwarding parameters:                               │
│      * Destination: syslog-ems                                        │
│      * IP: forwardingRuleIpAddress                                     │
│      * Port: ginLoggingMetricsPort (default 5140)                       │
│      * Transport: ginLoggingMetricsProtocol (default tcp)               │
│      * Filter: syslog-ems                                               │
│      * Severities: INFORMATIONAL, EMERGENCY, ERROR, ALERT, NOTICE      │
│    - Calls: provider.CreateEMSEventForwarding()                         │
│    File: core/orchestrator/activities/psc_activities.go:179             │
└────────────────────┬──────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 7. VSA PROVIDER IMPLEMENTATION                                          │
│    File: core/vsa/security_log_forwarding.go:52                         │
│    Function: CreateEMSEventForwarding()                               │
│    - Gets ONTAP REST client                                            │
│    - Gets Support client (not Security client)                         │
│    - Calls 4 generated client methods:                                │
│      1. EMSEventDestinationCreate()                                   │
│      2. EMSEventFilterCreate()                                         │
│      3. EMSEventFilterRuleAdd()                                        │
│      4. EMSEventDestinationModify()                                    │
│    File: core/vsa/security_log_forwarding.go:52-120                    │
└────────────────────┬──────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 8. SUPPORT CLIENT (Generated ONTAP REST Client)                         │
│    File: core/ontap-rest/support_client.go                              │
│    - Uses generated client from clients/ontap-rest/client/support/     │
│    - Methods: EmsDestinationCreate, EmsFilterCreate, etc.              │
│    - Converts params to ONTAP models                                    │
│    - Handles idempotent operations                                      │
│    File: core/ontap-rest/support_client.go:27-174                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Detailed Step-by-Step Flow

### Step 1: Google Proxy Receives Request

**File**: `google-proxy/api/endpoints/pool_endpoints.go`
**Function**: `V1betaCreatePool()`

```go
// Line 96-221
func (h Handler) V1betaCreatePool(ctx context.Context, req *gcpgenserver.PoolV1beta, params gcpgenserver.V1betaCreatePoolParams) {
    // Validates request
    // Parses region/zone
    // Prepares CreatePoolParams
    created, operationID, err := h.Orchestrator.CreatePool(ctx, createPoolParams)
}
```

**Key Points**:
- Receives HTTP POST request from Google Cloud
- Validates pool creation parameters
- Extracts project number, location, region, zone
- Prepares `CreatePoolParams` structure
- Calls orchestrator to create pool

---

### Step 2: Orchestrator Creates Workflow

**File**: `core/orchestrator/factory/gcp/pool.go`
**Function**: `CreatePool()`

```go
// Line 144-153
workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
err = workflowExecutor.ExecuteWorkflow(
    ctx,
    createdJob.WorkflowID,
    workflowengine.CustomerTaskQueue,
    workflows.CreatePoolWorkflow,  // ← Workflow name
    workflowengine.GetCreatePoolWorkflowRunTimeout(params.LargeCapacity),
    params,
    dbPool,
)
```

**Key Points**:
- Creates pool and job records in database
- Starts Temporal workflow: `CreatePoolWorkflow`
- Returns operation ID to Google proxy
- Workflow runs asynchronously

---

### Step 3: Pool Creation Workflow Executes

**File**: `core/orchestrator/workflows/pool_workflows.go`
**Function**: `CreatePoolWorkflow()`

```go
// Line 107-142
func CreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) error {
    // Sets up workflow
    // Executes pool creation activities
    // Deploys VSA instances
    // Configures network
    // ...
    
    // If logging feature flag enabled:
    if ginLoggingFeatureFlag {
        err = workflow.ExecuteChildWorkflow(
            setupPSCEndpoint, 
            ConfigurePSCEndpointWorkflow,  // ← Child workflow
            tenancyDetails.RegionalTenantProject, 
            params.Region, 
            node
        ).Get(ctx, nil)
    }
}
```

**Key Points**:
- Main workflow orchestrates pool creation
- Executes multiple activities (VSA deployment, network setup, etc.)
- Conditionally executes `ConfigurePSCEndpointWorkflow` if logging is enabled
- Uses child workflow for PSC endpoint setup

---

### Step 4: PSC Endpoint Configuration Workflow

**File**: `core/orchestrator/workflows/pool_workflows.go`
**Function**: `ConfigurePSCEndpointWorkflow()`

```go
// Line 2134-2216
func ConfigurePSCEndpointWorkflow(ctx workflow.Context, projectName string, region string, node *models.Node) error {
    // 1. Create internal infra subnet
    // 2. Create PSC endpoint address
    // 3. Create forwarding rule
    // 4. Get forwarding rule IP address
    // 5. Update security audit
    // 6. Create cluster log forwarding
    // 7. Create EMS event forwarding ⭐
    
    err = workflow.ExecuteActivity(
        setupPscCtx, 
        pscActivity.CreateEMSEventForwarding,  // ← Activity
        node, 
        forwardingRuleIpAddress
    ).Get(ctx, nil)
}
```

**Key Points**:
- Creates GCP PSC endpoint infrastructure
- Gets forwarding rule IP address (destination for EMS events)
- Executes `CreateEMSEventForwarding` activity
- Passes node and IP address to activity

---

### Step 5: EMS Event Forwarding Activity

**File**: `core/orchestrator/activities/psc_activities.go`
**Function**: `CreateEMSEventForwarding()`

```go
// Line 160-184
func (j *PSCActivity) CreateEMSEventForwarding(ctx context.Context, node *models.Node, address string) error {
    // Get VSA provider
    provider, err := hyperscaler2.GetProviderByNode(ctx, node)
    
    // Prepare EMS forwarding parameters
    emsEventForwardingParams := vsa.CreateEMSEventForwardingParams{
        DestinationName: "syslog-ems",
        DestinationIP:   address,  // PSC forwarding rule IP
        DestinationPort: ginLoggingMetricsPort,  // 5140
        Transport:       ginLoggingMetricsProtocol,  // "tcp"
        TimestampFormat: "rfc-3164",
        MessageFormat:   "legacy-netapp",
        FilterName:      "syslog-ems",
        Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
    }
    
    // Call provider
    err = provider.CreateEMSEventForwarding(emsEventForwardingParams)
}
```

**Key Points**:
- Gets VSA provider for the node
- Prepares EMS forwarding configuration
- Sets destination to PSC endpoint IP address
- Configures filter to forward specific severities
- Calls provider implementation

---

### Step 6: VSA Provider Implementation

**File**: `core/vsa/security_log_forwarding.go`
**Function**: `CreateEMSEventForwarding()`

```go
// Line 52-120
func (rc *OntapRestProvider) CreateEMSEventForwarding(params CreateEMSEventForwardingParams) error {
    // Get ONTAP REST client
    client, err := getOntapClientFunc(rc.ClientParams)
    if err != nil {
        return err
    }

    // Get Support client (not Security client)
    supportClient := client.Support()

    // Step 1: Create destination
    destinationParams := &ontapRest.EMSEventDestinationCreateParams{
        Name:                    nillable.GetStringPtr(params.DestinationName),
        Type:                    nillable.GetStringPtr("syslog"),
        SyslogHost:              nillable.GetStringPtr(params.DestinationIP),
        SyslogPort:              nillable.GetInt64Ptr(params.DestinationPort),
        SyslogTransport:         nillable.GetStringPtr(params.Transport),
        SyslogTimestampFormat:   nillable.GetStringPtr(params.TimestampFormat),
        SyslogMessageFormat:     nillable.GetStringPtr(params.MessageFormat),
    }
    err = supportClient.EMSEventDestinationCreate(destinationParams)

    // Step 2: Create filter
    filterParams := &ontapRest.EMSEventFilterCreateParams{
        Name: nillable.GetStringPtr(params.FilterName),
    }
    err = supportClient.EMSEventFilterCreate(filterParams)

    // Step 3: Add filter rules
    ruleParams := &ontapRest.EMSEventFilterRuleAddParams{
        FilterName: nillable.GetStringPtr(params.FilterName),
        Type:       nillable.GetStringPtr("include"),
        Severity:   params.Severities,
    }
    err = supportClient.EMSEventFilterRuleAdd(ruleParams)

    // Step 4: Link filter to destination
    modifyParams := &ontapRest.EMSEventDestinationModifyParams{
        Filters: []string{params.FilterName},
    }
    err = supportClient.EMSEventDestinationModify(params.DestinationName, modifyParams)
}
```

**Key Points**:
- Uses generated ONTAP REST client methods (SupportClient)
- Makes 4 sequential API calls through generated client
- Handles idempotency (ignores "already exists" errors)
- Links filter to destination to enable forwarding
- Follows same pattern as SecurityLogForwardingCreate

---

### Step 7: Support Client Implementation

**File**: `core/ontap-rest/support_client.go`
**Interface**: `SupportClient`

```go
// Line 13-21
type SupportClient interface {
    EMSEventDestinationCreate(params *EMSEventDestinationCreateParams) error
    EMSEventDestinationGet(name string) (*EMSEventDestination, error)
    EMSEventDestinationModify(name string, params *EMSEventDestinationModifyParams) error
    EMSEventFilterCreate(params *EMSEventFilterCreateParams) error
    EMSEventFilterGet(name string) (*EMSEventFilter, error)
    EMSEventFilterRuleAdd(params *EMSEventFilterRuleAddParams) error
}
```

**Implementation Details**:
- Uses generated client from `clients/ontap-rest/client/support/`
- Converts our params to ONTAP models (`emsDestinationCreateParamsToONTAP`)
- Calls generated methods: `EmsDestinationCreate`, `EmsFilterCreate`, etc.
- Handles idempotent operations gracefully
- Returns our domain models from ONTAP responses

**Generated Client Methods Used**:
1. `EmsDestinationCreate()` - Creates EMS destination
2. `EmsDestinationGet()` - Gets EMS destination
3. `EmsDestinationModify()` - Modifies EMS destination
4. `EmsFilterCreate()` - Creates EMS filter
5. `EmsFilterGet()` - Gets EMS filter
6. `EmsFiltersRulesCreate()` - Adds rule to filter

**Key Points**:
- Uses generated ONTAP REST client (consistent with other operations)
- Type-safe API calls with generated models
- Proper error handling and idempotency
- Follows established patterns in the codebase

---

## Data Flow Summary

```
Google Proxy Request
    ↓
CreatePoolParams {
    AccountName, Region, Name, ...
}
    ↓
Temporal Workflow: CreatePoolWorkflow
    ↓
Child Workflow: ConfigurePSCEndpointWorkflow
    ↓
Activity: CreateEMSEventForwarding
    ↓
VSA Provider: CreateEMSEventForwardingParams {
    DestinationName: "syslog-ems",
    DestinationIP: "{psc-ip}",
    DestinationPort: 5140,
    Transport: "tcp",
    FilterName: "syslog-ems",
    Severities: [...]
}
    ↓
SupportClient (Generated ONTAP REST Client)
    ↓
ONTAP REST API Calls (4 requests via generated client)
    ↓
ONTAP Configuration Complete
```

## Key Components

### 1. **Google Proxy** (`google-proxy/`)
- Receives external API requests
- Validates and transforms requests
- Calls orchestrator

### 2. **Orchestrator** (`core/orchestrator/`)
- Business logic layer
- Creates workflows
- Manages job lifecycle

### 3. **Workflows** (`core/orchestrator/workflows/`)
- Temporal workflows for orchestration
- Long-running processes
- Activity coordination

### 4. **Activities** (`core/orchestrator/activities/`)
- Temporal activities (executable units)
- Business operations
- Can be retried

### 5. **VSA Provider** (`core/vsa/`)
- ONTAP interaction layer
- Uses generated REST client methods
- Abstraction over ONTAP operations

### 6. **ONTAP REST Client** (`core/ontap-rest/`)
- ONTAP API client wrappers
- SupportClient for EMS operations
- SecurityClient for security operations
- Authentication and request/response handling

### 7. **Generated ONTAP Client** (`clients/ontap-rest/client/`)
- Auto-generated from swagger.yaml
- Support client for EMS operations
- Type-safe API methods

## Error Handling

- **Workflow Level**: Temporal retries with exponential backoff
- **Activity Level**: Activity-specific retry policies
- **Provider Level**: Idempotent operations (ignores "already exists")
- **API Level**: HTTP error handling and logging

## Configuration

EMS event forwarding is controlled by:
- **Feature Flag**: `ginLoggingFeatureFlag` (enables/disables PSC endpoint setup)
- **Port**: `ginLoggingMetricsPort` (default: 5140)
- **Protocol**: `ginLoggingMetricsProtocol` (default: "tcp")
- **Destination Name**: "syslog-ems" (hardcoded)
- **Filter Name**: "syslog-ems" (hardcoded)

## Testing

See `doc/testing/ems-event-forwarding-testing.md` for:
- Unit tests
- Integration tests
- Manual testing procedures
- Verification commands
