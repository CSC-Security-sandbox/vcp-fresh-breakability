# Telemetry Component Design Document

## 1. Overview

The Telemetry Component is a critical microservice within the VSA (Volume Service Architecture) Control Plane that handles the collection, processing, aggregation, and delivery of performance and usage metrics for Google Cloud NetApp Volumes (GCNV) resources. It serves as the central hub for monitoring and billing operations, ensuring accurate metric reporting to Google Cloud's billing and monitoring systems.

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Telemetry & Monitoring System                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────────┤
│  │                        Control Plane Layer                                 │
│  ├─────────────────────────────────────────────────────────────────────────────┤
│  │ ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ │   API       │  │ Scheduler   │  │   Workers   │  │   Temporal  │        │
│  │ │ Endpoints   │  │ (Cloud      │  │  (Queues)   │  │ Workflows   │        │
│  │ │             │  │ Scheduler)  │  │             │  │             │        │
│  │ └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
│  │         │                │                │                │               │
│  │         └────────────────┼────────────────┼────────────────┘               │
│  │                          │                │                                │
│  └─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────────┤
│  │                     Collection & Processing Layer                          │
│  ├─────────────────────────────────────────────────────────────────────────────┤
│  │ ┌─────────────────────────────────────────┐ ┌─────────────────────────────┐│
│  │ │          Batch Collection Pipeline      │ │    Real-time Collection     ││
│  │ │ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌───│ │ ┌─────────────────────────┐ ││
│  │ │ │Collector│→│Processor│→│Aggreg-  │→│Sink│ │ │     Harvest Farm        │ ││
│  │ │ │         │ │         │ │ator     │ │   │ │ │ ┌─────────┐ ┌─────────┐ │ ││
│  │ │ └─────────┘ └─────────┘ └─────────┘ └───│ │ │ │ Poller  │ │OpenTel  │ │ ││
│  │ └─────────────────────────────────────────┘ │ │ │ Manager │ │Collector│ │ ││
│  │                     │                       │ │ └─────────┘ └─────────┘ │ ││
│  │                     ▼                       │ │           │             │ ││
│  │           ┌─────────────────┐               │ │           ▼             │ ││
│  │           │  Google Cloud   │◄──────────────┘ │ ┌─────────────────────┐ │ ││
│  │           │   Monitoring    │                 │ │   Prometheus        │ │ ││
│  │           └─────────────────┘                 │ │   Targets           │ │ ││
│  │                     │                         │ └─────────────────────┘ │ ││
│  │                     ▼                         └─────────────────────────┘ ││
│  │           ┌─────────────────┐                                             ││
│  │           │  Google Cloud   │                                             ││
│  │           │    Billing      │                                             ││
│  │           └─────────────────┘                                             ││
│  └─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────────┤
│  │                            Data Sources Layer                              │
│  ├─────────────────────────────────────────────────────────────────────────────┤
│  │ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │ │ VCP Database│ │ Metrics DB  │ │ Google APIs │ │    ONTAP Clusters       ││
│  │ │             │ │             │ │             │ │ ┌─────┐ ┌─────┐ ┌─────┐ ││
│  │ │   Pools     │ │  Hydrated   │ │  Cloud      │ │ │Node1│ │Node2│ │...  │ ││
│  │ │   Volumes   │ │  Metrics    │ │ Monitoring  │ │ │     │ │     │ │     │ ││
│  │ │   Backups   │ │ Aggregated  │ │   APIs      │ │ └─────┘ └─────┘ └─────┘ ││
│  │ │             │ │   Usage     │ │             │ │      ▲       ▲       ▲   ││
│  │ └─────────────┘ └─────────────┘ └─────────────┘ └──────┼───────┼───────┼───┘│
│  └────────────────────────────────────────────────────────┼───────┼───────┼────┤
│                                                            │       │       │    │
│                                ┌───────────────────────────┘       │       │    │
│                                │ ┌─────────────────────────────────┘       │    │
│                                │ │ ┌───────────────────────────────────────┘    │
│                                ▼ ▼ ▼                                            │
│                        ┌─────────────────┐                                     │
│                        │ Harvest Pollers │                                     │
│                        │ (REST/Perf APIs)│                                     │
│                        └─────────────────┘                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Interactions

The telemetry system operates through a pipeline architecture where each component has specific responsibilities:

1. **Collection**: Gathers raw metrics from various sources
2. **Processing**: Transforms and enriches raw data
3. **Aggregation**: Combines metrics for billing purposes
4. **Delivery**: Sends processed metrics to Google Cloud services

## 3. Core Components

### 3.1 API Layer

**File**: `telemetry/api/endpoints/`
**Purpose**: External interface for triggering metric collection operations

#### Endpoints
- `POST /v1/performance` - Triggers performance metrics collection
- `POST /v1/usage` - Triggers usage metrics aggregation and billing
- `GET /metrics` - Prometheus metrics endpoint

#### Features
- RESTful API design using OpenAPI 3.0 specification
- Asynchronous processing (returns 202 Accepted)
- Cloud Scheduler integration via OIDC authentication
- Built-in logging and error handling

### 3.2 Collector Module

**Files**: `telemetry/collector/`
**Purpose**: Responsible for gathering metrics from multiple data sources

#### Key Components

##### 3.2.1 Pool Collector
- **File**: `pool_collector.go`
- **Function**: Collects storage pool performance metrics
- **Data Sources**: VCP database, ONTAP REST APIs
- **Metrics**: Capacity, IOPS, throughput, latency

##### 3.2.2 Volume Collector
- **File**: `volume_collector.go`
- **Function**: Collects volume-level metrics
- **Integration**: Google Cloud Monitoring API
- **Metrics**: Volume performance, capacity utilization

##### 3.2.3 Backup Collector
- **Function**: Collects backup-related metrics
- **Metrics**: Backup size, frequency, success rates

#### 3.2.4 Harvest Farm Integration
- **Purpose**: Manages ONTAP node registration for real-time metrics collection
- **Components**: Harvest Farm service, OpenTelemetry Collector, Prometheus endpoints
- **Function**: Collects real-time performance metrics directly from ONTAP clusters

#### Data Flow
```
VCP Database → Pool Metrics
ONTAP APIs → Volume Metrics  → Raw Metrics → Hydrated Metrics
Google APIs → Backup Metrics
Harvest Farm → ONTAP Real-time Metrics → OpenTelemetry → Google Cloud Monitoring
```

### 3.3 Processor Module

**File**: `telemetry/processor/processor.go`
**Purpose**: Central orchestrator for metric processing workflows

#### Key Functions

##### 3.3.1 ProcessPerformanceMetrics()
- Collects pool, volume, and backup metrics
- Hydrates raw data with metadata
- Stores processed metrics in telemetry database
- Delivers metrics to performance sink

##### 3.3.2 ProcessUsageMetrics()
- Triggers billing aggregation
- Processes hourly usage data
- Handles retry logic for failed submissions

##### 3.3.3 CollectMetrics()
- Project-specific metric collection
- Asynchronous processing for volume metrics
- Batch processing for efficiency

#### Processing Pipeline
```
Raw Data → Validation → Enrichment → Storage → Delivery
```

### 3.4 Aggregator Module

**Files**: `telemetry/aggregator/`
**Purpose**: Aggregates raw metrics for billing and reporting purposes

#### Key Components

##### 3.4.1 BillingProvider
- **File**: `metrics_processor.go`
- **Function**: Processes billing metrics with configurable job definitions
- **Features**:
  - Resource grouping by unique identifiers
  - Time-based aggregation windows
  - Retry mechanism for failed submissions
  - Support for multiple measurement types

##### 3.4.2 Job Definitions
- Configurable aggregation rules
- Resource type and measurement type mapping
- Time window specifications
- Aggregation functions (sum, average, max, etc.)

#### Aggregation Flow
```
HydratedMetrics → Resource Grouping → Time Windowing → Aggregation → AggregatedUsage
```

### 3.5 Sink Module

**Files**: `telemetry/performance/sink.go`, `telemetry/usage/sink.go`
**Purpose**: Delivers processed metrics to external systems

#### Performance Sink
- **Target**: Google Cloud Monitoring
- **Data**: Real-time performance metrics
- **Format**: Cloud Monitoring time series

#### Usage Sink (GoogleUsageSink)
- **Target**: Google Cloud Billing
- **Data**: Aggregated usage for billing
- **Features**:
  - Batch processing for efficiency
  - Validation and filtering
  - Error handling and retry logic
  - Billing label management

## 4. Data Models

### 4.1 HydratedMetrics
```go
type HydratedMetrics struct {
    ID              int64
    MetricTimestamp time.Time
    MeasuredType    metadata.MeasuredType
    ResourceType    metadata.ResourceType
    Quantity        float64
    ResourceName    string
    ConsumerID      string
    Location        string
    Metadata        []byte
    DeploymentName  string
}
```

### 4.2 AggregatedUsage
```go
type AggregatedUsage struct {
    ID                     int64
    VendorCustomerID       *string
    AggregationEnd         time.Time
    AggregationStart       time.Time
    MeasuredType           metadata.MeasuredType
    Quantity               float64
    ResourceName           *string
    ResourceType           metadata.ResourceType
    AggregationType        string
    State                  TrackingState
    // ... additional billing fields
}
```

## 5. Queue System

### 5.1 Queue Types
- **PerformanceQueue**: Handles performance metric processing
- **UsageQueue**: Manages billing aggregation jobs
- **CollectionQueue**: Processes metric collection tasks

### 5.2 Worker Architecture
- Multiple workers per queue type
- Configurable worker count via `telemetryConfig.NumWorkers`
- Background job processing with PostgreSQL-based queue
- Automatic retry and error handling

### 5.3 Job Processing
```go
// Performance metrics job
&jobs.ProcessPerformanceMetrics{}

// Usage metrics job  
&jobs.ProcessUsageMetrics{}

// Collection job
&jobs.CollectMetrics{}
```

## 6. Configuration

### 6.1 Telemetry Configuration
- **File**: `telemetry/common/config.go`
- **Environment Variables**:
  - `ENABLE_VOLUME_METRICS`: Feature flag for volume metrics
  - `ENABLE_BACKUP_METRICS`: Feature flag for backup metrics
  - `PUSH_BATCH_SIZE`: Batch size for database operations
  - `NUM_WORKERS`: Number of background workers

### 6.2 Database Configuration
- **VCP Database**: Core application data
- **Metrics Database**: Telemetry-specific data storage
- **Connection Pooling**: Configurable pool sizes and timeouts

## 7. Scheduling and Automation

### 7.1 Cloud Scheduler Integration
- **Performance Collection**: Every 5 minutes (`*/5 * * * *`)
- **Usage Processing**: Every hour (`15 * * * *`)
- **OIDC Authentication**: Service account-based security
- **Endpoints**:
  - `/v1/performance` for performance metrics
  - `/v1/usage` for billing aggregation

### 7.2 Deployment Automation
- **Tool**: `tools/telemetry-deployer/main.go`
- **Features**:
  - Cloud Run service deployment
  - Cloud Scheduler job creation/updates
  - Network and security configuration
  - Environment variable management

## 8. Monitoring and Observability

### 8.1 Metrics Exposure
- **Prometheus Endpoint**: `/metrics`
- **Custom Metrics**: Processing times, error rates, queue depths
- **Health Checks**: Database connectivity, external API availability

### 8.1 Logging
- **Structured Logging**: JSON format with correlation IDs
- **Log Levels**: Debug, Info, Warn, Error
- **Context Propagation**: Request tracing across components

### 8.3 Error Handling
- **Graceful Degradation**: Continue processing on partial failures
- **Retry Logic**: Exponential backoff for transient failures
- **Dead Letter Queues**: Failed jobs for manual intervention

## 9. Security

### 9.1 Authentication
- **OIDC Tokens**: Cloud Scheduler authentication
- **Service Accounts**: Google Cloud API access
- **Network Security**: VPC-native networking

### 9.2 Data Protection
- **Encryption**: Data at rest and in transit
- **Access Control**: Role-based permissions
- **Audit Logging**: All metric operations logged

## 10. Scalability and Performance

### 10.1 Horizontal Scaling
- **Stateless Design**: Multiple replicas supported
- **Queue-Based Processing**: Distributed workload
- **Database Sharding**: Partition by time or resource

### 10.2 Performance Optimizations
- **Batch Processing**: Configurable batch sizes
- **Connection Pooling**: Efficient database utilization
- **Async Processing**: Non-blocking operations
- **Caching**: Metadata and configuration caching

## 11. Data Flow Diagrams

### 11.1 Performance Metrics Flow
```
Cloud Scheduler → API Endpoint → Queue → Worker → Collector → Processor → Sink → Google Cloud Monitoring
                                                     ↓
                                                 Database
```

### 11.2 Usage Metrics Flow
```
Cloud Scheduler → API Endpoint → Queue → Worker → Aggregator → Usage Sink → Google Cloud Billing
                                                     ↓
                                                 Database
```

## 12. Low-Level Design Details

### 12.1 Database Schema Design

#### 12.1.1 Jobs Table
```sql
CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    type_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    queue TEXT NOT NULL,
    data TEXT NOT NULL,
    error TEXT,
    attempt INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    scheduled_at TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_jobs_queue_status ON jobs(queue, status);
CREATE INDEX idx_jobs_scheduled_at ON jobs(scheduled_at);
CREATE INDEX idx_jobs_type_name ON jobs(type_name);
```

#### 12.1.2 HydratedMetrics Table
```sql
CREATE TABLE hydrated_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_timestamp TIMESTAMP NOT NULL,
    measured_type VARCHAR(100) NOT NULL,
    resource_type VARCHAR(100) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    resource_name VARCHAR(255),
    consumer_id VARCHAR(255),
    location VARCHAR(255),
    metadata JSONB,
    deployment_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for efficient querying
CREATE INDEX idx_hydrated_metrics_timestamp ON hydrated_metrics(metric_timestamp);
CREATE INDEX idx_hydrated_metrics_resource_measured ON hydrated_metrics(resource_type, measured_type);
CREATE INDEX idx_hydrated_metrics_consumer ON hydrated_metrics(consumer_id);
```

#### 12.1.3 AggregatedUsage Table
```sql
CREATE TABLE aggregated_usage (
    id BIGSERIAL PRIMARY KEY,
    vendor_customer_id VARCHAR(255),
    aggregation_end TIMESTAMP NOT NULL,
    aggregation_start TIMESTAMP NOT NULL,
    measured_type VARCHAR(100) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    resource_name VARCHAR(255),
    resource_type VARCHAR(100) NOT NULL,
    aggregation_type VARCHAR(100) NOT NULL,
    last_counter_value DOUBLE PRECISION,
    state INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    error_message TEXT,
    is_billable BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 12.2 Job Queue Implementation

#### 12.2.1 Queue Architecture
The PostgreSQL-based job queue provides:

```go
type JobQueue struct {
    db           *sql.DB
    processor    interface{}
    mutex        sync.Mutex
    typeRegistry map[string]reflect.Type
}
```

**Key Features:**
- **SKIP LOCKED**: Prevents worker conflicts using PostgreSQL's SKIP LOCKED
- **Type Registry**: Dynamic job type registration using reflection
- **Transactional Safety**: All operations wrapped in database transactions
- **Retry Logic**: Automatic retry with configurable max attempts (3)

#### 12.2.2 Job Processing Flow
```go
// 1. Enqueue Job
func (j *JobQueue) Enqueue(ctx context.Context, job Job, queue string) error {
    typeName := j.typeName(job)
    data, _ := json.Marshal(job)
    
    _, err = j.db.ExecContext(ctx,
        `INSERT INTO jobs (type_name, status, queue, data) VALUES ($1, $2, $3, $4)`,
        typeName, JOB_STATUS_SCHEDULED, queue, data)
    return err
}

// 2. Dequeue and Process
func (j *JobQueue) Dequeue(ctx context.Context, queues []string) error {
    // Update job status atomically using FOR UPDATE SKIP LOCKED
    sqlStmt := `
        UPDATE jobs SET status = $1, started_at = clock_timestamp(), attempt = attempt + 1
        WHERE id IN (
            SELECT id FROM jobs j
            WHERE (j.status = $2 or (j.status = $3 and j.attempt < $4))
            AND j.queue = any($5)
            AND j.type_name = any($6) 
            ORDER BY j.scheduled_at, j.created_at
            FOR UPDATE SKIP LOCKED LIMIT 1
        )
        RETURNING id, type_name, data, attempt`
    
    // Execute job using reflection-based type loading
    loadedJob, _ := jobType.Load(job.Data)
    err = loadedJob.Perform(j.processor, int32(job.Attempt))
}
```

### 12.3 Metric Collection Implementation

#### 12.3.1 Google Cloud Monitoring Integration
```go
type GoogleVolumeMetricsProvider struct {
    client               MonitoringClient
    tenantProjectProvider TenantProjectProvider
    metrics              []MetricDefinition
    startTime            time.Time
    endTime              time.Time
    jobQueue             *utils.JobQueue
}

func (g *GoogleVolumeMetricsProvider) CollectProjectMetrics(ctx context.Context, logger log.Logger, projectID string) ([]datamodel.HydratedMetrics, error) {
    projectName := fmt.Sprintf("projects/%s", projectID)
    
    for _, metric := range g.metrics {
        filter := fmt.Sprintf(`metric.type="%s/%s"`, metric.ResourceType, metric.Metric)
        req := &monitoringpb.ListTimeSeriesRequest{
            Name:   projectName,
            Filter: filter,
            Interval: &monitoringpb.TimeInterval{
                StartTime: timestamppb.New(g.startTime),
                EndTime:   timestamppb.New(g.endTime),
            },
            View:     monitoringpb.ListTimeSeriesRequest_FULL,
            PageSize: 100,
        }
        
        it := g.client.ListTimeSeries(ctx, req)
        // Process time series data and create HydratedMetrics
    }
}
```

#### 12.3.2 Value Extraction and Type Handling
```go
func extractValue(Value *monitoringpb.TypedValue) float64 {
    switch v := Value.Value.(type) {
    case *monitoringpb.TypedValue_DoubleValue:
        return v.DoubleValue
    case *monitoringpb.TypedValue_Int64Value:
        return float64(v.Int64Value)
    case *monitoringpb.TypedValue_BoolValue:
        if v.BoolValue {
            return 1.0
        }
        return 0.0
    default:
        return 0
    }
}
```

### 12.4 Aggregation Engine

#### 12.4.1 Job Definition System
```go
type AggregationJobDefinition struct {
    MeasuredType    metadata.MeasuredType
    ResourceType    metadata.ResourceType
    AggregationType JobType
    IsBillable      bool
    SKU             string
}

var DefaultAggregationJobDefinitions = map[metadata.CombinedKeyResourceTypeMeasuredType]AggregationJobDefinition{
    {ResourceType: metadata.Volume, MeasuredType: metadata.AllocatedSize}: {
        AggregationType: IntegralAggregation,
        IsBillable:      false,
    },
    {ResourceType: metadata.VolumeReplicationRelationship, MeasuredType: metadata.XregionReplicationTotalTransferBytes}: {
        AggregationType: CounterAggregation,
        IsBillable:      true,
        SKU:             "/ReplicationBytesTransferred",
    },
}
```

#### 12.4.2 Aggregation Functions

**Integral Aggregation** (for capacity over time):
```go
func Integral(metrics []datamodel.HydratedMetrics) float64 {
    // Sort by timestamp
    sort.Slice(metrics, func(i, j int) bool {
        return metrics[i].MetricTimestamp.Before(metrics[j].MetricTimestamp)
    })
    
    var integral float64
    for i := 1; i < len(metrics); i++ {
        duration := metrics[i].MetricTimestamp.Sub(metrics[i-1].MetricTimestamp).Hours()
        integral += metrics[i].Quantity * duration
    }
    return integral
}
```

**Counter Aggregation** (for monotonic counters with reset handling):
```go
func CounterDelta(metrics []datamodel.HydratedMetrics) float64 {
    var aggregate float64
    var lastMetric *datamodel.HydratedMetrics
    
    for _, metric := range metrics {
        if lastMetric == nil {
            lastMetric = &metric
            continue
        }
        
        quantity := metric.Quantity - lastMetric.Quantity
        
        // Handle counter reset (value decreases)
        if quantity < 0 {
            // If current < 25% of previous, assume reset
            if metric.Quantity < lastMetric.Quantity*0.25 {
                quantity = metric.Quantity
            } else {
                continue // Skip anomalous data point
            }
        }
        
        aggregate += quantity
        lastMetric = &metric
    }
    return aggregate
}
```

### 12.5 Error Handling and Resilience

#### 12.5.1 Retry Mechanisms
```go
const (
    JOB_STATUS_SCHEDULED = "new"
    JOB_STATUS_FINISHED  = "finished"
    JOB_STATUS_FAILED    = "failed"
    MAX_RETRY = 3
)

// Jobs are retried up to MAX_RETRY times
// Failed jobs are marked with error details for debugging
```

#### 12.5.2 Graceful Degradation
- **Partial Failures**: Continue processing other metrics when individual collections fail
- **Data Validation**: Skip invalid data points while logging warnings
- **Circuit Breaking**: Temporary failures don't stop the entire pipeline

### 12.6 Performance Optimizations

#### 12.6.1 Batch Processing
```go
// Configurable batch sizes for database operations
config.PushBatchSize = 1000 // Default batch size

func (db *Storage) CreateHydratedMetricsBatch(ctx context.Context, metrics []datamodel.HydratedMetrics, batchSize int) error {
    for i := 0; i < len(metrics); i += batchSize {
        end := i + batchSize
        if end > len(metrics) {
            end = len(metrics)
        }
        batch := metrics[i:end]
        // Insert batch
    }
}
```

#### 12.6.2 Connection Management
```go
// Database connection pooling configuration
DB_MAX_OPEN_CONNS = 25
DB_MAX_IDLE_CONNS = 25
DB_CONN_MAX_LIFETIME = "1h"
```

#### 12.6.3 Async Processing
```go
// Volume metrics collection runs asynchronously
go func(ctx context.Context) {
    asyncCtx := context.WithValue(context.Background(), 
        middleware.CorrelationContextKey, 
        ctx.Value(middleware.CorrelationContextKey))
    mp.processRawMetrics(asyncCtx)
}(ctx)
```

### 12.7 Security Implementation

#### 12.7.1 OIDC Authentication Flow
```go
// Cloud Scheduler uses OIDC tokens for service authentication
job := &cloudscheduler.Job{
    HttpTarget: &cloudscheduler.HttpTarget{
        Uri:        config.ServiceURL,
        HttpMethod: "POST",
        OidcToken: &cloudscheduler.OidcToken{
            ServiceAccountEmail: config.ServiceAccountName,
            Audience:            config.ServiceURL,
        },
    },
}
```

#### 12.7.2 Network Security
- **VPC-native**: Cloud Run services deployed in private subnets
- **Service Accounts**: Least-privilege access to Google Cloud APIs
- **TLS**: All external communications encrypted

### 12.8 Monitoring and Telemetry

#### 12.8.1 Custom Metrics
```go
// Prometheus metrics exposed at /metrics endpoint
var (
    jobsProcessed = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "telemetry_jobs_processed_total",
            Help: "Total number of jobs processed",
        },
        []string{"queue", "status"},
    )
    
    processingDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "telemetry_processing_duration_seconds",
            Help: "Time spent processing metrics",
        },
        []string{"operation"},
    )
)
```

#### 12.8.2 Structured Logging
```go
// Correlation ID propagation for request tracing
ctx := context.WithValue(context.Background(), 
    middleware.CorrelationContextKey, 
    uuid.NewString())

logger.Infof("Processing metrics", 
    "correlation_id", ctx.Value(middleware.CorrelationContextKey),
    "operation", "performance_collection",
    "metrics_count", len(metrics))
```

## 13. Future Enhancements

### 13.1 Planned Features
- **Real-time Streaming**: Event-driven metric collection using Pub/Sub
- **Machine Learning**: Anomaly detection and predictive analytics
- **Multi-cloud Support**: Azure and AWS metric collection
- **Advanced Aggregations**: Custom aggregation functions via configuration

### 13.2 Technical Debt
- **Code Coverage**: Increase test coverage to >90%
- **Documentation**: API documentation improvements
- **Performance**: Query optimization and indexing
- **Monitoring**: Enhanced observability and alerting

## 14. Harvest Farm System

### 14.1 Overview

The Harvest Farm is a specialized component that manages the registration and unregistration of ONTAP nodes for real-time metrics collection. It acts as a bridge between the VSA Control Plane and ONTAP clusters, enabling continuous monitoring and performance data collection.

### 14.2 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Harvest Farm System                         │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Harvest   │  │   OpenTel   │  │ Prometheus  │             │
│  │   Farm      │  │ Collector   │  │ Scraper     │             │
│  │  Service    │  │             │  │             │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│         │                │                │                    │
│         └────────────────┼────────────────┘                    │
│                          │                                     │
│  ┌─────────────────────────────────────────────────────────────┤
│  │                ONTAP Clusters                               │
│  ├─────────────────────────────────────────────────────────────┤
│  │ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐             │
│  │ │   Node 1    │ │   Node 2    │ │   Node N    │             │
│  │ │  (Poller)   │ │  (Poller)   │ │  (Poller)   │             │
│  │ └─────────────┘ └─────────────┘ └─────────────┘             │
│  └─────────────────────────────────────────────────────────────┘
└─────────────────────────────────────────────────────────────────┘
```

### 14.3 Node Registration Process

#### 14.3.1 Registration Workflow
The registration process is orchestrated by Temporal workflows:

```go
type RegisterNodeToHarvestFarmWorkflowInput struct {
    PoolID            int64
    MaxNodesPerGroup  int
    CustomerProjectID string
    TenantProjectID   string
    PoolUUID          string
    AccountID         int64
    DeploymentName    string
}
```

#### 14.3.2 Registration Steps

**Step 1: Node Assignment**
```go
// Assigns two nodes to two different node groups for redundancy
nodeMappings, err := a.SE.AssignTwoNodesToTwoGroups(ctx, datamodel.NodeGroupAssignmentParams{
    Node1:            nodes[0],
    Node2:            nodes[1],
    MaxNodesPerGroup: input.MaxNodesPerGroup,
    CustomerProject:  input.CustomerProjectID,
    TenantProject:    input.TenantProjectID,
    DeploymentName:   input.DeploymentName,
})
```

**Step 2: Kubernetes Lease Management**
```go
// Creates Kubernetes leases for high availability
leaseName := leasePrefix + nodeGroup.UUID
if err := createKubernetesLease(ctx, vcpLeaseNameSpace, leaseName); err != nil {
    return err
}
nodeGroup.LeaseName = leaseName
```

**Step 3: Configuration Template Rendering**
```go
// Renders Harvest configuration from template
tmplStr, err := renderFunc(mapping.HarvestConfig)
if err != nil {
    return errors.New("template render failed: " + err.Error())
}
```

**Step 4: Configuration Upload**
```go
// Uploads configuration to Harvest Farm service
resp, err := uploadYAMLFile(ctx, UploadYAMLFileInput{
    URL:       input.UploadURL,
    YAML:      tmplStr,
    LeaseName: leaseName,
    NodeID:    mapping.NodeID,
})
```

#### 14.3.3 Harvest Configuration Template

The Harvest configuration defines how to connect to and collect metrics from ONTAP nodes:

```yaml
# Harvest Configuration File
Exporters:
  prometheus:
    exporter: Prometheus
    local_http_addr: 0.0.0.0
    port: {{.PORT}}
  service_control:
    exporter: ServiceControl
    url: {{.SERVICE_CONTROL_URL}}
    service_name: {{.SERVICE_NAME}}
    mappings:
      volume:
        - source: "capacity"
          target: "netapp.googleapis.com/volume/allocated_bytes"
        - source: "read_ops"
          target: "netapp.googleapis.com/volume/operation_count"
          labels:
            type: "read"
        - source: "read_latency"
          target: "netapp.googleapis.com/volume/average_latency"
          labels:
            method: "read"

Pollers:
  {{.POLLER_NAME}}:
    datacenter: {{.DATACENTER}}
    addr: {{.NODE_IP}}
    auth_style: {{.AUTH_STYLE}}
    username: {{.USERNAME}}
    password: {{.PASSWORD}}
    labels:
      - project: {{.PROJECT}}
      - tenant_project: {{.TENANT_PROJECT}}
      - deployment_name: {{.DEPLOYMENT_NAME}}
    use_insecure_tls: true
    exporters:
      - service_control
      - prometheus
```

### 14.4 Node Unregistration Process

#### 14.4.1 Unregistration Workflow
```go
type unRegisterNodeFromHarvestFarmParams struct {
    PoolID            int64
    CustomerProjectID string
    TenantProjectID   string
}
```

#### 14.4.2 Unregistration Steps

**Step 1: Validate and Get Nodes**
```go
// Gets nodes in deleted state that need to be unregistered
err = workflow.ExecuteActivity(ctx, unRegisterActivity.ValidateAndGetNodes, activityParams).Get(ctx, &dbNodes)
```

**Step 2: Get Node Group Mappings**
```go
// Retrieves node-to-group mappings that are not soft deleted
err = workflow.ExecuteActivity(ctx, unRegisterActivity.GetNodeGroupMapping, activityParams).Get(ctx, &nodeGroupMap)
```

**Step 3: Delete Pollers from Harvest**
```go
// Removes pollers from Harvest Farm service
deletePollerRestResponse = func(ctx context.Context, url string) (*http.Response, error) {
    req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
    if err != nil {
        return nil, err
    }
    client := &http.Client{}
    return client.Do(req)
}
```

**Step 4: Clean Up Kubernetes Leases**
```go
// Removes Kubernetes leases for the unregistered nodes
deleteKubernetesLease = utils.DeleteKubernetesLease
```

### 14.5 OpenTelemetry Integration

#### 14.5.1 Collection Pipeline
The Harvest Farm uses OpenTelemetry Collector to process and forward metrics:

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: 'otel-collector'
          scrape_interval: 300s
          http_sd_configs:
            - url: http://localhost:3000/pollers/prometheus-targets
              refresh_interval: 30s

processors:
  batch:
  groupbyattrs:
    keys:
      - tenant_project
  transform:
    error_mode: ignore
    metric_statements:
      - set(resource.attributes["gcp.project.id"], resource.attributes["tenant_project"])
      - delete_key(resource.attributes, "tenant_project")

exporters:
  googlecloud:
    project: "netapp-au-se1-autopush-sde-tst"
    metric:
      prefix: custom.googleapis.com
      skip_create_descriptor: true
    sending_queue:
      enabled: true
      queue_size: 40000
```

#### 14.5.2 Service Discovery
- **Dynamic Targets**: Harvest Farm provides Prometheus targets via HTTP service discovery
- **Auto-scaling**: New pollers are automatically discovered and added to collection
- **Health Monitoring**: Failed pollers are automatically removed from targets

### 14.6 High Availability and Resilience

#### 14.6.1 Node Group Strategy
- **Redundancy**: Two nodes assigned to different groups for fault tolerance
- **Load Balancing**: Distributes collection load across multiple pollers
- **Failover**: Automatic failover between nodes in the same group

#### 14.6.2 Kubernetes Lease Management
```go
// Lease-based coordination prevents conflicts
const leasePrefix = "harvest-"
vcpLeaseNameSpace = env.GetString("LEASE_NAMESPACE", "vcp")

// RBAC permissions for lease management
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

#### 14.6.3 Error Handling
- **Non-retryable Errors**: Configuration errors that require manual intervention
- **Retryable Errors**: Temporary network or service issues
- **Graceful Degradation**: Continue with available nodes if some fail

### 14.7 Security and Authentication

#### 14.7.1 ONTAP Authentication
```go
// Supports multiple authentication methods
mapping.HarvestConfig.AUTH_TYPE = pool.PoolCredentials.AuthType
mapping.HarvestConfig.SECRET_ID = pool.PoolCredentials.SecretID
mapping.HarvestConfig.SECRET_PROJECT = env.SecretManagerProjectID

// Fallback to password-based auth
if !smHarvestAuthEnabled && credentials != nil {
    mapping.HarvestConfig.PASSWORD = strconv.Quote(credentials.AdminPassword)
}
```

#### 14.7.2 TLS and Network Security
- **Insecure TLS**: Disabled for internal ONTAP connections
- **Service Accounts**: Kubernetes service accounts for API access
- **Network Policies**: Restricted network access between components

### 14.8 Monitoring and Observability

#### 14.8.1 Harvest Farm Metrics
- **Poller Health**: Active/inactive poller status
- **Collection Rates**: Metrics collection frequency and success rates
- **Error Rates**: Failed collections and authentication errors

#### 14.8.2 Integration with Telemetry
- **Real-time Data**: Supplements batch collection with real-time metrics
- **Data Correlation**: Links Harvest metrics with VCP database records
- **Unified Reporting**: Combines real-time and historical data for comprehensive analytics

### 14.9 Data Models

#### 14.9.1 Node Group Assignment
```go
type NodeGroupAssignmentParams struct {
    Node1            *Node
    Node2            *Node
    MaxNodesPerGroup int
    CustomerProject  string
    TenantProject    string
    DeploymentName   string
}
```

#### 14.9.2 Harvest Configuration
```go
type HarvestConfig struct {
    POLLER_NAME      string
    DATACENTER       string
    NODE_IP          string
    AUTH_STYLE       string
    USERNAME         string
    PASSWORD         string
    AUTH_TYPE        string
    SECRET_ID        string
    SECRET_PROJECT   string
    PROJECT          string
    TENANT_PROJECT   string
    DEPLOYMENT_NAME  string
    LEASE_NAME       string
    PORT             string
}
```

### 13.3 Scalability Roadmap
- **Horizontal Partitioning**: Shard metrics by time or tenant
- **Read Replicas**: Separate read/write workloads
- **Caching Layer**: Redis for frequently accessed metadata
- **Stream Processing**: Apache Kafka for real-time metrics

---
