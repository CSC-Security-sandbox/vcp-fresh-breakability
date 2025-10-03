# ADR-0011: Slog Logging Framework for VSA Control Plane

## Status
Accepted

## Context

The VSA Control Plane requires a robust, structured logging solution that provides:
- Enhanced traceability across distributed services
- Integration with OpenTelemetry for distributed tracing
- Flexible configuration for different environments
- Correlation of logs across HTTP requests and Temporal workflows
- Support for Google Cloud Logging conventions
- Proper error handling and debugging capabilities

## Investigation & High-level requirements

We evaluated logging frameworks against the following requirements:
- Scalability: handle high-volume logs without impacting application latency
- Reliability: durable behavior during outages and graceful degradation
- Flexibility: pluggable handlers (JSON, OTEL, stdout/file) and configurable levels
- Standardised log format: structured JSON, ISO-8601 timestamps, consistent field names
- Correlation: request / workflow correlation ID propagated across services
- Contextual information: include service name, operation, workflow/job IDs and user activity
- Sensitive data protection: avoid logging PII/credentials; provide selective redaction
- Exception logging: include stack traces and error codes where appropriate
- Centralisation & retention: centralised sink (Cloud Logging) with a 90‑day retention and archival to GCS

We ran a focused comparison of two candidates (Zap and slog) aligned to these requirements.

## Alternatives considered

### Zap
Pros:
- High throughput and low allocation logging designed for high-volume production workloads
- Highly configurable and battle-tested in many Go projects
- Native JSON output and performant ergonomics

Cons:
- More complex API and configuration surface; higher learning curve for developers
- Integration with context-based structured logging and OpenTelemetry requires additional glue

### slog
Pros:
- Native context-aware logging (loggers can carry contextual state across API boundaries)
- Modular handler/formatter design (easy to plug JSON or OpenTelemetry handlers)
- Simpler, more idiomatic API for context propagation; adds minimal cognitive overhead
- Supported in Go standard library area (available with Go 1.21+ adoption)

Cons:
- Smaller community and fewer third-party integrations compared to Zap
- Slightly lower raw throughput in microbenchmarks versus Zap (acceptable with mitigations)

Conclusion from investigation:
- Both libraries satisfy structured JSON logging and OpenTelemetry compatibility. For VCP the decisive factors are context propagation (for correlation across HTTP + Temporal workflows), simpler developer ergonomics, and compatibility with Go 1.21+ features. slog provides built-in, idiomatic support for these patterns and reduces glue code, so it was chosen.

## Decision

We will use the **slog** framework as the primary logging solution for the VSA Control Plane. Rationale:
- Native context propagation simplifies propagating correlation IDs and workflow context into logs.
- Modular handlers let us emit Google Cloud compatible JSON and also export into OpenTelemetry when required.
- Simpler developer API reduces risk during migration and instrumentation across many services.
- Performance concerns can be addressed with mitigations (buffered handlers, sampling, async export).

### Core Components

1. **Logger Interface** (`utils/middleware/log/logger.go`)
   - Defines a consistent logging API across all services
   - Supports context-aware logging with correlation IDs
   - Provides structured logging with key-value pairs

2. **Slogger Implementation** (`utils/middleware/log/slogger.go`)
   - Implements the Logger interface using Go's `log/slog` package
   - Supports JSON and OpenTelemetry handlers
   - Integrates with Google Cloud Logging conventions
   - Includes span context for distributed tracing

3. **Logging Middleware** (`utils/middleware/log/logger.go`)
   - Injects request-specific loggers into HTTP context
   - Extracts correlation IDs and request metadata
   - Propagates logging context to Temporal workflows

4. **GORM Integration** (`database/utils/logger/logger.go`)
   - Custom GORM logger that integrates with the slog framework
   - Maintains request context in database operations
   - Supports configurable log levels and slow query detection

### Key Features

#### 1. Structured Logging
- All logs are output in JSON format for easy parsing and analysis
- Consistent field naming following Google Cloud Logging conventions
- Support for nested field grouping and key-value pairs

#### 2. Context Propagation
- Request correlation IDs are automatically propagated through the system
- Temporal workflow context is maintained across activities
- OpenTelemetry trace and span IDs are included in log entries

#### 3. Environment Configuration
The logging framework is configured via environment variables:
# Core logging configuration
LOGGER_LEVEL=info                    # Log level (debug, info, warn, error)
LOGGER_TYPE=slog                     # Logger type (currently only slog supported)
SLOG_HANDLER_TYPE=json               # Handler type (json, otelslog)
EXPORTER_TYPE=stdout                 # Exporter type (stdout, file, etc.)
### Performance mitigation strategies
- Use async/buffered handlers for networked exporters to avoid blocking application threads.
- Apply sampling on debug-level/high-volume events where appropriate (retain full traces for errors).
- Use structured fields sparingly for hot paths; prefer pre-constructed context fields rather than frequent map allocations.
- In critical hot-loops, allow toggling to a reduced log level or no-op logger via configuration.
### Log format and fields (standardized)
All services MUST produce structured JSON logs with ISO‑8601 timestamps and the fields below where applicable:
- timestamp: ISO-8601 UTC
- severity / level
- message
- service.name: logical service name (core-api, worker, google-proxy)
- build.version: build tag or git commit hash
- correlation_id: propagated request/workflow correlation id
- trace/span: OpenTelemetry trace/span ids mapped to Google Cloud Logging fields when available
- workflow_id / job_id: Temporal workflow identifiers when applicable
- user_activity: high-level user action (CreateVolume, DeleteSnapshot, etc.)
- user_id / account_id (only if non-sensitive and required) — ensure PII policies are followed
- request: { method, path, remote_address }
- error: { code, message, stack } for errors
Example log entry:
  "timestamp": "2025-10-02T15:30:45.123456Z",
  "message": "Received incoming HTTP request",
  "service.name": "google-proxy",
  "build.version": "v1.2.3+commit",
  "correlation_id": "9ea8f2b4-639e-4de7-b406-f6cd3a155e9f",
  "user_activity": "CreateVolume",
  "user_id": "12345",
  "request": { "method": "POST", "path": "/v1/projects/.../volumes", "remote_address": "192.168.1.100" }

### Sensitive data and PII handling
- Default behavior: do not log secrets, passwords, or PII fields. Use redaction helpers for allowed structured fields.
- Logging middleware MUST filter sensitive fields from request/response bodies before emitting logs. Provide a safe allow-list and redaction utilities in the logging utils package.

### Log retention and archival
- Default retention policy: 90 days in the centralised logging store.
- Archive logs older than 90 days to Google Cloud Storage (GCS) for cost-effective long-term storage and forensic needs.
- Define automated lifecycle jobs to move logs at the retention boundary and to verify archival integrity.

### Operational notes
- Correlation ID assignment: assign at ingress (google-proxy / load balancer) if client does not supply X-Correlation-ID. Propagate via headers and Temporal workflow metadata.
- Investigate passing correlation ID to VSA clusters for uniform tracing; treat as an open question that requires storage/networking team input.

### Open questions
- Can we safely pass correlation IDs into VSA/ONTAP logs? (network and storage ops to confirm security and PII implications)
- Determine exact sampling strategy for high-volume debug logs vs. error-level full traces.
- Decide retention SLA beyond 90 days for regulatory or legal needs (if any).

1. **Core Services**: All core services (core-api, worker, google-proxy) use the logging framework
2. **Temporal Integration**: Workflows and activities have access to contextual loggers
3. **Database Integration**: GORM operations are logged with request context
4. **HTTP Transport**: ONTAP REST client includes request/response logging

### Future Enhancements

1. **Advanced OpenTelemetry Features**:
   - Enhanced trace correlation
   - Custom metrics integration
   - Advanced sampling strategies

2. **Additional Exporters**:
   - File-based logging
   - Database logging
   - Custom log aggregation endpoints

3. **Log Archival**:
   - Automated log rotation and cleanup

   - Real-time log analysis
   - Automated alerting based on log patterns
   - Performance metrics from log data

### Configuration Examples

#### Development Environment
```bash
LOGGER_LEVEL=debug
SLOG_HANDLER_TYPE=json
ADD_LOG_SOURCE_FILE=true
EXPORTER_TYPE=stdout
```

#### Production Environment
```bash
LOGGER_LEVEL=info
SLOG_HANDLER_TYPE=json
ADD_LOG_SOURCE_FILE=false
EXPORTER_TYPE=stdout
OTEL_SERVICE_NAME=VCP-VSA
OTEL_GOOGLE_PROJECT_ID=production-project-id
```

#### Staging Environment
func FunctionA() {
    if err := FunctionB(); err != nil {
        log.Error("FunctionA failed", err)
    }
}
```

#### 5. Debug Logging Enhancement
Debug logs include source file path and line number for precise debugging:

```go
// Debug logs automatically include source information when ADD_LOG_SOURCE_FILE=true
logger.Debug("Processing request", "requestID", reqID, "userID", userID)
```

### Integration Points

#### 1. HTTP Middleware
```go
// LoggingMiddleware injects a logger into the request context
func LoggingMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        logger, logFields := NewRequestLogger(r)
        ctx := context.WithValue(r.Context(), middleware.ContextSLoggerKey, logger)
        ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logFields)
        r = r.WithContext(ctx)
        next.ServeHTTP(w, r)
    })
}
```

#### 2. Temporal Workflow Integration
```go
// Logger is extracted from workflow context
func (wf *BaseWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
    logger := util.GetLogger(ctx)
    logger.Info("Starting workflow", "workflowID", wf.ID)
    // ... workflow logic
}
```

#### 3. Database Operations
```go
// GORM logger maintains request context
func (l *GormSlogLogger) Info(ctx context.Context, msg string, data ...interface{}) {
    if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
        l.Slogger.WithFields("requestFields", loggerFields).InfoContext(ctx, msg, data...)
    } else {
        l.Slogger.InfoContext(ctx, msg, data...)
    }
}
```

### Log Format

All logs follow this structured format:

```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "severity": "INFO",
  "message": "Processing request",
  "logging.googleapis.com/trace": "projects/project-id/traces/trace-id",
  "logging.googleapis.com/spanId": "span-id",
  "logging.googleapis.com/trace_sampled": true,
  "requestFields": {
    "x-request-id": "req-123",
    "x-correlation-id": "corr-456"
  },
  "workflowID": "op-789",
  "activityName": "CreateVolumeActivity"
}
```

## Consequences

### Positive
- **Enhanced Traceability**: Correlation IDs enable tracking requests across services
- **Structured Data**: JSON logs are easily parseable by log aggregation systems
- **OpenTelemetry Integration**: Seamless integration with distributed tracing
- **Flexible Configuration**: Environment-based configuration for different deployments
- **Consistent API**: Single logging interface across all services
- **Google Cloud Compatibility**: Logs follow Google Cloud Logging conventions

### Negative
- **Learning Curve**: Developers need to understand structured logging concepts
- **Performance Overhead**: JSON serialization adds slight overhead
- **Configuration Complexity**: Multiple environment variables need to be managed
- **Debug Information**: Source file information adds overhead in production

### Neutral
- **Migration Effort**: Existing log statements need to be updated to use structured logging
- **Tooling Requirements**: Log analysis tools need to support JSON parsing

## Implementation Details

### Current Implementation Status
The slog logging framework is already implemented and integrated across the VSA Control Plane:
### Log Format
5. **OpenTelemetry**: Basic tracing integration is implemented
All logs follow this structured format:
   - Integration with Google Cloud Storage for log archival
```json
   - Compliance and retention policies
  "timestamp": "2024-01-15T10:30:45.123Z",
4. **Enhanced Monitoring**:
  "message": "Processing request",
  "logging.googleapis.com/trace": "projects/project-id/traces/trace-id",
  "logging.googleapis.com/spanId": "span-id",
  "logging.googleapis.com/trace_sampled": true,
  "requestFields": {
    "x-request-id": "req-123",
    "x-correlation-id": "corr-456"
  },
  "workflowID": "op-789",
  "activityName": "CreateVolumeActivity"
```bash
```
LOGGER_LEVEL=debug
SLOG_HANDLER_TYPE=json
ADD_LOG_SOURCE_FILE=true
EXPORTER_TYPE=stdout
OTEL_SERVICE_NAME=VCP-VSA-staging
OTEL_GOOGLE_PROJECT_ID=staging-project-id
```

## References

- [Go slog Package Documentation](https://pkg.go.dev/log/slog)
- [OpenTelemetry Go Documentation](https://opentelemetry.io/docs/instrumentation/go/)
- [Google Cloud Logging Documentation](https://cloud.google.com/logging/docs)
- [Temporal Go SDK Documentation](https://docs.temporal.io/docs/go/)
- [GORM Logger Documentation](https://gorm.io/docs/logger.html)

## Related ADRs

- [ADR-0010: Temporal as Orchestrator Engine](./0010-temporal-as-orchestrator-engine.md)
- [ADR-0002: Database Choice for VCP](./0002-database-choice-for-vcp.md)
- [ADR-0004: Use Chi as Go Server Framework](./0004-use-chi-as-go-server-framework.md)