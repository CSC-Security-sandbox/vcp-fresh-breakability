# #VCP Observability Design Document
This document outlines the design considerations and implementation strategies for observability within the VCP infrastructure. 
The goal is to ensure comprehensive monitoring, logging, and tracing capabilities to enhance system reliability, performance, and user experience.

## 1. Objectives
- **Comprehensive Monitoring**: Implement monitoring solutions to track system health, performance metrics, and resource utilization.
- **Centralized Logging**: Establish a centralized logging system to aggregate logs from various services for easier access and analysis.
- **Distributed Tracing**: Enable distributed tracing to track requests across microservices, facilitating root cause analysis.
- **Alerting and Notifications**: Set up alerting mechanisms to notify the team of critical issues in real-time.

## 2. Monitoring
### 2.1. Metrics Collection
- **Prometheus & OpenTelemetry**: All microservices in the VCP workspace expose metrics in a Prometheus-compatible format, instrumented using OpenTelemetry standards, enabling centralized metrics collection and storage via Prometheus.
### 2.2. Metrics Exporting
- **Google Cloud Monitoring**: Metrics collected via OpenTelemetry are exported to Google Cloud Monitoring (Stackdriver) using the OTEL Collector. The collector is configured to scrape Prometheus endpoints from all microservices and forward metrics to Stackdriver for unified monitoring and alerting.
### 2.3. Dashboards
#### - **Google Cloud Monitoring Dashboards**: Metrics exported to Google Cloud Monitoring(Stackdriver) are visualized using Google Cloud Monitoring dashboards, providing real-time insights into system performance and health
   **Product Team Dashboard**: High-level metrics and KPIs for product management and business decisions.
   **Engineering Leaders Dashboard**: Aggregated system health, reliability, and performance metrics for leadership oversight.
   **Engineers Dashboard**: Detailed technical metrics and analysis for troubleshooting, optimization, and deep dives.

## 3. Design Rationale

- **OpenTelemetry Adoption**: Chosen for industry-standard metrics instrumentation, ensuring compatibility and extensibility across cloud providers.
- **Prometheus Format**: Enables seamless integration with existing monitoring tools and supports flexible scraping by the OTEL Collector.
- **Stackdriver Export**: Centralizes metrics in Google Cloud Monitoring for unified alerting and dashboarding, aligning with cloud-native best practices.
- **Extensibility**: The observability stack is designed to support future integrations (e.g., additional cloud providers, custom exporters) with minimal changes.

## 4. High-Level Observability Architecture Diagram
![Architecture Diagram](doc/infrastructure/design/images/highlevelmetric.PNG)


## 5. Implementation Diagram
![Implementation Diagram](doc/infrastructure/design/images/flow.PNG)

## 6. Metrics Tracked
### 6.1. Workflow level Metrics
- **Workflow Execution Count**: Total number of workflow executions initiated, categorized by workflow type.
- **Workflow Success Rate**: Percentage of successfully completed workflows versus total initiated workflows.
- **Average Workflow Duration**: Average time taken for workflow completion, segmented by workflow type.
- **Workflow Failure Rate**: Percentage of workflows that failed during execution, categorized by failure reasons
- **Workflow Queue Time**: Average time workflows spend in the queue before execution starts.

### 6.2. Temporal Cluster Metrics
- **Task Queue Depth**: Number of tasks waiting in the queue for processing.
- **Worker Activity**: Number of active workers processing tasks in the Temporal cluster.
- **Temporal API Latency**: Average response time for Temporal API calls.
- **Temporal System Health**: Metrics indicating the health and performance of the Temporal cluster (e.g., CPU, memory usage).
- **Temporal Task Failures**: Number and types of task failures within the Temporal cluster.

### 6.3. Custom Metrics
- **Custom Business Metrics**: Any additional metrics specific to business logic or application requirements, such as customer adoption or volumes availability.
