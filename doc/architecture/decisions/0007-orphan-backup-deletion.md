# ADR 7: Orphan Backup Deletion Architecture for VSA Control Plane

## Context
The **Abstract Data Container (ADC)**, managed by NetApp, is a critical component in the VSA Control Plane that handles backup deletion operations when volumes are deleted. This document outlines the evolution from a traditional container-based approach to a serverless **Cloud Run** implementation to address scalability and concurrency limitations.

---

## Problem Statement

### **Original ADC Container Limitations**
The original ADC implementation used a traditional **Kubernetes StatefulSet** deployment with the following constraints:
- **Single Request Processing**: ADC containers could only handle one request at a time.
- **Bottleneck Issues**: When multiple backup deletion requests arrived in parallel, they would queue up, creating performance bottlenecks.
- **Resource Inefficiency**: Containers remained running even when idle, consuming resources unnecessarily.
- **Scaling Challenges**: Manual scaling was required to handle increased load.

### **Impact on Operations**
- **Delayed Backup Deletions**: Parallel requests would experience significant delays.
- **Resource Waste**: Idle containers consumed CPU and memory resources.
- **Operational Overhead**: Manual intervention required for scaling during peak loads.
- **Customer Experience**: Slower response times for backup operations.

---

## Solution: Cloud Run Implementation

### **Architecture Overview**
The new implementation leverages **Google Cloud Run** to provide a serverless, event-driven approach for ADC operations:

---

### **Workflow Diagram**
The backup deletion workflow is enhanced with the following steps:
1. **Resource Preparation**: Generate timestamps and retrieve bucket details.
2. **Service Account Creation**: Create temporary service accounts for ADC operations.
3. **Role Assignment**: Attach necessary GCP roles for storage operations.
4. **HMAC Key Creation**: Generate access credentials for object storage.
5. **Cloud Run Deployment**: Deploy ADC service to Cloud Run.
6. **Service Verification**: Wait for the service to be ready and retrieve the URL.
7. **Backup Deletion**: Execute ADC deletion operations.
8. **Cleanup**: Remove Cloud Run service and service accounts.

---

### **Key Components**

#### **1. ADC Workflow**
The main orchestrator manages the entire backup deletion process through coordinated steps:
- **Cloud Run Operations**:
  - Deploy ADC container to Cloud Run.
  - Retrieve the service URL.
  - Monitor deployment status.
  - Remove the service after completion.
- **Authentication Operations**:
  - Create temporary service accounts.
  - Assign necessary permissions.
  - Generate storage access credentials.
  - Clean up permissions.
- **ADC Operations**:
  - Initiate backup deletion.
  - Monitor deletion progress.

#### **2. Cloud Run Service**
Google Cloud Run provides the following features:
- **Ephemeral Deployment**: Services are created on-demand and destroyed after use.
- **Automatic Scaling**: Handles scaling automatically based on demand.
- **Resource Isolation**: Each request gets its own isolated environment.
- **Cost Optimization**: Pay only for actual usage time.

---

### **Implementation Details**

#### **Configuration**
- **Environment Variables**:
  - ADC image configuration.
  - Region and project settings.
  - Provider type and storage URL.
  - Port configurations.
- **Polling Configuration**:
  - Redirect URL trigger intervals.
  - Maximum polling attempts.
  - Cloud Run deployment timeout settings.

#### **Cloud Run Service Configuration**
- **Project and Location**: GCP project and region settings.
- **Service Naming**: Unique service names with timestamps.
- **Labels and Annotations**: Metadata for resource management.
- **Environment Variables**: ADC-specific configuration.
- **Volume Mounts**: Certificate and configuration mounting.
- **Resource Limits**: CPU and memory constraints.

---

### **Security Implementation**

#### **Service Account Management**
- **Temporary Creation**: Service accounts are created with unique timestamps.
- **Minimal Permissions**: Only necessary roles are assigned.
- **Automatic Cleanup**: Service accounts are deleted after operations complete.

#### **Required Roles**
- Storage HMAC key administration.
- Object storage administration.
- Storage administration.
- Service account administration.

#### **HMAC Key Management**
- **Base64 Encoding**: Keys are encoded to avoid storing sensitive data in Temporal DB.
- **Temporary Usage**: Keys are used only for the duration of the operation.
- **Automatic Cleanup**: Keys are automatically cleaned up after use.

#### **Identity Token Authentication**
- **IAM Authentication**: Cloud Run services are configured with IAM authentication enabled.
- **Identity Token Generation**: The system generates Google Cloud identity tokens for secure communication.
- **Bearer Token Authorization**: HTTP requests to Cloud Run include Bearer tokens for authentication.
- **Secure Communication**: All communication between VSA Control Plane and Cloud Run is authenticated and encrypted.

---

### **Error Handling and Rollback**

#### **Rollback Manager**
- Automatic cleanup of resources on failure.
- Disconnected context execution for cleanup operations.
- Comprehensive error handling and logging.

#### **Automatic Cleanup**
- Cloud Run services are automatically deleted after operations.
- Service accounts are cleaned up regardless of success/failure.
- HMAC keys are removed to prevent security issues.

---

## Benefits of Cloud Run Implementation

### **1. Scalability**
- **Automatic Scaling**: Cloud Run automatically scales from 0 up to 1000 instances per service by default (limit may be increased by quota request). [See Cloud Run Quotas](https://cloud.google.com/run/quotas)
- **Concurrent Processing**: Multiple requests can be processed simultaneously.
- **No Bottlenecks**: Each request gets its own isolated environment.

### **2. Cost Optimization**
- **Pay-per-Use**: Only charged for actual processing time.
- **No Idle Resources**: No costs when no requests are being processed.
- **Resource Efficiency**: Optimal resource utilization.

### **3. Operational Benefits**
- **Reduced Maintenance**: No need to manage container lifecycle.
- **Automatic Updates**: Cloud Run handles infrastructure updates.
- **Built-in Monitoring**: Integrated logging and monitoring.

### **4. Security Enhancements**
- **Isolated Execution**: Each request runs in its own secure environment.
- **Temporary Credentials**: Short-lived service accounts and HMAC keys.
- **IAM Authentication**: Secure communication using identity tokens.
- **Automatic Cleanup**: Resources are automatically cleaned up.

---

## Current Implementation Status

**Fully Implemented and Active** - The Cloud Run implementation for ADC operations has been successfully implemented and is actively handling orphan backup deletion operations.

### Current Implementation Details

#### 1. ADC Workflow Implementation
The `ADCWorkflow` is fully implemented and integrated into the backup deletion process:

```go
// Current implementation in adc_workflow.go
func ADCWorkflow(ctx workflow.Context, params *common.DeleteBackupParams, 
    backupVault *datamodel.BackupVault, backup *datamodel.Backup, 
    account *datamodel.Account) (bool, error) {
    // Full Cloud Run ADC implementation with:
    // - Service account creation and management
    // - HMAC key generation and management
    // - Cloud Run service deployment
    // - ADC deletion operations with polling
    // - Automatic cleanup of resources
}
```

#### 2. Orphan Backup Detection and Handling
The backup deletion workflow now properly detects and handles orphaned backups:

```go
// Current implementation in backup_workflow.go
if isVolumeDeleted || isSnapmirrorDeleted {
    cloudDeletionIntiated := false
    // if volume is deleted then we need to delete the backup with adc
    err = workflow.ExecuteChildWorkflow(ctx, ADCWorkflow, deleteBackupParams, 
        dbBackupVault, dbBackup, account).Get(ctx, &cloudDeletionIntiated)
    if err != nil {
        wf.Logger.Errorf("Backup deletion failed with ADC, backupUUID: %s, error: %v", 
            dbBackup.UUID, err)
        if cloudDeletionIntiated {
            wf.deleteInitiated = true
        }
    }
}
```

#### 3. Cloud Run ADC Service Configuration
- **Environment Variables**: Configurable ADC image, region, project, and storage settings
- **Service Account Management**: Automatic creation and cleanup of temporary service accounts
- **Role Assignment**: Required GCP roles for storage operations
- **HMAC Key Management**: Secure key generation and cleanup
- **Cloud Run Deployment**: On-demand service deployment with automatic scaling

#### 4. ADC Workflow Steps
1. **Service Account Creation**: Creates temporary service accounts with unique timestamps
2. **Role Assignment**: Attaches necessary GCP roles for storage operations
3. **HMAC Key Creation**: Generates access credentials for object storage
4. **Cloud Run Deployment**: Deploys ADC service to Cloud Run with proper configuration
5. **Service Verification**: Waits for service readiness and retrieves service URL
6. **Backup Deletion**: Executes ADC deletion operations with polling mechanism
7. **Cleanup**: Removes Cloud Run service and service accounts automatically

#### 5. Polling and Status Management
- **Redirect Handling**: Follows HTTP 307 redirects for long-running operations
- **Status Polling**: Configurable polling intervals and maximum attempts
- **Error Handling**: Comprehensive error handling for various HTTP status codes
- **Timeout Management**: Configurable timeouts for different operations

#### 6. Security Implementation
- **Temporary Credentials**: Short-lived service accounts and HMAC keys
- **IAM Authentication**: Cloud Run services configured with IAM authentication
- **Identity Token Generation**: Secure communication using Google Cloud identity tokens
- **Automatic Cleanup**: Resources are automatically cleaned up after operations

### Key Features Implemented

1. **Cloud Run Integration**: Full Cloud Run deployment and management for ADC operations
2. **Orphan Backup Detection**: Automatic detection of orphaned backups (volume deleted)
3. **ADC Workflow Execution**: Complete ADC workflow for orphan backup deletion
4. **Service Account Management**: Temporary service account creation and cleanup
5. **HMAC Key Management**: Secure key generation and automatic cleanup
6. **Polling Mechanism**: Robust polling for long-running ADC operations
7. **Error Handling**: Comprehensive error handling with rollback capabilities
8. **Resource Cleanup**: Automatic cleanup of all created resources

### Current Architecture Benefits

#### 1. Scalability
- **Automatic Scaling**: Cloud Run automatically scales based on demand
- **Concurrent Processing**: Multiple orphan backup deletion requests can be processed simultaneously
- **No Bottlenecks**: Each request gets its own isolated Cloud Run environment

#### 2. Cost Optimization
- **Pay-per-Use**: Only charged for actual processing time
- **No Idle Resources**: No costs when no requests are being processed
- **Resource Efficiency**: Optimal resource utilization through serverless execution

#### 3. Security
- **Isolated Execution**: Each request runs in its own secure environment
- **Temporary Credentials**: Short-lived service accounts and HMAC keys
- **IAM Authentication**: Secure communication using identity tokens
- **Automatic Cleanup**: Resources are automatically cleaned up

#### 4. Operational Benefits
- **Reduced Maintenance**: No need to manage container lifecycle
- **Automatic Updates**: Cloud Run handles infrastructure updates
- **Built-in Monitoring**: Integrated logging and monitoring
- **Error Recovery**: Comprehensive error handling and rollback mechanisms

## Conclusion
The Cloud Run implementation for ADC operations has been successfully implemented and is actively handling orphan backup deletion operations. The system now provides unlimited scalability, cost efficiency, and operational simplicity while maintaining security and reliability.

### **Key Benefits Achieved**
- **Unlimited Scalability**: Automatic scaling based on demand with Cloud Run
- **Cost Efficiency**: Pay only for actual usage with no idle resource costs
- **Operational Simplicity**: Reduced maintenance overhead with serverless architecture
- **Enhanced Security**: Temporary credentials, IAM authentication, and isolated execution
- **Better Performance**: Consistent response times regardless of load
- **Orphan Backup Handling**: Complete solution for orphaned backup deletion
- **Resource Management**: Automatic cleanup of all created resources

### **Implementation Success**
- **Full ADC Workflow**: Complete implementation of ADC workflow for orphan backup deletion
- **Cloud Run Integration**: Successful integration with Google Cloud Run services
- **Security Implementation**: Comprehensive security with temporary credentials and IAM
- **Error Handling**: Robust error handling with rollback capabilities
- **Monitoring**: Full monitoring and logging capabilities

This architecture has successfully positioned the VSA Control Plane for scalable orphan backup deletion and provides a solid foundation for additional serverless implementations across the platform.
