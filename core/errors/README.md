# VSA Control Plane Custom Error System

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Core Components](#core-components)
  - [Error Categories](#error-categories)
- [CustomError Structure](#customerror-structure)
- [Best Practices](#best-practices)
  - [Error Creation](#1-error-creation)
  - [Error Handling](#2-error-handling)
  - [HTTP Response Handling](#3-http-response-handling)
  - [Logging and Debugging](#4-logging-and-debugging)
  - [Temporal Workflow Integration](#5-temporal-workflow-integration)
- [Error Code Management](#error-code-management)
- [Testing Best Practices](#testing-best-practices)
- [Error Wrapping Best Practices](#error-wrapping-best-practices)
- [Common Patterns](#common-patterns)
- [Performance Considerations](#performance-considerations)
- [Security Considerations](#security-considerations)
- [Troubleshooting](#troubleshooting)
- [Migration Guide](#migration-guide)

## Overview

The VSA Control Plane implements a comprehensive custom error handling system that provides structured error management, tracking, and integration with Temporal workflows. This system extends Go's standard error handling with additional capabilities for error categorization, retry logic, HTTP status codes, and user-friendly error messages.

> **Note**: The code examples in this documentation assume you have the necessary imports. For HTTP handling examples, you'll need `net/http` and `encoding/json`. For Temporal workflows, you'll need `go.temporal.io/sdk/workflow`. For logging, you'll need `log/slog` or your preferred logging package.

## Architecture

### Core Components

1. **CustomError**: The main error type that wraps standard errors with additional metadata
2. **Error Codes**: Numeric constants for categorizing different types of errors
3. **Error Messages**: Structured error information including descriptions, user messages, and metadata
4. **Temporal Integration**: Functions for wrapping errors as Temporal application errors

### Error Categories

The error system organizes errors into logical categories using numeric ranges:

- **1000-1999**: General/Workflow/Validation errors
- **2000-2999**: Database errors  
- **3000-3999**: GCP/Cloud errors
- **4000-4999**: VSA Cluster/Pool errors
- **5000-5999**: ONTAP errors
- **6100-6999**: Replication errors
- **7000-7999**: Snapshot/Volume errors
- **12000-12999**: Backup errors

## CustomError Structure

```go
type CustomError struct {
    TrackingID  int     // Unique numeric identifier for the error type
    Message     string  // User-friendly error message
    Retriable   bool    // Whether the operation can be retried
    HttpCode    *int    // HTTP status code for API responses
    OriginalErr error   // The underlying error that caused this
}
```

## Best Practices

### 1. Error Creation

**Use `NewVCPError` for creating custom errors:**

```go
// Good: Create a custom error with proper tracking
if err := someOperation(); err != nil {
    return errors.NewVCPError(errors.ErrDatabaseConnectionClosed, err)
}

// Avoid: Creating raw errors without context
if err := someOperation(); err != nil {
    return err // Loses context and tracking
}
```

**Always provide the original error:**

```go
// Good: Preserve the original error for debugging
if err := db.Query(); err != nil {
    return errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
}

// Avoid: Losing the original error context
if err := db.Query(); err != nil {
    return errors.NewVCPError(errors.ErrDatabaseDataReadError, nil)
}
```

**Using placeholders in error messages:**

```go
// JSON configuration example:
// {
//   "1001": {
//     "description": "Workflow configuration error with context",
//     "message": "Internal error occurred in %s: %s",
//     "retriable": false,
//     "http_code": 500
//   }
// }

// Good: Create error with placeholder arguments
if err := operation(); err != nil {
    return errors.NewVCPErrorWithArgs(
        errors.ErrWorkflowConfigurationError, 
        err, 
        "pool",           // First placeholder: %s
        "connection failed" // Second placeholder: %s
    )
}

// Good: Create error with single placeholder
if err := validatePool(poolName); err != nil {
    return errors.NewVCPErrorWithArgs(
        errors.ErrResourceNotFound, 
        err, 
        poolName // First placeholder: %s
    )
}

// Good: Reuse existing error with different arguments
baseErr := errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
poolErr := baseErr.WithArgs("pool", "validation failed")
```

**Avoid double wrapping errors:**

```go
// ❌ BAD: Double wrapping - loses original error context
if err := someOperation(); err != nil {
    customErr := errors.NewVCPError(errors.ErrDatabaseConnectionClosed, err)
    return errors.NewVCPError(errors.ErrWorkflowConfigurationError, customErr) // Double wrapped!
}

// ✅ GOOD: Single wrapping preserves original error
if err := someOperation(); err != nil {
    return errors.NewVCPError(errors.ErrDatabaseError, err)
}

// ✅ GOOD: If you need to change error type, check if already wrapped
if err := someOperation(); err != nil {
    var customErr *errors.CustomError
    if errors.As(err, &customErr) {
        // Error is already wrapped, just return it
        return err
    }
    // Error is not wrapped, wrap it now
    return errors.NewVCPError(errors.ErrDatabaseConnectionClosed, err)
}
```

**Preserve original errors when changing context:**

```go
// ✅ GOOD: When you need to change error context, preserve the original
func processDatabaseOperation() error {
    if err := db.Connect(); err != nil {
        // Wrap with database context
        return errors.NewVCPError(errors.ErrDatabaseConnectionClosed, err)
    }
    
    if err := db.Query(); err != nil {
        // Wrap with query context, but preserve the original database error
        return errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
    }
    
    return nil
}

// ✅ GOOD: Use error composition for complex scenarios
func complexOperation() error {
    if err := step1(); err != nil {
        // Create a new error with additional context
        contextErr := fmt.Errorf("step 1 failed: %w", err)
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, contextErr)
    }
    
    if err := step2(); err != nil {
        // Add step context while preserving original
        contextErr := fmt.Errorf("step 2 failed: %w", err)
        return errors.NewVCPError(errors.ErrInternalServerError, contextErr)
    }
    
    return nil
}
```

### 2. Error Handling

**Check error types using `Is` and `As`:**

```go
// Check if error is a specific type
if errors.Is(err, targetError) {
    // Handle specific error
}

// Extract custom error details
var customErr *errors.CustomError
if errors.As(err, &customErr) {
    if customErr.IsError(errors.ErrResourceNotFound) {
        // Handle resource not found
    }
    
    if customErr.IsRetriable() {
        // Implement retry logic
    }
}
```

**Handle retriable vs non-retriable errors:**

```go
var customErr *errors.CustomError
if errors.As(err, &customErr) {
    if customErr.IsRetriable() {
        // Implement exponential backoff and retry
        return retryWithBackoff(operation)
    } else {
        // Log and return immediately
        logger.Error("Non-retriable error occurred", "error", err)
        return err
    }
}
```

### 3. HTTP Response Handling

**Use HTTP codes for API responses:**

```go
func handleAPIRequest(w http.ResponseWriter, err error) {
    var customErr *errors.CustomError
    if errors.As(err, &customErr) {
        hasCode, httpCode := customErr.GetHttpCode()
        if hasCode {
            w.WriteHeader(httpCode)
        } else {
            w.WriteHeader(http.StatusInternalServerError)
        }
        
        // Return user-friendly message
        response := map[string]string{
            "error": customErr.GetMessage(),
        }
        json.NewEncoder(w).Encode(response)
    } else {
        // Fallback for non-custom errors
        w.WriteHeader(http.StatusInternalServerError)
        w.Write([]byte("Internal server error"))
    }
}
```

### 4. Logging and Debugging

**Use structured logging with error details:**

```go
func processRequest(ctx context.Context, req Request) error {
    if err := validateRequest(req); err != nil {
        customErr := errors.NewVCPError(errors.ErrInputValidationError, err)
        
        // Log with context
        logger.Error("Request validation failed",
            "tracking_id", customErr.TrackingID,
            "retriable", customErr.IsRetriable(),
            "original_error", customErr.OriginalErr.Error())
        
        return customErr
    }
    return nil
}
```

**Extract customer-facing messages:**

```go
func handleErrorResponse(err error) string {
    // Get user-friendly message
    message := errors.ExtractCustomerFacingErrorMessage(ctx, err)
    
    // Log technical details for debugging
    logger.Debug("Error details", "error", err, "user_message", message)
    
    return message
}
```

### 5. Temporal Workflow Integration

**Wrap errors for Temporal workflows:**

```go
func workflowActivity(ctx workflow.Context) error {
    if err := performOperation(); err != nil {
        // Wrap as Temporal application error
        return errors.WrapAsTemporalApplicationError(err)
    }
    return nil
}

func workflowActivityNonRetryable(ctx workflow.Context) error {
    if err := performOperation(); err != nil {
        // Wrap as non-retryable Temporal error
        return errors.WrapAsNonRetryableTemporalApplicationError(err)
    }
    return nil
}
```

**Extract errors from Temporal workflows:**

```go
func handleWorkflowError(err error) *errors.CustomError {
    // Extract custom error from Temporal application error
    return errors.ExtractCustomError(err)
}
```

## Error Code Management

### Adding New Error Codes

### Using Placeholders in Error Messages

The custom error system supports Go's `fmt.Sprintf` style placeholders in error messages. This allows you to create dynamic, context-aware error messages.

#### Placeholder Types Supported

- **`%s`**: String values
- **`%d`**: Integer values  
- **`%v`**: Default format for any value
- **`%+v`**: Detailed format for structs
- **`%#v`**: Go syntax representation
- **`%t`**: Boolean values
- **`%f`**: Floating point numbers

#### JSON Configuration Example

```json
{
  "1001": {
    "description": "Workflow configuration error with context",
    "message": "Internal error occurred in %s: %s",
    "retriable": false,
    "http_code": 500
  },
  "2001": {
    "description": "Database connection error with details",
    "message": "Failed to connect to database %s on host %s: %s",
    "retriable": true,
    "http_code": 503
  }
}
```

#### Usage Examples

```go
// Single placeholder
if err := validateResource(resourceName); err != nil {
    return errors.NewVCPErrorWithArgs(
        errors.ErrResourceNotFound, 
        err, 
        resourceName
    )
}

// Multiple placeholders
if err := connectToDatabase(dbName, host); err != nil {
    return errors.NewVCPErrorWithArgs(
        errors.ErrDatabaseConnectionClosed,
        err,
        dbName,    // First %s
        host,      // Second %s
        err.Error() // Third %s
    )
}

// Reusing errors with different arguments
baseErr := errors.NewVCPError(errors.ErrWorkflowConfigurationError, nil)
poolErr := baseErr.WithArgs("pool", "validation failed")
volumeErr := baseErr.WithArgs("volume", "creation failed")
```

#### Best Practices for Placeholders

1. **Use descriptive placeholders**: Make it clear what each placeholder represents
2. **Order matters**: Ensure placeholder order matches argument order
3. **Validate arguments**: Check that arguments match expected types
4. **Keep messages readable**: Don't overuse placeholders - keep messages human-readable
5. **Consistent formatting**: Use consistent placeholder patterns across related errors

#### Error Message Examples

```go
// Good: Clear and descriptive
"Failed to create volume %s in pool %s: %s"
"User %s attempted to access resource %s without permission"
"Operation %s failed after %d retries: %s"

// Avoid: Too many placeholders
"Error %s in %s for %s with %s: %s" // Hard to understand
```

### Adding New Error Codes

1. **Define the constant** in the appropriate range:

```go
const (
    ErrNewFeatureError = 1017  // Add to general errors (1000-1999)
)
```

2. **Update error message configuration** (referenced by `errorMap`):

```json
{
    "1017": {
        "description": "New feature encountered an error",
        "message": "Unable to process new feature request",
        "retriable": true,
        "http_code": 503
    }
}
```

### Error Code Naming Conventions

- **Prefix**: Use `Err` prefix for all error constants
- **Descriptive**: Make names clear and specific
- **Consistent**: Use consistent terminology across related errors
- **Grouped**: Keep related errors in the same numeric range

## Testing Best Practices

### Unit Testing Custom Errors

```go
func TestCustomError(t *testing.T) {
    originalErr := errors.New("database connection failed")
    customErr := errors.NewVCPError(errors.ErrDatabaseConnectionClosed, originalErr)
    
    // Test error properties
    assert.Equal(t, errors.ErrDatabaseConnectionClosed, customErr.TrackingID)
    assert.Equal(t, "database connection failed", customErr.OriginalErr.Error())
    assert.False(t, customErr.IsRetriable())
    
    // Test error interface implementation
    assert.Implements(t, (*error)(nil), customErr)
}

func TestErrorWrapping(t *testing.T) {
    originalErr := errors.New("validation failed")
    customErr := errors.NewVCPError(errors.ErrInputValidationError, originalErr)
    
    // Test error unwrapping
    unwrapped := errors.Unwrap(customErr)
    assert.Equal(t, originalErr, unwrapped)
}
```


## Error Wrapping Best Practices

### Understanding Error Wrapping

The custom error system uses Go's error wrapping mechanism to preserve the original error while adding context. Understanding how to properly wrap errors is crucial for maintaining error traceability.

### When Double Wrapping Might Be Necessary

While generally avoiding double wrapping is best practice, there are scenarios where you might need to change error context:

1. **Error Type Transformation**: Converting a database error to a workflow error
2. **Context Addition**: Adding workflow-specific context to existing errors
3. **Error Aggregation**: Combining multiple errors with a common context
4. **API Layer Translation**: Converting internal errors to user-facing errors

**Important**: When you must change error context, extract the original error rather than double wrapping.

**Key Principles:**
1. **Single Wrap**: Each error should be wrapped only once with `NewVCPError`
2. **Preserve Original**: Always pass the original error as the second parameter
3. **Context Addition**: Use `fmt.Errorf` with `%w` verb to add context before wrapping
4. **Check Before Wrap**: Verify if an error is already wrapped before wrapping again

### Error Wrapping Patterns

**Basic Wrapping:**
```go
// ✅ Simple case - wrap once
if err := operation(); err != nil {
    return errors.NewVCPError(errors.ErrInternalServerError, err)
}
```

**Adding Context Before Wrapping:**
```go
// ✅ Add context with fmt.Errorf, then wrap
if err := db.Query(); err != nil {
    contextErr := fmt.Errorf("failed to query user table: %w", err)
    return errors.NewVCPError(errors.ErrDatabaseDataReadError, contextErr)
}
```

**Conditional Wrapping:**
```go
// ✅ Check if already wrapped before wrapping
func processError(err error) error {
    var customErr *errors.CustomError
    if errors.As(err, &customErr) {
        // Already wrapped, return as-is
        return err
    }
    
    // Not wrapped, wrap it now
    return errors.NewVCPError(errors.ErrInternalServerError, err)
}
```

**Error Composition for Complex Scenarios:**
```go
// ✅ Compose errors with multiple context layers
func multiStepOperation() error {
    if err := step1(); err != nil {
        // Add step context
        stepErr := fmt.Errorf("step 1 initialization failed: %w", err)
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, stepErr)
    }
    
    if err := step2(); err != nil {
        // Add step context with additional details
        stepErr := fmt.Errorf("step 2 processing failed: %w", err)
        return errors.NewVCPError(errors.ErrInternalServerError, stepErr)
    }
    
    return nil
}
```

**Avoiding Double Wrapping:**
```go
// ❌ WRONG: Double wrapping loses original error
func badExample() error {
    if err := operation(); err != nil {
        customErr := errors.NewVCPError(errors.ErrDatabaseConnectionClosed, err)
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, customErr) // Double wrapped!
    }
    return nil
}

// ✅ CORRECT: Single wrapping preserves original
func goodExample() error {
    if err := operation(); err != nil {
        return errors.NewVCPError(errors.ErrDatabaseConnectionClosed, err)
    }
    return nil
}
```

**Handling Double Wrapping Scenarios:**

Sometimes you need to change the error context (e.g., from a database error to a workflow error). In these cases, extract the original error instead of double wrapping:

```go
// ✅ CORRECT: Extract original error when changing context
func changeErrorContext(err error) error {
    var customErr *errors.CustomError
    if errors.As(err, &customErr) {
        // Extract the original error and wrap with new context
        originalErr := customErr.OriginalErr
        if originalErr == nil {
            // If no original error, create one from the message
            originalErr = fmt.Errorf(customErr.Message)
        }
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, originalErr)
    }
    
    // Not wrapped, wrap normally
    return errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
}

// ✅ CORRECT: Preserve error chain when changing context
func preserveErrorChain(err error) error {
    var customErr *errors.CustomError
    if errors.As(err, &customErr) {
        // Combine contexts without double wrapping
        combinedErr := fmt.Errorf("workflow failed: %w", customErr)
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, combinedErr)
    }
    
    // Not wrapped, wrap normally
    return errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
}

// ✅ CORRECT: Extract and rewrap with additional context
func extractAndRewrap(err error) error {
    var customErr *errors.CustomError
    if errors.As(err, &customErr) {
        // Get the most original error in the chain
        originalErr := getOriginalError(customErr)
        
        // Add new context and wrap
        contextErr := fmt.Errorf("workflow processing failed: %w", originalErr)
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, contextErr)
    }
    
    return errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
}

// Helper function to get the most original error
func getOriginalError(err error) error {
    var customErr *errors.CustomError
    if errors.As(err, &customErr) && customErr.OriginalErr != nil {
        return getOriginalError(customErr.OriginalErr)
    }
    return err
}
```

**When You Must Change Error Context - Extract Original Error:**
```go
// ✅ CORRECT: Extract original error when changing context is necessary
func changeErrorContext() error {
    if err := operation(); err != nil {
        var customErr *errors.CustomError
        if errors.As(err, &customErr) {
            // Extract the original error and wrap with new context
            originalErr := customErr.OriginalErr
            if originalErr == nil {
                // If no original error, use the custom error message
                originalErr = fmt.Errorf(customErr.Message)
            }
            return errors.NewVCPError(errors.ErrWorkflowConfigurationError, originalErr)
        }
        
        // Not wrapped, wrap normally
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
    }
    return nil
}

// ✅ CORRECT: Preserve error chain when changing context
func preserveErrorChain() error {
    if err := operation(); err != nil {
        var customErr *errors.CustomError
        if errors.As(err, &customErr) {
            // Create a new error that combines both contexts
            combinedErr := fmt.Errorf("workflow failed: %w", customErr)
            return errors.NewVCPError(errors.ErrWorkflowConfigurationError, combinedErr)
        }
        
        // Not wrapped, wrap normally
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
    }
    return nil
}
```

## Common Patterns

### 1. Function Return Pattern

```go
func processData(data []byte) error {
    if len(data) == 0 {
        return errors.NewVCPError(errors.ErrResourceEmptyError, nil)
    }
    
    if err := validateData(data); err != nil {
        return errors.NewVCPError(errors.ErrInputValidationError, err)
    }
    
    if err := storeData(data); err != nil {
        return errors.NewVCPError(errors.ErrDatabaseDataInsertError, err)
    }
    
    return nil
}
```

### 2. Error Chain Handling

```go
func handleOperation() error {
    if err := step1(); err != nil {
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
    }
    
    if err := step2(); err != nil {
        return errors.NewVCPError(errors.ErrInternalServerError, err)
    }
    
    return nil
}
```

### 3. Error Context Transformation

```go
// When you need to change error context (e.g., from database to workflow)
func transformErrorContext(err error) error {
    var customErr *errors.CustomError
    if errors.As(err, &customErr) {
        // Extract original error and wrap with new context
        originalErr := customErr.OriginalErr
        if originalErr == nil {
            originalErr = fmt.Errorf(customErr.Message)
        }
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, originalErr)
    }
    
    // Not wrapped, wrap normally
    return errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
}

// Example usage in workflow
func workflowStep(ctx interface{}) error {
    if err := databaseOperation(); err != nil {
        // Transform database error to workflow error
        return transformErrorContext(err)
    }
    return nil
}
```

### 4. Error with Placeholders

```go
// Create errors with dynamic context using placeholders
func createResourceError(resourceType, resourceName string, err error) error {
    return errors.NewVCPErrorWithArgs(
        errors.ErrResourceNotFound,
        err,
        resourceType,  // First placeholder: %s
        resourceName   // Second placeholder: %s
    )
}

// Reuse error templates with different arguments
func handlePoolError(poolName string, operation string, err error) error {
    baseErr := errors.NewVCPError(errors.ErrWorkflowConfigurationError, err)
    
    if operation == "create" {
        return baseErr.WithArgs("pool creation", poolName)
    } else if operation == "delete" {
        return baseErr.WithArgs("pool deletion", poolName)
    }
    
    return baseErr.WithArgs("pool operation", poolName)
}

// Example usage
func processPool(poolName string) error {
    if err := validatePool(poolName); err != nil {
        return createResourceError("pool", poolName, err)
    }
    
    if err := createPool(poolName); err != nil {
        return handlePoolError(poolName, "create", err)
    }
    
    return nil
}
```

### 5. Retry Logic

```go
func retryOperation(operation func() error, maxRetries int) error {
    for attempt := 0; attempt < maxRetries; attempt++ {
        err := operation()
        if err == nil {
            return nil
        }
        
        var customErr *errors.CustomError
        if errors.As(err, &customErr) && !customErr.IsRetriable() {
            return err // Don't retry non-retriable errors
        }
        
        if attempt < maxRetries-1 {
            time.Sleep(time.Duration(attempt+1) * time.Second)
        }
    }
    
    return errors.NewVCPError(errors.ErrMaxRetriesExceeded, nil)
}
```

## Performance Considerations

1. **Error Creation**: `NewVCPError` performs map lookups, so avoid creating errors in tight loops
2. **Error Checking**: Use `errors.Is` and `errors.As` efficiently - they traverse the error chain
3. **Memory**: CustomError structs are lightweight, but consider pooling for high-frequency error scenarios

## Security Considerations

1. **Error Messages**: Never expose sensitive information in user-facing error messages
2. **Error Logging**: Log original errors for debugging but sanitize user responses
3. **HTTP Codes**: Use appropriate HTTP status codes to avoid information leakage

## Troubleshooting

### Common Issues

1. **Missing Error Codes**: If an error code isn't defined, the system falls back to `ErrInternalServerError`
2. **Nil Error Handling**: Always check for nil before calling error methods
3. **Temporal Integration**: Ensure errors are properly wrapped when used in workflows
4. **Double Wrapping**: Wrapping an already wrapped error loses the original error context
5. **Lost Original Errors**: Not preserving the original error makes debugging difficult

### Debugging Tips

1. **Use `ExtractCustomError`** to inspect error details
2. **Check `TrackingID`** to identify specific error types
3. **Verify `OriginalErr`** for underlying cause
4. **Use `IsRetriable()`** to determine retry behavior
5. **Check for Double Wrapping**: Use `errors.Unwrap()` to see if errors are wrapped multiple times
6. **Trace Error Chain**: Use `errors.As` to traverse the error chain and identify wrapping issues
7. **Log Original Errors**: Always log `OriginalErr.Error()` for debugging context

## Migration Guide

### From Standard Errors

```go
// Before
if err != nil {
    return fmt.Errorf("failed to process request: %w", err)
}

// After
if err != nil {
    return errors.NewVCPError(errors.ErrInternalServerError, err)
}
```

### From Custom Error Types

```go
// Before
type ValidationError struct {
    Field string
    Value interface{}
}

func (e ValidationError) Error() string {
    return fmt.Sprintf("validation failed for field %s", e.Field)
}

// After
// Use existing error codes or create new ones
return errors.NewVCPError(errors.ErrInputValidationError, 
    fmt.Errorf("validation failed for field %s", field))
```

### From Error Wrapping Patterns

```go
// Before: Multiple layers of error wrapping
func oldPattern() error {
    if err := operation(); err != nil {
        wrappedErr := fmt.Errorf("operation failed: %w", err)
        return fmt.Errorf("workflow failed: %w", wrappedErr) // Double wrapped!
    }
    return nil
}

// After: Single wrapping with context preservation
func newPattern() error {
    if err := operation(); err != nil {
        // Add context first, then wrap once
        contextErr := fmt.Errorf("workflow operation failed: %w", err)
        return errors.NewVCPError(errors.ErrWorkflowConfigurationError, contextErr)
    }
    return nil
}

// Before: Losing original error context
func oldErrorHandling() error {
    if err := db.Query(); err != nil {
        return fmt.Errorf("database error") // Original error lost!
    }
    return nil
}

// After: Preserving original error
func newErrorHandling() error {
    if err := db.Query(); err != nil {
        return errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
    }
    return nil
}
```

This custom error system provides a robust foundation for error handling across the VSA Control Plane, ensuring consistent error management, proper tracking, and seamless integration with Temporal workflows. 
