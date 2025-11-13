# ONTAP Proxy Rule Engine

## Overview

The ONTAP Proxy Rule Engine is a middleware-based system that provides fine-grained control over ONTAP REST API requests and responses. It enables request validation, field injection, response modification, and access control for ONTAP API endpoints. The rule engine acts as a policy enforcement layer between clients and the ONTAP REST API, allowing the control plane to enforce business rules and security policies.

## Architecture

The rule engine is implemented as HTTP middleware that intercepts requests before they reach the ONTAP backend and processes responses before they are returned to clients. The architecture consists of the following components:

### Components

1. **Rule Engine Middleware** (`middleware/rule_engine.go`)
   - Intercepts HTTP requests
   - Matches request paths to configured rules
   - Executes action processors
   - Stores action context for response processing

2. **Rule Map** (`rules/rule_map.go`)
   - Central registry of all configured rules
   - Maps API paths to rule definitions
   - Supports path normalization with UUID placeholders

3. **Action Processors** (`actions/processor/`)
   - Implement the `RequestProcessor` interface
   - Handle request validation and modification
   - Process response transformations

4. **Response Validator** (`actions/responsevalidator.go`)
   - Processes responses after receiving from ONTAP
   - Applies response rules (injection/removal)
   - Integrated with reverse proxy's `ModifyResponse` hook

### Request Flow

```
Client Request
    ↓
Rule Engine Middleware
    ↓
1. Extract ONTAP path from request URL
2. Normalize UUIDs in path (e.g., /api/storage/volumes/{uuid})
3. Match path to rule in rule map
4. Get action processor for HTTP method (GET/POST/PATCH/DELETE)
5. Validate request (ShouldAllow)
6. Process request modifications (ProcessRequest)
7. Store action in context
    ↓
Reverse Proxy (forwards to ONTAP)
    ↓
ONTAP REST API
    ↓
Response Validator (ProcessResponseModification)
    ↓
1. Retrieve action from context
2. Apply response rules (injection/removal)
3. Return modified response
    ↓
Client Response
```

## Rule Types

The rule engine supports three types of action processors:

### 1. Allow Action

The `Allow` action permits requests with optional validation and field manipulation. It supports:

- **Request Validation**: Validates required fields and allowed values
- **Request Injection**: Injects fields into request bodies
- **Response Injection**: Adds fields to responses
- **Response Removal**: Removes fields from responses

**Use Case**: When you want to allow an operation but enforce specific constraints or modify data.

**Example**:
```go
GET: &processor.Allow{
    Name: "Allow aggregate listing",
}
```

### 2. Deny Action

The `Deny` action blocks all requests for a specific endpoint and HTTP method. It always returns `false` from `ShouldAllow`, preventing the request from proceeding.

**Use Case**: When you want to explicitly block certain operations.

**Example**:
```go
POST: &processor.Deny{
    Name: "Aggregate creation not allowed",
}
```

### 3. VolumeAction

A specialized action processor for volume operations. It provides the same capabilities as `Allow` but is tailored for volume-specific workflows. It includes special handling for GET and DELETE operations and is designed to integrate with reconciliation APIs (planned feature).

**Use Case**: Volume operations that require specific business logic or reconciliation.

**Example**:
```go
POST: &processor.VolumeAction{
    Name: "Allow volume creation",
    RequestRule: actions.RequestRule{
        ValidationRules: [...],
        InjectionRules: [...],
    },
    ResponseRule: actions.ResponseRule{
        RemovalRules: [...],
    },
}
```

## Rule Categories

Rules are organized into two categories: **Request Rules** and **Response Rules**.

### Request Rules

Request rules are applied to incoming requests before they are forwarded to ONTAP.

#### Validation Rules

Validation rules enforce constraints on request payloads:

- **Required Fields**: Ensures specified fields are present in the request
- **Allowed Values**: Restricts field values to a predefined list
- **Field Paths**: Supports nested field paths using dot notation (e.g., `guarantee.type`, `space.logical_space.enforcement`)

**ValidationRule Structure**:
```go
type ValidationRule struct {
    FieldPath string        // JSON path (e.g., "size", "guarantee.type")
    Required  bool          // Whether field must be present
    MinValue  interface{}   // Minimum value (future use)
    MaxValue  interface{}   // Maximum value (future use)
    Values    []interface{} // Allowed values for the field
}
```

**Example**:
```go
ValidationRules: []actions.ValidationRule{
    {
        FieldPath: "size",
        Required:  true,
    },
    {
        FieldPath: "guarantee.type",
        Values:    []interface{}{"none"},
    },
    {
        FieldPath: "space.logical_space.enforcement",
        Values:    []interface{}{true},
    },
}
```

#### Injection Rules

Injection rules automatically add or override fields in request bodies:

- **Field Injection**: Injects values into specified field paths
- **Automatic Creation**: Creates nested objects if they don't exist
- **Override Behavior**: Overwrites existing values

**InjectionRule Structure**:
```go
type InjectionRule struct {
    FieldPath string      // JSON path to inject value
    Value     interface{} // Value to inject
}
```

**Example**:
```go
InjectionRules: []actions.InjectionRule{
    {
        FieldPath: "space.logical_space.enforcement",
        Value:     true,
    },
}
```

### Response Rules

Response rules are applied to responses received from ONTAP before they are returned to clients.

#### Response Injection Rules

Similar to request injection, but applied to response bodies. Useful for adding computed fields or metadata.

**Example**:
```go
ResponseRule: actions.ResponseRule{
    InjectionRules: []actions.InjectionRule{
        {
            FieldPath: "computed_field",
            Value:     "computed_value",
        },
    },
}
```

#### Response Removal Rules

Removal rules strip sensitive or unnecessary fields from responses:

- **Field Removal**: Removes specified fields from response bodies
- **Nested Path Support**: Supports nested field paths
- **Array Handling**: Automatically processes arrays of records

**RemovalRule Structure**:
```go
type RemovalRule struct {
    FieldPath string // JSON path to remove
}
```

**Example**:
```go
ResponseRule: actions.ResponseRule{
    RemovalRules: []actions.RemovalRule{
        {FieldPath: "efficiency"},
        {FieldPath: "space.physical_used"},
        {FieldPath: "space.logical_space.enforcement"},
    },
}
```

## Path Matching

The rule engine uses intelligent path matching with UUID normalization:

### Path Normalization

1. **Extraction**: Extracts the ONTAP API path from the full request URL
   - Looks for `/ontap-api` segment in the path
   - Extracts everything after this segment

2. **UUID Normalization**: Replaces UUID patterns with `{uuid}` placeholders
   - Pattern: `/[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}`
   - Example: `/api/storage/volumes/123e4567-e89b-12d3-a456-426614174000` → `/api/storage/volumes/{uuid}`

3. **Exact Matching**: Matches normalized paths to rule definitions

**Example Paths**:
- Request: `/proxy/ontap-api/api/storage/volumes/123e4567-e89b-12d3-a456-426614174000`
- Extracted: `/api/storage/volumes/123e4567-e89b-12d3-a456-426614174000`
- Normalized: `/api/storage/volumes/{uuid}`
- Matches Rule: `/api/storage/volumes/{uuid}`

## HTTP Method Support

Rules are defined per HTTP method:

- **GET**: Read operations
- **POST**: Create operations
- **PATCH**: Update operations
- **DELETE**: Delete operations

Each method can have a different action processor, allowing fine-grained control:

```go
"/api/storage/aggregates": {
    GET:    &processor.Allow{...},  // Allow listing
    POST:   &processor.Deny{...},   // Block creation
    PATCH:  &processor.Deny{...},   // Block modification
    DELETE: &processor.Deny{...},   // Block deletion
}
```

## Field Path Syntax

Field paths use dot notation to navigate nested JSON structures:

- **Simple Path**: `"size"` → `request.size`
- **Nested Path**: `"guarantee.type"` → `request.guarantee.type`
- **Deep Nested**: `"space.logical_space.enforcement"` → `request.space.logical_space.enforcement`

The path resolution:
1. Splits the path by dots
2. Navigates through nested maps
3. Creates intermediate objects if needed (for injection)
4. Validates existence (for validation)

## Current Rule Implementations

### Volume Operations

#### List Volumes (`GET /api/storage/volumes`)
- **Action**: `VolumeAction`
- **Request Rules**: None
- **Response Rules**: Removes `efficiency` and `space.physical_used` fields

#### Create Volume (`POST /api/storage/volumes`)
- **Action**: `VolumeAction`
- **Request Rules**:
  - Validates: `size` (required), `name` (required)
  - Validates: `guarantee.type` must be `"none"`
  - Validates: `space.logical_space.enforcement` must be `true`
  - Validates: `space.logical_space.reporting` must be `true`
  - Injects: `space.logical_space.enforcement = true`
- **Response Rules**: Removes `efficiency` and `space.physical_used` fields

#### Get Volume (`GET /api/storage/volumes/{uuid}`)
- **Action**: `VolumeAction`
- **Request Rules**: None
- **Response Rules**: Removes `efficiency`, `space.physical_used`, `space.logical_space.enforcement`, and `space.logical_space.reporting` fields

#### Update Volume (`PATCH /api/storage/volumes/{uuid}`)
- **Action**: `VolumeAction`
- **Request Rules**:
  - Validates: `size` (required), `name` (required)
  - Validates: `guarantee.type` must be `"none"` or `"volume"`
  - Validates: `space.logical_space.enforcement` must be `true`
- **Response Rules**: Removes `efficiency` and `space.physical_used` fields

#### Delete Volume (`DELETE /api/storage/volumes/{uuid}`)
- **Action**: `VolumeAction`
- **Request Rules**: None
- **Response Rules**: Removes `efficiency` and `space.physical_used` fields

### Aggregate Operations

#### List Aggregates (`GET /api/storage/aggregates`)
- **Action**: `Allow`
- **Request Rules**: None
- **Response Rules**: None

#### Create/Update/Delete Aggregates
- **Actions**: `Deny` (all blocked)
- **Rationale**: Aggregate management is restricted in the control plane

## Error Handling

The rule engine provides comprehensive error handling:

1. **No Rule Found**: Request passes through to next middleware (no rule enforcement)
2. **Method Not Allowed**: Returns `405 Method Not Allowed`
3. **Validation Error**: Returns `400 Bad Request` with error message
4. **Request Denied**: Returns `403 Forbidden`
5. **Processing Error**: Returns `500 Internal Server Error`

## Integration Points

### Middleware Integration

The rule engine is integrated as HTTP middleware in the request chain:

```go
// In main.go or router setup
router.Use(middleware.RuleEngineMiddleware())
```

### Response Processing

Response processing is integrated with the reverse proxy:

```go
proxy := &httputil.ReverseProxy{
    ModifyResponse: actions.ProcessResponseModification,
    // ...
}
```

The `ProcessResponseModification` function:
1. Retrieves the action from request context
2. Calls `ProcessResponse` on the action
3. Applies response rules (injection/removal)
4. Returns modified response

## Extensibility

The rule engine is designed for extensibility:

### Adding New Rules

1. Define rule in `rules/rule_map.go`:
```go
"/api/storage/new-resource": {
    GET: &processor.Allow{
        Name: "Allow new resource listing",
        // ... rules
    },
}
```

### Creating Custom Action Processors

Implement the `RequestProcessor` interface:

```go
type CustomAction struct {
    Name         string
    RequestRule  actions.RequestRule
    ResponseRule actions.ResponseRule
}

func (c *CustomAction) ShouldAllow(r *http.Request) (bool, error) {
    // Custom validation logic
}

func (c *CustomAction) ProcessRequest(r *http.Request, w http.ResponseWriter) error {
    // Custom request processing
}

func (c *CustomAction) ProcessResponse(resp *http.Response) error {
    // Custom response processing
}
```

## Future Enhancements

Based on code comments and structure, planned enhancements include:

1. **Reconciliation API Integration**: Volume operations will call reconciliation APIs for state management
2. **MinValue/MaxValue Validation**: Support for numeric range validation
3. **Conditional Rules**: Rules based on request context or headers
4. **Rule Chaining**: Multiple rules per endpoint
5. **Dynamic Rule Loading**: Runtime rule updates without code changes

## Best Practices

1. **Use Specific Rules**: Define rules for specific endpoints rather than broad wildcards
2. **Validate Early**: Use validation rules to catch errors before forwarding to ONTAP
3. **Remove Sensitive Data**: Use removal rules to strip sensitive information from responses
4. **Document Rules**: Add descriptive names to actions for debugging and maintenance
5. **Test Edge Cases**: Test with various field combinations and nested structures

## Examples

### Complete Rule Definition

```go
"/api/storage/volumes": {
    POST: &processor.VolumeAction{
        Name: "Allow volume creation",
        RequestRule: actions.RequestRule{
            ValidationRules: []actions.ValidationRule{
                {
                    FieldPath: "size",
                    Required:  true,
                },
                {
                    FieldPath: "name",
                    Required:  true,
                },
                {
                    FieldPath: "guarantee.type",
                    Values:    []interface{}{"none"},
                },
            },
            InjectionRules: []actions.InjectionRule{
                {
                    FieldPath: "space.logical_space.enforcement",
                    Value:     true,
                },
            },
        },
        ResponseRule: actions.ResponseRule{
            RemovalRules: []actions.RemovalRule{
                {FieldPath: "efficiency"},
                {FieldPath: "space.physical_used"},
            },
        },
    },
}
```

This rule:
- Validates that `size` and `name` are provided
- Ensures `guarantee.type` is `"none"`
- Automatically sets `space.logical_space.enforcement` to `true`
- Removes `efficiency` and `space.physical_used` from the response

## Summary

The ONTAP Proxy Rule Engine provides a powerful, flexible system for enforcing policies on ONTAP API interactions. It supports:

- ✅ Request validation (required fields, allowed values)
- ✅ Request field injection
- ✅ Response field injection
- ✅ Response field removal
- ✅ Access control (allow/deny)
- ✅ Path-based rule matching with UUID normalization
- ✅ Per-method rule definitions
- ✅ Nested field path support
- ✅ Extensible action processor architecture

The rule engine enables the control plane to enforce business rules, security policies, and data transformations transparently between clients and the ONTAP REST API.

