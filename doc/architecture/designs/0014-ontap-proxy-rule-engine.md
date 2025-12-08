# ONTAP Proxy Rule Engine

## Overview

The ONTAP Proxy Rule Engine is a middleware-based system that provides fine-grained control over ONTAP REST API requests and responses. It enables request validation, field injection, response modification, and access control for ONTAP API endpoints. The rule engine acts as a policy enforcement layer between clients and the ONTAP REST API, allowing the control plane to enforce business rules and security policies.

## Architecture

The rule engine is implemented as HTTP middleware that intercepts requests before they reach the ONTAP backend and processes responses before they are returned to clients.

### Components

1. **Rule Engine Middleware** (`middleware/rule_engine.go`)
   - Intercepts HTTP requests
   - Matches request paths to configured rules
   - Executes action processors
   - Stores action context for response processing

2. **DSL Package** (`dsl/`)
   - `action.go` - Defines the `IAction` interface and `Rule` struct
   - `actions.go` - Implements action types: `Allow`, `AllowAll`, `Deny`, `DenyAll`, `When`
   - `validation.go` - Condition helpers: `HasFields`, `And`, `Or`, `Not`, `IfPresentThenValue`, etc.
   - `modification.go` - Modification types: `SetFields`, `RemoveFields`, `SetRequestFields`, etc.

3. **Rule Map** (`rules_v2/rule_map.go`)
   - Central registry of all configured rules
   - Maps API paths to rule definitions using DSL
   - Supports path normalization with UUID placeholders

4. **External Validators** (`rules_v2/validators.go`)
   - Custom validation functions for external API calls
   - Integration with core services

5. **Response Processor** (`middleware/response_processor.go`)
   - Processes responses after receiving from ONTAP
   - Applies response modifications (field injection/removal)
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
4. Get action for HTTP method (GET/POST/PATCH/DELETE/PUT/HEAD)
5. Evaluate conditions (ShouldAllow)
6. Process request modifications (ProcessRequest)
7. Store action in context
    ↓
Reverse Proxy (forwards to ONTAP)
    ↓
ONTAP REST API
    ↓
Response Processor (ProcessResponseModification)
    ↓
1. Retrieve action from context
2. Apply response modifications (injection/removal)
3. Return modified response
    ↓
Client Response
```

## DSL Components

### Actions

Actions implement the `IAction` interface and determine how requests are handled:

```go
type IAction interface {
    // ShouldAllow returns (true, "") if allowed, or (false, "reason") if denied
    ShouldAllow(r *http.Request) (allowed bool, reason string)
    
    // ProcessRequest applies modifications before forwarding
    ProcessRequest(r *http.Request, w http.ResponseWriter) (actionName string, err error)
    
    // ProcessResponse applies modifications before returning to client
    ProcessResponse(resp *http.Response) (actionName string, err error)
}
```

#### Allow

Permits requests and optionally applies modifications:

```go
Allow{
    Name: "Allow volume listing",
    ModifyRequest: SetRequestFields{
        Fields: map[string]interface{}{
            "space.logical_space.enforcement": true,
        },
    },
    ModifyResponse: RemoveFields{
        Fields: []string{"$.efficiency", "$.space.physical_used"},
    },
}
```

#### AllowAll

Simple action that permits requests without any modifications:

```go
AllowAll{}  // Pass through unchanged
```

#### Deny

Blocks requests with a specific reason:

```go
Deny{Name: "Aggregate creation not allowed"}
```

#### DenyAll

Blocks all requests with a generic "Access denied" message:

```go
DenyAll{}
```

#### When

Conditional action that branches based on a condition:

```go
When{
    Name: "Volume creation validation",
    Condition: And(
        HasFields("size", "name"),
        IfPresentThenValue("guarantee.type", "none"),
    ),
    IsTrue: Allow{
        Name: "Allow volume creation",
        ModifyResponse: RemoveFields{Fields: []string{"$.efficiency"}},
    },
    // IsFalse is optional - uses condition's reason if not set
}
```

### Conditions

Conditions are functions that return `(bool, string)` - the boolean indicates pass/fail, and the string provides a reason for denial.

#### Field Validation

```go
// Check if fields exist in request body
HasFields("size", "name")

// Check if field has specific value
HasFieldValue("guarantee.type", "none")

// Check if field value is in allowed list
HasFieldValueIn("guarantee.type", "none", "volume")

// If field present, validate its value
IfPresentThenValue("guarantee.type", "none", "volume")

// If field present, check exact equality
IfPresentThenEquals("space.logical_space.enforcement", true)
```

#### Header Validation

```go
HasHeader("X-Custom-Header", "expected-value")
```

#### Method Validation

```go
IsMethod(http.MethodPost)
```

#### Logical Operators

```go
// All conditions must pass
And(
    HasFields("size"),
    HasFieldValue("type", "dp"),
)

// At least one condition must pass
Or(
    HasFieldValue("type", "dp"),
    HasFieldValue("type", "rw"),
)

// Negate a condition
Not(HasFields("restricted_field"))
```

#### Custom Validators

External validation functions can be used as conditions:

```go
// In validators.go
func ValidateVolumeCreationWithCore(r *http.Request) (bool, string) {
    // Make API call to core service
    // Return (true, "") if approved
    // Return (false, "Quota exceeded") if denied
}

// In rule_map.go
Condition: And(
    HasFields("size", "name"),
    ValidateVolumeCreationWithCore,
)
```

### Modifications

Modifications transform request or response bodies.

#### Request Modifications

```go
// Set fields in request body
SetRequestFields{
    Fields: map[string]interface{}{
        "space.logical_space.enforcement": true,
        "comment": "Created by proxy",
    },
}

// Set HTTP headers
SetHeaders{
    Headers: map[string]string{
        "X-Proxy-Version": "2.0",
    },
}

// Set query parameters
SetQueryParams{
    Params: map[string]string{
        "return_timeout": "30",
    },
}
```

#### Response Modifications

```go
// Remove fields from response
RemoveFields{
    Fields: []string{
        "$.efficiency",
        "$.space.physical_used",
        "$.space.logical_space.enforcement",
    },
}

// Set fields in response (supports JSONPath copying)
SetFields{
    Fields: map[string]string{
        "$.status": "\"processed\"",      // Literal value
        "$.metadata.proxy": "\"v2\"",     // Nested field
        "$.copyOfName": "$.name",         // Copy from another field
    },
}
```

#### Combining Modifications

```go
AllOf(
    SetFields{Fields: map[string]string{"$.processed": "true"}},
    RemoveFields{Fields: []string{"$.sensitive"}},
)
```

## Rule Definition Examples

### Complete Rule Map

```go
func GetProxyRules() map[string]Rule {
    return map[string]Rule{
        // Deny all private API access
        "/api/private/*": {
            GET:    Deny{Name: "Private API access denied"},
            POST:   Deny{Name: "Private API access denied"},
            PATCH:  Deny{Name: "Private API access denied"},
            DELETE: Deny{Name: "Private API access denied"},
        },

        // Volume operations
        "/api/storage/volumes": {
            GET: Allow{
                Name: "Allow volume listing",
                ModifyResponse: RemoveFields{
                    Fields: []string{"$.efficiency", "$.space.physical_used"},
                },
            },
            POST: When{
                Name: "Volume creation validation",
                Condition: And(
                    HasFields("size", "name"),
                    IfPresentThenValue("guarantee.type", "none"),
                    IfPresentThenEquals("space.logical_space.enforcement", true),
                    ValidateVolumeCreationWithCore,
                ),
                IsTrue: Allow{
                    Name: "Allow volume creation",
                    ModifyRequest: SetRequestFields{
                        Fields: map[string]interface{}{
                            "space.logical_space.enforcement": true,
                        },
                    },
                    ModifyResponse: RemoveFields{
                        Fields: []string{"$.efficiency"},
                    },
                },
            },
            PUT:    DenyAll{},
            PATCH:  DenyAll{},
            DELETE: DenyAll{},
        },

        // Specific volume operations
        "/api/storage/volumes/{uuid}": {
            GET: Allow{
                Name: "Allow volume details",
                ModifyResponse: RemoveFields{
                    Fields: []string{
                        "$.efficiency",
                        "$.space.logical_space.enforcement",
                    },
                },
            },
            PATCH: When{
                Name: "Volume modification validation",
                Condition: And(
                    IfPresentThenValue("guarantee.type", "none", "volume"),
                    IfPresentThenEquals("space.logical_space.enforcement", true),
                ),
                IsTrue: Allow{Name: "Allow volume modification"},
            },
            DELETE: Allow{Name: "Allow volume deletion"},
        },

        // Aggregate operations (mostly denied)
        "/api/storage/aggregates": {
            GET:    Allow{Name: "Allow aggregate listing"},
            POST:   Deny{Name: "Aggregate creation not allowed"},
            PATCH:  Deny{Name: "Aggregate modification not allowed"},
            DELETE: Deny{Name: "Aggregate deletion not allowed"},
        },
    }
}
```

## Path Matching

### Path Normalization

1. **Extraction**: Extracts the ONTAP API path from the full request URL
   - Looks for `/ontap` segment in the path
   - Extracts everything after this segment

2. **UUID Normalization**: Replaces UUID patterns with `{uuid}` placeholders
   - Pattern: `/[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}`
   - Example: `/api/storage/volumes/123e4567-e89b-12d3-a456-426614174000` → `/api/storage/volumes/{uuid}`

3. **Matching**:
   - Exact match first
   - Wildcard match (`/*` suffix) for path prefixes

### Example

- Request: `/v1beta/projects/123/locations/us-east4/pools/abc-def/ontap/api/storage/volumes/123e4567-e89b-12d3-a456-426614174000`
- Extracted: `/api/storage/volumes/123e4567-e89b-12d3-a456-426614174000`
- Normalized: `/api/storage/volumes/{uuid}`
- Matches Rule: `/api/storage/volumes/{uuid}`

## HTTP Method Support

Rules are defined per HTTP method:

- **GET**: Read operations
- **POST**: Create operations
- **PUT**: Full update operations
- **PATCH**: Partial update operations
- **DELETE**: Delete operations
- **HEAD**: Metadata operations

Each method can have a different action:

```go
"/api/storage/aggregates": {
    GET:    Allow{Name: "Allow listing"},
    POST:   Deny{Name: "Creation blocked"},
    PATCH:  Deny{Name: "Modification blocked"},
    DELETE: Deny{Name: "Deletion blocked"},
    HEAD:   AllowAll{},
}
```

## Array Handling

Response modifications automatically handle arrays. When a response contains a `records` array, modifications are applied to each record:

```json
{
    "num_records": 2,
    "records": [
        {"name": "vol1", "efficiency": {...}},
        {"name": "vol2", "efficiency": {...}}
    ]
}
```

With `RemoveFields{Fields: []string{"$.efficiency"}}`, the `efficiency` field is removed from each record.

## Error Handling

| Scenario | HTTP Status | Response |
|----------|-------------|----------|
| No rule found | Pass through | Request forwarded unchanged |
| Method not allowed | 405 | "Method not allowed" |
| Condition failed | 400 | Condition's reason message |
| Request denied | 400 | Action's denial reason |
| Processing error | 500 | "Internal server error" |

## File Structure

```
ontap-proxy/
├── dsl/
│   ├── action.go           # IAction interface, Rule struct
│   ├── action_test.go
│   ├── actions.go          # Allow, AllowAll, Deny, DenyAll, When
│   ├── actions_test.go
│   ├── modification.go     # SetFields, RemoveFields, SetRequestFields, etc.
│   ├── modification_test.go
│   ├── validation.go       # HasFields, And, Or, IfPresentThenValue, etc.
│   └── validation_test.go
├── rules_v2/
│   ├── rule_map.go         # Rule definitions
│   ├── rule_map_test.go
│   └── validators.go       # External validation functions
├── middleware/
│   ├── rule_engine.go      # Main middleware
│   ├── rule_engine_test.go
│   ├── response_processor.go
│   └── response_processor_test.go
└── ...
```

## Creating Custom Validators

External validators enable integration with other services:

```go
// validators.go
func ValidateVolumeCreationWithCore(r *http.Request) (bool, string) {
    // 1. Parse request body
    body, _ := io.ReadAll(r.Body)
    r.Body = io.NopCloser(bytes.NewReader(body)) // restore body
    
    var volumeRequest VolumeCreateRequest
    json.Unmarshal(body, &volumeRequest)
    
    // 2. Call core API
    resp, err := coreClient.ValidateVolume(ctx, volumeRequest)
    if err != nil {
        return false, fmt.Sprintf("Core API error: %v", err)
    }
    
    // 3. Return result
    if !resp.Allowed {
        return false, resp.Reason // e.g., "Quota exceeded: only 10GB remaining"
    }
    return true, ""
}

// WrapValidator helper for simple bool validators
func WrapValidator(fn func(r *http.Request) bool, reason string) Condition {
    return func(r *http.Request) (bool, string) {
        if fn(r) {
            return true, ""
        }
        return false, reason
    }
}
```

## Best Practices

1. **Use Descriptive Names**: Add meaningful names to actions for logging and debugging
2. **Validate Early**: Use conditions to catch errors before forwarding to ONTAP
3. **Remove Sensitive Data**: Use `RemoveFields` to strip sensitive information
4. **Provide Clear Reasons**: Use specific denial messages for better error handling
5. **Test Edge Cases**: Test with various field combinations and nested structures
6. **Use Helper Functions**: Leverage `IfPresentThenValue` and `IfPresentThenEquals` for cleaner rules

## Summary

The ONTAP Proxy Rule Engine provides a declarative DSL for enforcing policies on ONTAP API interactions:

- ✅ Request validation with composable conditions
- ✅ Request field injection and modification
- ✅ Response field injection and removal
- ✅ Access control (Allow/Deny with reasons)
- ✅ Conditional actions with `When`
- ✅ Path-based rule matching with UUID normalization
- ✅ Per-method rule definitions
- ✅ Nested field path support (JSONPath)
- ✅ Array handling for list responses
- ✅ External validator integration
- ✅ Detailed error messages

The DSL-based approach provides cleaner, more maintainable rule definitions compared to the previous struct-based approach.
