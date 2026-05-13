# Support for New CA Pool Configuration

## Overview

This document describes the implementation of Certificate Authority (CA) pool support by storing CA pool configuration (CA Pool, CA Name, and CA Project ID) in the `pool_credentials` structure of the `pools` table in the database.

## Background

Previously, CA configuration was only available through environment variables, which meant all pools in a deployment shared the same CA configuration. With this enhancement, each pool can now have its own CA configuration stored in the database, while maintaining backward compatibility with existing pools through environment variable fallback.

## Implementation Details

### Database Schema

The `pool_credentials` column in the `pools` table is a JSONB field that stores pool authentication and certificate-related configuration. The structure includes:

- `secret_id`: Secret identifier for password-based authentication
- `certificate_id`: Certificate identifier for certificate-based authentication
- `password`: Password for basic authentication
- `auth_type`: Authentication type (0=username/password, 1=username/password in secret manager, 2=certificate)
- `ca_uri`: **New field** - Consolidated CA configuration in the format: `ca_pool_deployed_project_id/ca_pool_name/ca_name`

### CA URI Format

The `ca_uri` field consolidates three CA-related values into a single string:

```
ca_pool_deployed_project_id/ca_pool_name/ca_name
```

**Example:**
```
81821054389/vsa-ca-pool/vsa-intermediate-ca
```

Where:
- `ca_pool_deployed_project_id`: The GCP project ID where the CA pool is deployed
- `ca_pool_name`: The name of the CA pool
- `ca_name`: The name of the CA within the pool

### Environment Variables

The following environment variables are used as fallback values when `ca_uri` is not present in the database:

- `CA_POOL_DEPLOYED_PROJECT_ID`: Default CA pool deployed project ID
- `CA_POOL_NAME`: Default CA pool name
- `CA_NAME`: Default CA name

### Fallback Mechanism

The system implements a robust fallback mechanism to ensure backward compatibility:

1. **Primary Source**: When `ca_uri` is present in `pool_credentials`, it is parsed and used directly.
2. **Fallback**: If `ca_uri` is empty or missing, the system falls back to environment variables:
   - `CA_POOL_DEPLOYED_PROJECT_ID` → `ca_pool_deployed_project_id`
   - `CA_POOL_NAME` → `ca_pool_name`
   - `CA_NAME` → `ca_name`
3. **Partial Fallback**: If `ca_uri` exists but contains empty components (e.g., `81821054389//vsa-intermediate-ca`), the system fills in missing parts from environment variables.

### Code Implementation

#### Helper Methods

The following helper methods are available on `PoolCredentials`:

- `GetCaURIWithFallback(poolIdentifier ...string) string`: Returns the `ca_uri` from the database, or builds it from environment variables if not present.
- `ParseCaURIWithFallback(poolIdentifier ...string) (caPoolDeployedProjectID, caPoolName, caName string)`: Parses the `ca_uri` and returns individual components, falling back to environment variables for missing parts.

Both methods accept an optional `poolIdentifier` parameter (pool name or UUID) for debugging purposes.

#### Utility Functions

Located in `utils/env/env.go`:

- `BuildCaURI(caPoolDeployedProjectID, caPoolName, caName string) string`: Constructs a `ca_uri` string from individual components.
- `ParseCaURI(caURI string) (caPoolDeployedProjectID, caPoolName, caName string)`: Parses a `ca_uri` string into its components, with fallback to environment variables for empty parts.

## Backward Compatibility

### Existing Pools

Existing pools that were created before this feature was implemented will continue to work without modification:

1. **No Database Migration Required**: The `ca_uri` field is optional in the `pool_credentials` JSONB structure.
2. **Automatic Fallback**: When `ca_uri` is not present, the system automatically uses environment variables.
3. **No Service Disruption**: Existing pools continue to function exactly as before.

### New Pools

When creating new pools:

1. If CA configuration is provided during pool creation, it is stored in `pool_credentials.ca_uri`.
2. If CA configuration is not provided, the system uses environment variables and may optionally store them in the database for future reference.

## Usage Examples

### Creating a Pool with CA Configuration

When creating a pool with certificate authentication, the `ca_uri` is automatically populated from environment variables if not explicitly provided:

```go
poolCredentials := &datamodel.PoolCredentials{
    CertificateID: "my-cert-id",
    AuthType:      env.USER_CERTIFICATE,
    CaURI:         env.BuildCaURI(
        env.CaPoolDeployedProjectID,
        env.CaPoolName,
        env.CaName,
    ),
}
```

### Retrieving CA Configuration

```go
// Get the full CA URI (with fallback)
// Example result: "81821054389/vsa-ca-pool/vsa-intermediate-ca"
caURI := pool.PoolCredentials.GetCaURIWithFallback(pool.Name)

// Parse into individual components (with fallback)
// Example: projectID="81821054389", poolName="vsa-ca-pool", caName="vsa-intermediate-ca"
projectID, poolName, caName := pool.PoolCredentials.ParseCaURIWithFallback(pool.Name)
```

## Benefits

1. **Per-Pool Configuration**: Each pool can now have its own CA configuration, enabling multi-tenant scenarios.
2. **Backward Compatible**: Existing pools continue to work without any changes.
3. **Flexible**: Supports both database-stored and environment variable-based configuration.
4. **Hyperscaler Agnostic**: The `ca_uri` format is generic and can be extended for other cloud providers.

## Migration Path

For existing pools that need to migrate to database-stored CA configuration:

1. Update the `pool_credentials` JSONB field in the database to include `ca_uri`.
2. The `ca_uri` value should be constructed using: `BuildCaURI(projectID, poolName, caName)`.
   - Example: `BuildCaURI("81821054389", "vsa-ca-pool", "vsa-intermediate-ca")` results in `"81821054389/vsa-ca-pool/vsa-intermediate-ca"`
3. Once stored, the pool will use the database value instead of environment variables.

## Related Components

- **Pool Creation**: `core/orchestrator/pool.go` - Handles pool creation and CA URI population
- **Certificate Management**: `hyperscaler/ontap_provider.go` - Uses CA URI for certificate operations
- **Node Creation**: `hyperscaler/ontap_provider.go` - Populates CA URI in Node struct
- **API Endpoints**: `vcp-core/handlers/pool_endpoint.go` - Exposes CA URI in API responses

## Debugging

Debug logging has been added to trace the source of CA parameters:

- When `ca_uri` is found in the database, logs indicate: `"using ca_uri from DB"`
- When falling back to environment variables, logs indicate: `"ca_uri not found in DB, using env vars"`
- Pool identifier (name or UUID) is included in debug messages for easier troubleshooting

## Future Enhancements

Potential future improvements:

1. **API Support**: Allow specifying `ca_uri` directly in pool creation/update API requests
2. **Migration Tool**: Automated tool to migrate existing pools to database-stored CA configuration
3. **Validation**: Enhanced validation of `ca_uri` format and component values
4. **Multi-Cloud Support**: Extend the format to support other cloud providers' CA configurations

