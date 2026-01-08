# ONTAP Proxy API Translation Layer - High Level Design

> **Document Type:** Confluence Design Document  
> **Author:** gauravo  
> **Status:** 🟢 Implemented (Phase 1)  
> **Last Updated:** January 7, 2026  
> **Reviewers:** [Add reviewers]  
> **Approvers:** [Add approvers]

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Goals & Non-Goals](#3-goals--non-goals)
4. [Solution Overview](#4-solution-overview)
5. [Architecture](#5-architecture)
6. [Implementation Details](#6-implementation-details)
7. [Credential Handling](#7-credential-handling)
8. [Request Flow](#8-request-flow)
9. [API Translation Details](#9-api-translation-details)
10. [VLM Improvements Required](#10-vlm-improvements-required)
11. [Security Considerations](#11-security-considerations)
12. [Appendix](#12-appendix)

---

## 1. Executive Summary

This document describes the design for an **API Translation Layer** in the ONTAP Proxy service. This layer enables specific API endpoints to be translated into different ONTAP operations, primarily to support operations that are only available via ONTAP's CLI passthrough API (`/api/private/cli`).

### Key Changes

| Component | Current State | Proposed State |
|-----------|--------------|----------------|
| **ONTAP Proxy Routing** | All requests go through RuleEngine → ReverseProxy | Special APIs routed to dedicated handlers; others use ReverseProxy |
| **Core API Credentials** | Returns only `gcnvadmin` credentials | Returns either `gcnvadmin` OR `admin` credentials based on request |
| **Credential Middleware** | Fetches `gcnvadmin` for all requests | Identifies special APIs and requests `admin` credentials when needed |
| **Private CLI Access** | Not supported | Supported via translation handlers with admin credentials |

---

## 2. Problem Statement

### 2.1 Current Limitation

The ONTAP Proxy currently operates as a reverse proxy, forwarding REST API requests directly to ONTAP clusters. However, certain ONTAP operations are **not available** through REST APIs and can only be performed via:

- ONTAP CLI commands
- The private CLI passthrough API (`POST /api/private/cli`)

### 2.2 Specific Use Case: Snaplock Privileged Delete

The `DELETE /storage/snaplock/file/{volume.uuid}/{path}` operation requires:

1. **CLI-only operation** - The privileged delete is only available via CLI
2. **Admin credentials** - The private CLI requires `admin` user (not `gcnvadmin`)
3. **Parameter translation** - REST API uses `volume.uuid`, but CLI requires `volume_name` and `vserver_name`

### 2.3 Current Architecture Gap

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Current Flow                                                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  CCFE ──► ONTAP Proxy ──► ONTAP REST API (as gcnvadmin)                 │
│                │                                                         │
│                └── Uses only gcnvadmin credentials                       │
│                └── No CLI passthrough support                            │
│                └── Private CLI requires admin credentials ❌             │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Goals & Non-Goals

### ✅ Goals

| # | Goal |
|---|------|
| G1 | Enable ONTAP Proxy to translate specific REST API requests to CLI commands |
| G2 | Support admin credential retrieval for APIs requiring elevated privileges |
| G3 | Maintain backward compatibility - existing passthrough APIs continue to use `gcnvadmin` |
| G4 | Establish a reusable pattern for future API translations |
| G5 | Ensure secure handling of admin credentials |

### ❌ Non-Goals

| # | Non-Goal |
|---|----------|
| NG1 | Exposing raw CLI access to end users |
| NG2 | Changing the existing passthrough behavior for standard APIs |
| NG3 | Supporting arbitrary CLI commands beyond pre-defined translations |

---

## 4. Solution Overview

### 4.1 High-Level Approach

Introduce a **Translation Layer** that:

1. **Intercepts** specific API endpoints before the standard reverse proxy
2. **Identifies** APIs requiring admin credentials
3. **Fetches** appropriate credentials (admin vs gcnvadmin) from Core API
4. **Translates** requests to ONTAP CLI commands
5. **Transforms** CLI responses back to REST API format

### 4.2 Components Affected

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Components to Modify                               │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  1. Core API Service                                                      │
│     └── Add support for fetching admin credentials                        │
│     └── New parameter: credentialType (gcnvadmin | admin)                 │
│                                                                           │
│  2. ONTAP Proxy - Credential Middleware                                   │
│     └── Identify "special" APIs that need admin credentials               │
│     └── Request appropriate credential type from Core API                 │
│                                                                           │
│  3. ONTAP Proxy - New Handlers Package                                    │
│     └── Translation handlers for specific endpoints                       │
│     └── CLI command builders                                              │
│     └── Response transformers                                             │
│                                                                           │
│  4. ONTAP Proxy - Routing (main.go)                                       │
│     └── Route special endpoints to dedicated handlers                     │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Architecture

### 5.1 Current Architecture

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                              ONTAP Proxy (Current)                             │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Request                                                                      │
│      │                                                                         │
│      ▼                                                                         │
│  ┌──────────────────┐                                                          │
│  │ URL Validation   │                                                          │
│  └────────┬─────────┘                                                          │
│           ▼                                                                    │
│  ┌──────────────────┐                                                          │
│  │   Body Limit     │                                                          │
│  └────────┬─────────┘                                                          │
│           ▼                                                                    │
│  ┌──────────────────┐      ┌──────────────┐                                   │
│  │   Credential     │ ───► │  Core API    │  Fetches gcnvadmin only           │
│  │   Middleware     │ ◄─── │              │                                   │
│  └────────┬─────────┘      └──────────────┘                                   │
│           ▼                                                                    │
│  ┌──────────────────┐                                                          │
│  │  Certificate     │                                                          │
│  └────────┬─────────┘                                                          │
│           ▼                                                                    │
│  ┌──────────────────┐                                                          │
│  │   Rule Engine    │                                                          │
│  └────────┬─────────┘                                                          │
│           ▼                                                                    │
│  ┌──────────────────┐      ┌──────────────┐                                   │
│  │  Reverse Proxy   │ ───► │    ONTAP     │  Uses gcnvadmin credentials       │
│  └──────────────────┘      └──────────────┘                                   │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Implemented Architecture

The implementation uses **ogen** (OpenAPI code generator) for type-safe handlers, with routing split at the chi router level:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                              ONTAP Proxy (Implemented)                         │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Request                                                                      │
│      │                                                                         │
│      ▼                                                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐     │
│  │                    Chi Router + Global Middleware                     │     │
│  │  ┌────────────────────────────────────────────────────────────────┐  │     │
│  │  │  • httphelpers.LoggingHttpHandler                               │  │     │
│  │  │  • log.LoggingMiddleware                                        │  │     │
│  │  │  • log.RecoverMiddleware                                        │  │     │
│  │  │  • auth.AuthMiddleware (JWT validation, headers in context)     │  │     │
│  │  └────────────────────────────────────────────────────────────────┘  │     │
│  └────────┬─────────────────────────────────────────────────────────────┘     │
│           │                                                                    │
│           ▼                                                                    │
│  ╔════════════════════════════════════════════════════════════════════════╗   │
│  ║              ROUTING DECISION (at chi router level)                     ║   │
│  ╠════════════════════════════════════════════════════════════════════════╣   │
│  ║                                                                         ║   │
│  ║   Route: /v1beta/projects/{projectId}/locations/{locationId}/          ║   │
│  ║          pools/{poolId}/ontap/...                                       ║   │
│  ║                                                                         ║   │
│  ║   ┌─────────────────────────────────────────────────────────────────┐  ║   │
│  ║   │ Ogen Handler Route (no chi middleware)                           │  ║   │
│  ║   │                                                                  │  ║   │
│  ║   │  DELETE /api/storage/snaplock/file/{volumeUuid}/*               │  ║   │
│  ║   │         │                                                        │  ║   │
│  ║   │         ▼                                                        │  ║   │
│  ║   │  ┌────────────────────────────────────────────────────────────┐ │  ║   │
│  ║   │  │              Ogen Handler (endpoints.go)                    │ │  ║   │
│  ║   │  │                                                            │ │  ║   │
│  ║   │  │  1. SetupCredentialsForHandler(ctx, admin)  ──► Core API   │ │  ║   │
│  ║   │  │  2. EnsureCertificateOrPassword(ctx)        ──► SecretMgr  │ │  ║   │
│  ║   │  │  3. NewOntapClientFromContext(ctx)          ──► ClientPool │ │  ║   │
│  ║   │  │  4. GetVolumeByUUID(volumeUuid)             ──► ONTAP      │─┼──╬───► ONTAP
│  ║   │  │  5. ExecuteCLI(command)                     ──► ONTAP      │ │  ║   │
│  ║   │  │  6. Return typed ogen response                             │ │  ║   │
│  ║   │  └────────────────────────────────────────────────────────────┘ │  ║   │
│  ║   └─────────────────────────────────────────────────────────────────┘  ║   │
│  ║                                                                         ║   │
│  ║   ┌─────────────────────────────────────────────────────────────────┐  ║   │
│  ║   │ Passthrough Routes (chi middleware chain)                        │  ║   │
│  ║   │                                                                  │  ║   │
│  ║   │  /* (all other /ontap/* requests)                               │  ║   │
│  ║   │         │                                                        │  ║   │
│  ║   │         ▼                                                        │  ║   │
│  ║   │  ┌────────────────────────────────────────────────────────────┐ │  ║   │
│  ║   │  │  • URLValidationMiddleware()                                │ │  ║   │
│  ║   │  │  • bodyLimitMiddleware()                                    │ │  ║   │
│  ║   │  │  • CredentialMiddleware() ──► Core API (always gcnvadmin)  │ │  ║   │
│  ║   │  │  • CertificateMiddleware() ──► Secret Manager               │ │  ║   │
│  ║   │  │  • RuleEngineMiddleware()                                   │ │  ║   │
│  ║   │  └────────────────────────────────────────────────────────────┘ │  ║   │
│  ║   │         │                                                        │  ║   │
│  ║   │         ▼                                                        │  ║   │
│  ║   │  ┌────────────────────────────────────────────────────────────┐ │  ║   │
│  ║   │  │  Reverse Proxy (gcnvadmin credentials)                     │─┼──╬───► ONTAP
│  ║   │  └────────────────────────────────────────────────────────────┘ │  ║   │
│  ║   └─────────────────────────────────────────────────────────────────┘  ║   │
│  ║                                                                         ║   │
│  ╚════════════════════════════════════════════════════════════════════════╝   │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 Key Implementation Decisions

| Decision | Rationale |
|----------|-----------|
| **Ogen for typed handlers** | Type-safe request/response handling, automatic parameter validation, OpenAPI spec as source of truth |
| **Route split at chi level** | Ogen handlers bypass chi middleware chain; call auth functions directly with typed parameters |
| **No credential routing in middleware** | Admin routes handled by ogen handlers; middleware always uses gcnvadmin for passthrough |
| **Connection pooling** | Reuse HTTP clients per pool+auth combination for efficiency |
| **Typed error responses** | Ogen handlers return typed errors (e.g., `SnaplockFileDeleteBadRequest`) instead of Go errors |

---

## 6. Implementation Details

### 6.1 File Structure

```
ontap-proxy/
├── api/
│   ├── api.yaml                           # OpenAPI spec with snaplock endpoint
│   ├── endpoints/
│   │   ├── endpoints.go                   # Ogen handler implementations
│   │   └── endpoints_test.go              # Handler unit tests
│   └── ontap-proxy-servergen/             # Ogen generated code
│       ├── oas_handlers_gen.go
│       ├── oas_parameters_gen.go
│       ├── oas_schemas_gen.go
│       └── ...
├── handlers/
│   ├── ontap_client.go                    # ONTAP REST client with connection pooling
│   ├── ontap_client_test.go
│   ├── snaplock_file.go                   # CLI command builder, constants
│   ├── snaplock_file_test.go
│   ├── response_transformer.go            # CLI response parsing (IsCLISuccess, ParseCLIError)
│   └── response_transformer_test.go
├── middleware/
│   ├── auth_setup.go                      # SetupCredentialsForHandler() for ogen handlers
│   ├── auth_setup_test.go
│   ├── cert_setup.go                      # EnsureCertificateOrPassword() for ogen handlers
│   ├── cert_setup_test.go
│   ├── credential.go                      # CredentialMiddleware (passthrough only)
│   └── certificate.go                     # CertificateMiddleware (passthrough only)
├── cache/
│   └── auth_cache.go                      # Auth data cache with helper functions
├── reverseproxy/
│   └── ontap_proxy.go                     # Global client pool, reverse proxy
└── main.go                                # Chi router setup with ogen integration
```

### 6.2 Routing Configuration (main.go)

```go
func setupHTTPServer(handler http.Handler) *http.Server {
    mux := chi.NewRouter()

    // Global middleware (applied to all routes)
    mux.Use(httphelpers.LoggingHttpHandler)
    mux.Use(log.LoggingMiddleware)
    mux.Use(log.RecoverMiddleware)
    mux.Use(auth.AuthMiddleware(false)) // JWT validation, headers in context

    ontapProxy := reverseproxy.BuildOntapRESTProxy()

    // ONTAP API routes
    mux.Route("/v1beta/projects/{projectId}/locations/{locationId}/pools/{poolId}/ontap", func(r chi.Router) {
        // Ogen-handled routes (handler calls auth functions directly)
        r.Delete("/api/storage/snaplock/file/{volumeUuid}/*", handler.ServeHTTP)

        // Passthrough routes (chi middleware for reverse proxy)
        r.Group(func(r chi.Router) {
            r.Use(middleware.URLValidationMiddleware())
            r.Use(bodyLimitMiddleware(dsl.MaxRequestBodySize))
            r.Use(middleware.CredentialMiddleware())  // Always gcnvadmin
            r.Use(middleware.CertificateMiddleware())
            r.Use(middleware.RuleEngineMiddleware())
            r.Handle("/*", ontapProxy)
        })
    })

    // Mount OpenAPI server for /health endpoint
    mux.Mount("/", handler)

    return &http.Server{Handler: mux, ...}
}
```

### 6.3 OpenAPI Specification (api.yaml)

```yaml
paths:
  /v1beta/projects/{projectNumber}/locations/{locationId}/pools/{poolId}/ontap/api/storage/snaplock/file/{volumeUuid}/{filePath}:
    delete:
      operationId: snaplockFileDelete
      summary: Delete a SnapLock file using privileged delete
      tags:
        - snaplock
      parameters:
        - $ref: '#/components/parameters/projectNumberPathParameter'
        - $ref: '#/components/parameters/locationIdPathParameter'
        - $ref: '#/components/parameters/poolIdPathParameter'
        - $ref: '#/components/parameters/volumeUuidPathParameter'
        - $ref: '#/components/parameters/filePathPathParameter'
      responses:
        '200':
          description: Snaplock file delete initiated successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/SnaplockFileRetentionJobLinkResponse'
        '400':
          description: Bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        # ... other responses
```

### 6.4 Ogen Handler Implementation (endpoints.go)

```go
func (h Handler) SnaplockFileDelete(
    ctx context.Context,
    params oasgenserver.SnaplockFileDeleteParams,
) (oasgenserver.SnaplockFileDeleteRes, error) {
    logger := util.GetLogger(ctx)

    // 1. Setup admin credentials
    ctx, err := middleware.SetupCredentialsForHandler(
        ctx,
        params.ProjectNumber,
        params.PoolId.String(),
        middleware.CredentialTypeAdmin,
    )
    if err != nil {
        return &oasgenserver.SnaplockFileDeleteUnauthorized{
            Message: "authentication error: " + err.Error(),
        }, nil
    }

    // 2. Ensure certificate/password is fetched
    if err := middleware.EnsureCertificateOrPassword(ctx); err != nil {
        return &oasgenserver.SnaplockFileDeleteInternalServerError{
            Message: "credential setup failed: " + err.Error(),
        }, nil
    }

    // 3. Get ONTAP client
    ontapClient, err := handlers.NewOntapClientFromContext(ctx)
    if err != nil {
        return &oasgenserver.SnaplockFileDeleteUnauthorized{
            Message: "client creation failed: " + err.Error(),
        }, nil
    }

    // 4. Get volume info (need volume name and SVM name for CLI)
    volumeInfo, err := ontapClient.GetVolumeByUUID(ctx, params.VolumeUuid.String())
    if err != nil {
        return &oasgenserver.SnaplockFileDeleteNotFound{
            Message: "volume not found: " + err.Error(),
        }, nil
    }

    // 5. Build and execute CLI command
    filePath := strings.TrimPrefix(params.FilePath, "/")
    fullFilePath := fmt.Sprintf("/vol/%s/%s", volumeInfo.Name, filePath)
    cliCommand := handlers.BuildSnaplockDeleteCommand(fullFilePath, volumeInfo.SVM.Name)

    cliResponse, err := ontapClient.ExecuteCLI(ctx, cliCommand, handlers.SnaplockPrivilegeLevel)
    if err != nil {
        // Return typed error response
        return &oasgenserver.SnaplockFileDeleteBadRequest{
            Message: fmt.Sprintf("ONTAP operation failed: %s", err.Error()),
        }, nil
    }

    // 6. Check CLI success
    if !handlers.IsCLISuccess(cliResponse.Output) {
        _, message := handlers.ParseCLIError(cliResponse.Output)
        return &oasgenserver.SnaplockFileDeleteBadRequest{
            Message: fmt.Sprintf("snaplock delete failed: %s", message),
        }, nil
    }

    // 7. Return success response
    return &oasgenserver.SnaplockFileRetentionJobLinkResponse{
        // ... response fields
    }, nil
}
```

### 6.5 Auth Setup Functions (middleware/auth_setup.go)

```go
// SetupCredentialsForHandler sets up credentials for an ogen handler.
// Unlike CredentialMiddleware, this takes explicit parameters instead of
// extracting from request URL.
func SetupCredentialsForHandler(
    ctx context.Context,
    projectNumber string,
    poolID string,
    credentialType string,  // CredentialTypeAdmin or CredentialTypeGcnvAdmin
) (context.Context, error) {
    logger := util.GetLogger(ctx)

    // Get JWT token from context (set by auth.AuthMiddleware)
    jwtToken := getJWTFromContext(ctx)

    // Determine username based on credential type
    var userName string
    if credentialType == CredentialTypeAdmin {
        userName = AdminUserName  // "admin"
    } else {
        userName = env.ExpertModeUser  // "gcnvadmin"
    }

    poolDetails := &models.PoolDetails{
        ProjectNumber: projectNumber,
        PoolID:        poolID,
        AccountName:   projectNumber,
        UserName:      userName,
    }

    cacheKey := generateCacheKey(projectNumber, poolID, userName)

    // Fetch and cache credentials (reuses existing logic)
    if err := fetchAndCacheCredentials(ctx, poolDetails, cacheKey, jwtToken, logger); err != nil {
        return ctx, fmt.Errorf("failed to setup credentials: %w", err)
    }

    return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
}
```

---

## 7. Credential Handling

### 7.1 Overview

The implementation uses a **split approach**: ogen handlers call auth functions directly with the credential type they need, while passthrough routes always use gcnvadmin.

| Credential Type | User | Permission Level | Used For | How Requested |
|-----------------|------|------------------|----------|---------------|
| `gcnvadmin` | gcnvadmin | Limited admin | Standard passthrough APIs | `CredentialMiddleware` (automatic) |
| `admin` | admin | Full admin | Private CLI, privileged operations | `SetupCredentialsForHandler()` (explicit) |

### 7.2 Credential Flow Comparison

| Aspect | Passthrough (Middleware) | Ogen Handler (Direct Call) |
|--------|--------------------------|---------------------------|
| **Trigger** | Chi middleware chain | Handler code explicitly calls |
| **Credential Type** | Always `gcnvadmin` | Explicit: `CredentialTypeAdmin` |
| **Parameters From** | Extracted from URL | Typed params from ogen |
| **JWT Source** | Request header | Context (set by `auth.AuthMiddleware`) |
| **Cache Key Format** | `projectNumber:poolID:gcnvadmin` | `projectNumber:poolID:admin` |

### 7.3 Simplified CredentialMiddleware

Since admin routes are now handled by ogen handlers directly, the `CredentialMiddleware` was **simplified** to always use gcnvadmin:

```go
// middleware/credential.go
const (
    CredentialTypeAdmin     = "admin"
    CredentialTypeGcnvAdmin = "gcnvadmin"
    AdminUserName           = "admin"
)

func CredentialMiddleware() func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            logger := util.GetLogger(r.Context())

            // All passthrough routes now use gcnvadmin credentials.
            // Admin-specific routes are handled by ogen handlers directly,
            // which call SetupCredentialsForHandler() with CredentialTypeAdmin.
            poolDetails, err := extractPoolDetailsFromRequest(r, CredentialTypeGcnvAdmin)
            if err != nil {
                http.Error(w, "Invalid URI", http.StatusBadRequest)
                return
            }

            cacheKey := generateCacheKey(poolDetails.ProjectNumber, poolDetails.PoolID, poolDetails.UserName)
            jwtToken := extractJWTTokenFromRequest(r)

            err = fetchAndCacheCredentials(r.Context(), poolDetails, cacheKey, jwtToken, logger)
            if err != nil {
                handleCredentialError(w, err)
                return
            }

            ctx := context.WithValue(r.Context(), models.AuthDataKey, cacheKey)
            next.ServeHTTP(w, r.WithContext(ctx))
        })
    }
}
```

### 7.4 Security Considerations for Admin Credentials

| Concern | Mitigation |
|---------|------------|
| Admin credential exposure | Credentials never logged; only used server-side |
| Unauthorized admin access | Only ogen handlers can request admin credentials; routes are explicit |
| Credential caching | Separate cache keys for admin vs gcnvadmin (`projectNumber:poolID:admin` vs `projectNumber:poolID:gcnvadmin`) |
| Audit trail | Log which credential type was used for each request |

---

## 8. Request Flow

### 8.1 Passthrough API Flow (Unchanged)

```
┌─────────┐    ┌──────────────┐    ┌──────────┐    ┌───────┐
│  CCFE   │───►│ ONTAP Proxy  │───►│ Core API │───►│ ONTAP │
└─────────┘    └──────────────┘    └──────────┘    └───────┘
     │                │                   │             │
     │  1. Request    │                   │             │
     │  GET /volumes  │                   │             │
     │───────────────►│                   │             │
     │                │  2. Get creds     │             │
     │                │  (gcnvadmin)      │             │
     │                │──────────────────►│             │
     │                │◄──────────────────│             │
     │                │                   │             │
     │                │  3. Forward request (gcnvadmin) │
     │                │─────────────────────────────────►
     │                │◄─────────────────────────────────
     │  4. Response   │                   │             │
     │◄───────────────│                   │             │
```

### 8.2 Ogen Handler Flow (Implemented)

```
┌─────────┐    ┌──────────────────────────────────────────────────────────────┐    ┌───────┐
│  CCFE   │    │                        ONTAP Proxy                            │    │ ONTAP │
└────┬────┘    │  ┌─────────────────────────────────────────────────────────┐  │    └───┬───┘
     │         │  │                   Ogen Handler                          │  │        │
     │         │  │                  (endpoints.go)                         │  │        │
     │         │  └─────────────────────────────────────────────────────────┘  │        │
     │         └──────────────────────────────────────────────────────────────┘        │
     │                          │                                                      │
     │  1. DELETE /v1beta/projects/{proj}/locations/{loc}/pools/{pool}/                │
     │     ontap/api/storage/snaplock/file/{volumeUuid}/{filePath}                     │
     │─────────────────────────►│                                                      │
     │                          │                                                      │
     │                          │  2. SetupCredentialsForHandler(ctx, admin)           │
     │                          │     ├── Get JWT from context (set by AuthMiddleware) │
     │                          │     ├── Call Core API for admin credentials          │
     │                          │     └── Cache credentials, set AuthDataKey in ctx    │
     │                          │                                                      │
     │                          │  3. EnsureCertificateOrPassword(ctx)                 │
     │                          │     └── Fetch cert/password from Secret Manager      │
     │                          │                                                      │
     │                          │  4. NewOntapClientFromContext(ctx)                   │
     │                          │     └── Get/create HTTP client from connection pool  │
     │                          │                                                      │
     │                          │  5. GetVolumeByUUID(volumeUuid)                       │
     │                          │─────────────────────────────────────────────────────►│
     │                          │◄─────────────────────────────────────────────────────│
     │                          │     └── Extract volume_name, svm_name                │
     │                          │                                                      │
     │                          │  6. BuildSnaplockDeleteCommand(filePath, svmName)    │
     │                          │     └── "vserver context -username snaplock-user;    │
     │                          │         vol file privileged-delete -file /vol/..."   │
     │                          │                                                      │
     │                          │  7. ExecuteCLI(command, "admin")                     │
     │                          │     └── POST /api/private/cli                        │
     │                          │─────────────────────────────────────────────────────►│
     │                          │◄─────────────────────────────────────────────────────│
     │                          │                                                      │
     │                          │  8. IsCLISuccess(output) / ParseCLIError(output)     │
     │                          │     └── Check if CLI succeeded, parse errors         │
     │                          │                                                      │
     │                          │  9. Return typed ogen response                       │
     │                          │     └── SnaplockFileRetentionJobLinkResponse (200)   │
     │                          │     └── SnaplockFileDeleteBadRequest (400)           │
     │                          │     └── SnaplockFileDeleteNotFound (404)             │
     │                          │                                                      │
     │  10. Response            │                                                      │
     │◄─────────────────────────│                                                      │
```

---

## 9. API Translation Details

### 9.1 Snaplock Privileged Delete (Implemented ✅)

| Attribute | Value |
|-----------|-------|
| **Original API** | `DELETE /storage/snaplock/file/{volume.uuid}/{path}` |
| **Credential Required** | `admin` |
| **Translation Type** | REST → CLI |
| **Status** | ✅ Implemented |

#### Translation Steps

| Step | Action | Details |
|------|--------|---------|
| 1 | Extract parameters | `volumeUUID` and `path` from URL |
| 2 | Lookup volume | `GET /api/storage/volumes/{uuid}?fields=name,svm.name` |
| 3 | Build CLI command | See format below |
| 4 | Execute CLI | `POST /api/private/cli` |
| 5 | Transform response | CLI output → `snaplock_file_retention_job_link_response` |

#### CLI Command Format

```
vserver context -username snaplock-user;vol file privileged-delete -file vol/<volume_name>/<file_path> -vserver <vserver_name>
```

#### Parameter Mapping

| Source | Parameter | Target |
|--------|-----------|--------|
| URL path | `{volume.uuid}` | Used to lookup volume |
| URL path | `{path}` | `-file vol/<volume_name>/{path}` |
| Volume lookup | `name` | `<volume_name>` in file path |
| Volume lookup | `svm.name` | `-vserver <vserver_name>` |

### 9.2 SnapLock Event-Retention Operations (Future)

Event-Based Retention (EBR) allows extending the retention period of WORM files based on specific events. These APIs manage EBR policies and apply them to files/volumes.

#### 9.2.1 Apply Event-Retention Policy

| Attribute | Value |
|-----------|-------|
| **Original API** | `POST /storage/snaplock/event-retention/operations` |
| **Credential Required** | `admin` (or `gcnvprivilegedadmin`) |
| **Translation Type** | REST → CLI |
| **ONTAP CLI** | `snaplock event-retention apply` |

##### Request Body

```json
{
  "volume.name": "SLCVOL",
  "policy.name": "p1day",
  "path": "/dir1/file.txt"
}
```

##### Translation Steps

| Step | Action | Details |
|------|--------|---------|
| 1 | Extract parameters | `volume.name` or `volume.uuid`, `policy.name`, `path` |
| 2 | Lookup volume (if UUID) | `GET /api/storage/volumes/{uuid}?fields=name,svm.name` |
| 3 | Build CLI command | See format below |
| 4 | Execute CLI | `POST /api/private/cli/snaplock/event-retention/apply` |
| 5 | Transform response | CLI output → `ebr_operation` response |

##### CLI Command Format

```
snaplock event-retention apply -policy <policy_name> -path <file_path> -volume <volume_name> -vserver <vserver_name>
```

##### Parameter Mapping

| Source | Parameter | Target |
|--------|-----------|--------|
| Request body | `policy.name` | `-policy <policy_name>` |
| Request body | `path` | `-path <file_path>` |
| Request body / Lookup | `volume.name` | `-volume <volume_name>` |
| Volume lookup | `svm.name` | `-vserver <vserver_name>` |

#### 9.2.2 Abort Event-Retention Operation

| Attribute | Value |
|-----------|-------|
| **Original API** | `DELETE /storage/snaplock/event-retention/operations/{id}` |
| **Credential Required** | `admin` (or `gcnvprivilegedadmin`) |
| **Translation Type** | REST → CLI |
| **ONTAP CLI** | `snaplock event-retention abort` |

##### CLI Command Format

```
snaplock event-retention abort -id <operation_id>
```

### 9.3 SnapLock Legal-Hold (Litigation) Operations (Future)

Legal-Hold allows retaining Compliance-mode WORM files indefinitely for litigation purposes. Files under legal-hold cannot be deleted until the hold is lifted.

#### 9.3.1 Begin Legal-Hold

| Attribute | Value |
|-----------|-------|
| **Original API** | `POST /storage/snaplock/litigations` |
| **Credential Required** | `admin` (or `gcnvprivilegedadmin`) |
| **Translation Type** | REST → CLI |
| **ONTAP CLI** | `snaplock legal-hold begin` |

##### Request Body

```json
{
  "volume.name": "SLC1",
  "name": "litigation1",
  "path": "/important_file.txt"
}
```

##### Translation Steps

| Step | Action | Details |
|------|--------|---------|
| 1 | Extract parameters | `volume.name` or `volume.uuid`, `name` (litigation name), `path` |
| 2 | Lookup volume (if UUID) | `GET /api/storage/volumes/{uuid}?fields=name,svm.name` |
| 3 | Build CLI command | See format below |
| 4 | Execute CLI | `POST /api/private/cli/snaplock/legal-hold/begin` |
| 5 | Transform response | CLI output → `snaplock_litigation` response |

##### CLI Command Format

```
snaplock legal-hold begin -litigation-name <litigation_name> -volume <volume_name> -path <file_path> -vserver <vserver_name>
```

##### Parameter Mapping

| Source | Parameter | Target |
|--------|-----------|--------|
| Request body | `name` | `-litigation-name <litigation_name>` |
| Request body / Lookup | `volume.name` | `-volume <volume_name>` |
| Request body | `path` | `-path <file_path>` |
| Volume lookup | `svm.name` | `-vserver <vserver_name>` |

#### 9.3.2 End Legal-Hold

| Attribute | Value |
|-----------|-------|
| **Original API** | `DELETE /storage/snaplock/litigations/{id}` |
| **Credential Required** | `admin` (or `gcnvprivilegedadmin`) |
| **Translation Type** | REST → CLI |
| **ONTAP CLI** | `snaplock legal-hold end` |

##### Litigation ID Format

The litigation ID is a combination of volume UUID and litigation name: `<volume_uuid>:<litigation_name>`

##### Translation Steps

| Step | Action | Details |
|------|--------|---------|
| 1 | Parse litigation ID | Extract `volume_uuid` and `litigation_name` from `{id}` |
| 2 | Lookup volume | `GET /api/storage/volumes/{uuid}?fields=name,svm.name` |
| 3 | Build CLI command | See format below |
| 4 | Execute CLI | `POST /api/private/cli/snaplock/legal-hold/end` |
| 5 | Transform response | CLI output → success/error response |

##### CLI Command Format

```
snaplock legal-hold end -litigation-name <litigation_name> -volume <volume_name> -path / -vserver <vserver_name>
```

#### 9.3.3 Legal-Hold Operations on Specific Path

| Attribute | Value |
|-----------|-------|
| **Original API** | `POST /storage/snaplock/litigations/{litigation.id}/operations` |
| **Credential Required** | `admin` (or `gcnvprivilegedadmin`) |
| **Translation Type** | REST → CLI |
| **ONTAP CLI** | `snaplock legal-hold begin` or `snaplock legal-hold end` |

##### Request Body

```json
{
  "type": "begin",
  "path": "/specific_file.txt"
}
```

The `type` field determines the operation:
- `begin` → `snaplock legal-hold begin`
- `end` → `snaplock legal-hold end`

#### 9.3.4 Abort Legal-Hold Operation

| Attribute | Value |
|-----------|-------|
| **Original API** | `DELETE /storage/snaplock/litigations/{litigation.id}/operations/{id}` |
| **Credential Required** | `admin` (or `gcnvprivilegedadmin`) |
| **Translation Type** | REST → CLI |
| **ONTAP CLI** | `snaplock legal-hold abort` |

##### CLI Command Format

```
snaplock legal-hold abort -litigation-name <litigation_name> -volume <volume_name> -operation-id <operation_id>
```

### 9.4 SnapLock API Summary

| API Endpoint | Method | Operation | CLI Command | Credential |
|--------------|--------|-----------|-------------|------------|
| `/storage/snaplock/file/{volume.uuid}/{path}` | DELETE | Privileged Delete | `vol file privileged-delete` | admin |
| `/storage/snaplock/event-retention/operations` | POST | Apply EBR Policy | `snaplock event-retention apply` | admin |
| `/storage/snaplock/event-retention/operations/{id}` | DELETE | Abort EBR | `snaplock event-retention abort` | admin |
| `/storage/snaplock/litigations` | POST | Begin Legal-Hold | `snaplock legal-hold begin` | admin |
| `/storage/snaplock/litigations/{id}` | DELETE | End Legal-Hold | `snaplock legal-hold end` | admin |
| `/storage/snaplock/litigations/{litigation.id}/operations` | POST | Begin/End on Path | `snaplock legal-hold begin/end` | admin |
| `/storage/snaplock/litigations/{litigation.id}/operations/{id}` | DELETE | Abort Legal-Hold | `snaplock legal-hold abort` | admin |

### 9.5 Role Requirements

Most SnapLock CLI operations require the `vsadmin-snaplock` security login role. The credential middleware must ensure:

| Credential Type | Role | Permissions |
|-----------------|------|-------------|
| `gcnvadmin` | Standard | Cannot execute private CLI commands |
| `gcnvprivilegedadmin` | Elevated | Can execute SnapLock CLI commands via `/api/private/cli` |
| `admin` | Full Admin | Full access to all operations |

---

## 10. VLM Improvements Required (Future)

This section outlines the changes required in VLM (VSA Lifecycle Manager) to support the API translation layer with proper role-based credential management.

### 10.1 Overview

The current design uses the built-in `admin` credentials for privileged CLI operations. However, for production readiness, we need a dedicated ONTAP role with specific permissions rather than using the full `admin` role.

| Current State | Target State |
|---------------|--------------|
| Uses `admin` user for CLI operations | Uses `gcnvprivilegedadmin` user with custom role |
| Single expert mode user (`gcnvadmin`) | Two expert mode users: `gcnvadmin` + `gcnvprivilegedadmin` |
| No static snaplock user | Static snaplock user created during SVM creation |

### 10.2 New ONTAP Role: gcnvprivilegedadmin

#### 10.2.1 Role Definition

A new ONTAP role `gcnvprivilegedadmin` will be created with the following characteristics:

| Attribute | Value |
|-----------|-------|
| **Role Name** | `gcnvprivilegedadmin` |
| **Base Permissions** | All permissions from `gcnvadmin` role |
| **Additional Permissions** | Raw CLI execution, SSH access |
| **Use Cases** | Snaplock privileged delete, future BlueXP integration |

### 10.3 VCP Changes Required

#### 10.3.1 Pool Create Workflow

During pool creation, VCP will create **two users** instead of one:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Pool Create - User Creation                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Current State:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  VCP creates: gcnvadmin user                                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Target State:                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  VCP creates:                                                        │    │
│  │    1. gcnvadmin user (mapped to gcnvadmin role)                     │    │
│  │    2. gcnvprivilegedadmin user (mapped to gcnvprivilegedadmin role) │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 10.3.2 Credential Storage

| User | Storage Location | Retrieved By |
|------|------------------|--------------|
| `gcnvadmin` | Secret Manager | ONTAP Proxy (standard APIs) |
| `gcnvprivilegedadmin` | Secret Manager | ONTAP Proxy (privileged APIs) |

### 10.4 VLM Changes Required

#### 10.4.1 Expert Mode User Workflow

VLM's Expert Mode User workflow must be updated to accept **multiple users**:

| Current | Proposed |
|---------|----------|
| Single user input | Array of users input |
| Single role deployment | Multiple roles deployment |
| Single user-role mapping | Multiple user-role mappings |

```yaml
# Current Expert Mode User Request
expert_mode_user:
  username: gcnvadmin
  role: gcnvadmin

# Proposed Expert Mode User Request
expert_mode_users:
  - username: gcnvadmin
    role: gcnvadmin
  - username: gcnvprivilegedadmin
    role: gcnvprivilegedadmin
```

#### 10.4.2 Role Deployment

VLM must deploy the following roles during cluster setup:

| Role | Deployed By | Timing |
|------|-------------|--------|
| `gcnvadmin` | VLM | Cluster creation |
| `gcnvprivilegedadmin` | VLM | Cluster creation |

#### 10.4.3 SVM Create - Static Snaplock User

VLM must create a static snaplock user during SVM creation:

| Attribute | Value |
|-----------|-------|
| **User Name** | `snaplock-user` (or configurable) |
| **Created During** | SVM creation workflow |
| **Purpose** | Used in `vserver context -username snaplock-user` for privileged delete |
| **Scope** | SVM-level user |

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SVM Create Workflow (Updated)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Current Steps:                           New Steps:                         │
│  1. Create SVM                            1. Create SVM                      │
│  2. Configure SVM settings                2. Configure SVM settings          │
│  3. Create volumes                        3. Create snaplock-user  ◄── NEW   │
│  4. ...                                   4. Create volumes                  │
│                                           5. ...                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 10.5 ONTAP Proxy Changes

Once VLM changes are deployed, ONTAP Proxy will switch from using `admin` to `gcnvprivilegedadmin`:

| Component | Current | Target |
|-----------|---------|--------|
| Credential type for CLI APIs | `admin` | `gcnvprivilegedadmin` |
| Credential middleware pattern | Match → request `admin` | Match → request `gcnvprivilegedadmin` |
| Core API credential request | `credentialType=admin` | `credentialType=gcnvprivilegedadmin` |

### 10.6 Future Extensions

The `gcnvprivilegedadmin` role can be extended to support additional use cases:

| Use Case | Description | Timeline |
|----------|-------------|----------|
| BlueXP Integration | SSH/CLI access for BlueXP connector operations | Future |
| Advanced Snaplock Operations | Additional privileged snaplock commands | Future |
| Diagnostic Operations | Cluster diagnostics requiring elevated access | Future |

### 10.7 Implementation Sequence

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        Implementation Order                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Phase A: VLM Updates (Pre-requisite)                                         │
│  ├── A.1 Update Expert Mode workflow to accept multiple users                 │
│  ├── A.2 Implement gcnvprivilegedadmin role definition                        │
│  ├── A.3 Deploy both roles during cluster setup                               │
│  ├── A.4 Create snaplock-user during SVM creation                             │
│  └── A.5 Test and validate role permissions                                   │
│                                                                               │
│  Phase B: VCP Updates                                                         │
│  ├── B.1 Update pool create to provision two users                            │
│  ├── B.2 Store gcnvprivilegedadmin credentials in Secret Manager              │
│  └── B.3 Update Core API to return gcnvprivilegedadmin credentials            │
│                                                                               │
│  Phase C: ONTAP Proxy Updates                                                 │
│  ├── C.1 Switch from admin to gcnvprivilegedadmin                             │
│  └── C.2 Update credential type in middleware                                 │
│                                                                               │
│  Phase D: Migration (Existing Pools)                                          │
│  ├── D.1 Migration workflow to create gcnvprivilegedadmin on existing pools   │
│  └── D.2 Backfill credentials in Secret Manager                               │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 10.8 Summary of Changes by Component

| Component | Changes Required |
|-----------|------------------|
| **VLM** | Multi-user Expert Mode workflow, role deployment, snaplock-user creation |
| **VCP** | Create two users during pool create, store both credentials |
| **Core API** | Return gcnvprivilegedadmin credentials when requested |
| **ONTAP Proxy** | Use gcnvprivilegedadmin instead of admin for CLI operations |

---

## 11. Security Considerations

### 11.1 Credential Security

| Risk | Mitigation |
|------|------------|
| Admin credentials logged | Credentials are never logged; only masked values |
| Unauthorized admin access | Explicit allowlist of APIs that can use admin creds |
| Credential theft in transit | TLS encryption for all communication |
| Long-lived admin credentials | Credentials have TTL; refreshed as needed |

### 11.2 Command Injection Prevention

| Risk | Mitigation |
|------|------------|
| CLI command injection via file path | Input validation and proper escaping |
| Path traversal attacks | URL validation middleware + path sanitization |
| Arbitrary CLI execution | Only pre-defined command templates are used |

### 11.3 Audit & Compliance

| Event | Logged |
|-------|--------|
| Admin credential request | ✅ Yes (without credential values) |
| CLI command execution | ✅ Yes (sanitized) |
| Translation API invocation | ✅ Yes |
| Errors and failures | ✅ Yes |

---

## 12. Appendix

### A. Special API Pattern Registry

APIs that require admin credentials:

| Pattern | Method | Reason |
|---------|--------|--------|
| `/storage/snaplock/file/*` | DELETE | Privileged delete requires CLI |
| `/storage/snaplock/event-retention/operations` | POST | Apply EBR policy requires CLI |
| `/storage/snaplock/event-retention/operations/*` | DELETE | Abort EBR operation requires CLI |
| `/storage/snaplock/litigations` | POST | Begin legal-hold requires CLI |
| `/storage/snaplock/litigations/*` | DELETE | End legal-hold requires CLI |
| `/storage/snaplock/litigations/*/operations` | POST | Legal-hold begin/end on path requires CLI |
| `/storage/snaplock/litigations/*/operations/*` | DELETE | Abort legal-hold requires CLI |

### B. CLI Command Templates

```
# Snaplock Privileged Delete
vserver context -username snaplock-user;vol file privileged-delete -file vol/<volume_name>/<path> -vserver <vserver_name>

# Snaplock Event-Retention Apply
snaplock event-retention apply -policy <policy_name> -path <file_path> -volume <volume_name> -vserver <vserver_name>

# Snaplock Event-Retention Abort
snaplock event-retention abort -id <operation_id>

# Snaplock Legal-Hold Begin
snaplock legal-hold begin -litigation-name <litigation_name> -volume <volume_name> -path <file_path> -vserver <vserver_name>

# Snaplock Legal-Hold End
snaplock legal-hold end -litigation-name <litigation_name> -volume <volume_name> -path <file_path> -vserver <vserver_name>

# Snaplock Legal-Hold Abort
snaplock legal-hold abort -litigation-name <litigation_name> -volume <volume_name> -operation-id <operation_id>
```

### C. Response Schema References

- `snaplock_file_retention_job_link_response` - See ONTAP REST API documentation

### D. Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| AUTH_ADMIN_REQUIRED | 401 | Admin credentials required but not available |
| CLI_EXEC_FAILED | 502 | CLI command execution failed |
| VOLUME_NOT_FOUND | 404 | Volume UUID does not exist |
| TRANSFORM_ERROR | 500 | Response transformation failed |
| EBR_POLICY_NOT_FOUND | 404 | Event-retention policy does not exist |
| EBR_OPERATION_NOT_FOUND | 404 | EBR operation ID does not exist |
| LITIGATION_NOT_FOUND | 404 | Litigation does not exist |
| LEGAL_HOLD_IN_PROGRESS | 409 | Cannot delete litigation while operation in progress |
| FILE_UNDER_LEGAL_HOLD | 409 | Cannot apply EBR policy to file under legal-hold |
| INVALID_LITIGATION_ID | 400 | Litigation ID format is invalid |

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.1 | 2024-12-10 | gauravo | Initial draft |
| 1.0 | 2026-01-07 | gauravo | Phase 1 implementation completed: Snaplock privileged delete with ogen handlers |

---

## Implementation Status

| Feature | Status | PR |
|---------|--------|-----|
| Snaplock Privileged Delete | ✅ Implemented | VSCP-3575 |
| Event-Based Retention (EBR) | 🔲 Planned | - |
| Legal-Hold Operations | 🔲 Planned | - |
| VLM gcnvprivilegedadmin role | 🔲 Planned | - |
