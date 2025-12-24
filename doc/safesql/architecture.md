# SafeSQL Architecture

## Overview

SafeSQL is a production database safety framework that prevents accidental data corruption from manual SQL queries. It implements a **plan-then-apply** model inspired by Terraform, ensuring that:

1. Every query is validated before execution
2. State drift is detected automatically
3. All operations are audited
4. Rollback SQL is auto-generated

## Problem Statement

Running manual SQL queries against production databases is risky:
- **Missing WHERE clause**: `UPDATE volumes SET state='error'` affects ALL rows
- **Copy-paste errors**: Wrong query executed despite version control
- **State drift**: Data changes between review and execution
- **No audit trail**: Who ran what query and when?
- **No rollback**: Changes are difficult to reverse

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SafeSQL Execution Pipeline                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   GitHub                    SafeSQL CLI                     PostgreSQL      │
│   ──────                    ──────────                      ──────────      │
│                                                                             │
│   sql-queries/              ┌─────────────────────────────┐                 │
│   branch                    │  1. PLAN PHASE              │                 │
│       │                     │  ├─ Fetch from GitHub       │                 │
│       │                     │  ├─ Parse & validate SQL    │                 │
│       └────────────────────▶│  ├─ Analyze impact          │────────────────▶│
│                             │  ├─ Capture state snapshot  │  COUNT(*)       │
│                             │  ├─ Generate rollback SQL   │  SELECT *       │
│                             │  └─ Sign & store plan       │                 │
│                             └─────────────────────────────┘                 │
│                                        │                                    │
│                                        ▼                                    │
│                             ┌─────────────────────────────┐                 │
│                             │  2. APPLY PHASE             │                 │
│       ┌────────────────────▶│  ├─ Load plan               │                 │
│       │  Re-verify commit   │  ├─ Verify signature        │                 │
│       │                     │  ├─ Check plan expiry       │                 │
│       │                     │  ├─ Verify commit unchanged │────────────────▶│
│                             │  ├─ Verify state unchanged  │  COUNT(*)       │
│                             │  ├─ Execute in transaction  │  SELECT *       │
│                             │  ├─ Confirm row count       │  BEGIN/COMMIT   │
│                             │  └─ Log to audit            │                 │
│                             └─────────────────────────────┘                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Parser (`internal/parser/`)

Parses and validates SQL files:
- Extracts metadata (TICKET, AUTHOR, DESCRIPTION)
- Splits multi-statement SQL
- Detects statement types (SELECT, UPDATE, DELETE, INSERT)
- Extracts tables and WHERE clauses

**Validation Rules:**
- UPDATE/DELETE must have WHERE clause
- Blocks dangerous patterns (`WHERE 1=1`, `WHERE true`)
- Blocks DDL statements (CREATE, DROP, ALTER)
- Optional table whitelist

### 2. GitHub Client (`internal/github/`)

Integrates with GitHub for version-controlled queries:
- Fetches files from repository
- Captures commit SHA for immutability
- Retrieves PR metadata (approvers, merge time)
- Verifies commit hasn't changed at apply time

### 3. Analyzer (`internal/analyzer/`)

Analyzes query impact before execution:
- Counts affected rows (`SELECT COUNT(*)`)
- Captures row snapshots for state comparison
- Generates execution plans (`EXPLAIN`)
- Computes state hashes for drift detection

### 4. Planner (`internal/planner/`)

Creates signed execution plans:
- Bundles query, source, impact, and state
- Signs with SHA256 for tamper detection
- Sets expiry time (default: 1 hour)
- Stores to filesystem as JSON

### 5. Executor (`internal/executor/`)

Executes plans with verification:
- Verifies plan signature and expiry
- Re-checks GitHub commit (prevents copy-paste errors)
- Re-checks state hash (detects drift)
- Re-checks row count (detects concurrent changes)
- Executes in transaction with confirmation

### 6. Audit Logger (`internal/audit/`)

Records all operations:
- Plan creation
- Execution attempts (success/failure/abort)
- Rollback executions
- Stores pre-state for recovery

### 7. Rollback Generator (`internal/rollback/`)

Auto-generates undo SQL:
- UPDATE → UPDATE with original values
- DELETE → INSERT with captured data
- INSERT → DELETE by primary key

## Execution Plan Structure

```json
{
  "plan_id": "plan-20240115-143022-abc123",
  "created_at": "2024-01-15T14:30:22Z",
  "expires_at": "2024-01-15T15:30:22Z",
  "operator": "john.doe",
  "ticket": "JIRA-1234",
  
  "source": {
    "type": "github",
    "repository": "vcp-vsa/vsa-sql-queries",
    "branch": "sql-queries",
    "commit_sha": "a1b2c3d4e5f6...",
    "file_path": "queries/2024/01/JIRA-1234.sql",
    "pr_metadata": {
      "number": 42,
      "approvers": ["jane.smith"]
    }
  },
  
  "query": {
    "raw_sql": "UPDATE volumes SET state='error' WHERE uuid='abc-123'",
    "hash": "sha256:..."
  },
  
  "impact": {
    "total_rows": 1,
    "statements": [...]
  },
  
  "snapshots": [{
    "table": "volumes",
    "row_count": 1,
    "rows_hash": "sha256:...",
    "rows_preview": [{"uuid": "abc-123", "state": "creating"}]
  }],
  
  "rollback": [{
    "sql": "UPDATE volumes SET state='creating' WHERE uuid='abc-123'"
  }],
  
  "signature": "sha256:..."
}
```

## Safety Guarantees

| Check | Phase | Prevents |
|-------|-------|----------|
| WHERE required | Plan | Full-table UPDATE/DELETE |
| Dangerous patterns | Plan | `WHERE 1=1`, `WHERE true` |
| DDL blocked | Plan | Schema changes |
| Row count threshold | Plan | Accidental mass changes |
| Commit SHA match | Apply | Copy-paste wrong query |
| State hash match | Apply | Concurrent data changes |
| Row count match | Apply | Concurrent inserts/deletes |
| Plan expiry | Apply | Stale execution |
| Signature valid | Apply | Tampered plan files |
| Transaction + confirm | Apply | Unintended commits |

## Directory Structure

```
tools/safesql/
├── main.go                  # CLI entry point
├── cmd/
│   ├── root.go              # Command dispatcher
│   ├── plan.go              # Plan command
│   ├── apply.go             # Apply command
│   ├── show.go              # Show command
│   ├── audit.go             # Audit command
│   └── rollback.go          # Rollback command
├── config/
│   └── config.go            # Configuration
└── internal/
    ├── parser/              # SQL parsing
    ├── github/              # GitHub integration
    ├── database/            # DB operations
    ├── analyzer/            # Impact analysis
    ├── planner/             # Plan generation
    ├── executor/            # Safe execution
    ├── audit/               # Audit logging
    └── rollback/            # Rollback generation

.safesql/                    # Local storage (gitignored)
├── config.yaml              # Configuration
├── plans/                   # Pending plans
└── audit/                   # Audit logs

sql-queries/                 # Version-controlled queries
├── pending/                 # Queries awaiting execution
├── executed/                # Completed queries
└── templates/               # Reusable templates
```

## Configuration

```yaml
# .safesql/config.yaml
safesql:
  github:
    repository: "vcp-vsa/vsa-control-plane"
    branch: "sql-queries"
    require_github_source: true
    require_merged_pr: true
    min_approvers: 1
  
  database:
    host: "localhost"
    port: "5432"
    # Credentials from environment variables
  
  thresholds:
    max_rows_default: 100
    warning_threshold: 10
    block_threshold: 1000
    plan_expiry: "1h"
```

## Security Considerations

1. **GitHub Token**: Required for fetching queries; use environment variable
2. **Database Credentials**: Never stored in plan files; loaded at runtime
3. **Audit Logs**: Contain query text and affected data; secure appropriately
4. **Plan Files**: Signed but not encrypted; store in secure location

## Extension Points

- **Custom Validators**: Add domain-specific validation rules
- **Notification Hooks**: Slack/email on execution
- **GitHub Integration**: Auto-close PRs after execution
- **Database Support**: Currently PostgreSQL; extensible to MySQL/SQLite

