# SafeSQL Usage Guide

## Quick Start

### Running in Kubernetes Cluster

For executing SafeSQL against production databases in Kubernetes, follow these steps:

#### Step 1: Login to Jump Host

```bash
# SSH to your Kubernetes jump host
gcloud compute ssh <jump-host-name> --zone=<zone> --project=<project>
# Or using your organization's SSH method
```

#### Step 2: Port Forward to Database

```bash
# Port forward to the cloud-sql-proxy service in the cluster
kubectl port-forward -n sde svc/cloud-sql-proxy 5432:5432

# If port 5432 is already in use locally, use an alternative port:
kubectl port-forward -n sde svc/cloud-sql-proxy 5433:5432

# Keep this terminal session running
```

**Note:** The port-forward must remain active in a separate terminal session while executing SafeSQL.

#### Step 3: Set Database Environment Variables

In a new terminal (or after port-forwarding in the background), set the database connection environment variables:

```bash
export DB_HOST="localhost"          # Use localhost since we're port-forwarding
export DB_PORT="5432"                # Or 5433 if using alternative port
export DB_USER="<your-db-user>"
export DB_PASSWORD="<your-db-password>"
export DB_NAME="<your-database-name>"
export DB_SSLMODE="disable"           # SSL not needed for localhost port-forward

# Optional: GitHub token for GitHub integration
export SAFESQL_GITHUB_TOKEN="<your-github-token>"
# Or
export GITHUB_TOKEN="<your-github-token>"
```

#### Step 4: Copy the Binary

Build the Linux binary locally and copy it to the jump host:

```bash
# On your local machine, build Linux binary
make safesql-build-linux

# Copy to jump host
scp safesql-linux <user>@<jump-host>:/home/<user>/safesql
# Or using gcloud
gcloud compute scp safesql-linux <jump-host-name>:/home/<user>/safesql \
  --zone=<zone> --project=<project>

# On jump host, make it executable
chmod +x ~/safesql
```

#### Step 5: Execute SafeSQL

Now you can execute SafeSQL commands on the jump host:

```bash
# Generate a plan
~/safesql plan \
  --file /path/to/query.sql \
  --operator "your-name" \
  --ticket "TICKET-123" \
  --force

# Apply the plan
~/safesql apply --plan <plan-id>

# View plan details
~/safesql show --plan <plan-id>

# View audit history
~/safesql audit --last 10
```

**Important Notes:**
- Ensure the port-forward session remains active during execution
- Use `localhost` as DB_HOST when port-forwarding
- Set `DB_SSLMODE=disable` for localhost connections
- The binary must be in a directory that allows execution (not `/tmp` if mounted with `noexec`)
- Plans and audit logs are stored in `~/.safesql/` directory

### 1. Local Setup

```bash
# Build the tool
cd tools/safesql
go build -o safesql .

# Create config directory
mkdir -p .safesql

# Create config file
cat > .safesql/config.yaml << 'EOF'
safesql:
  github:
    repository: "your-org/your-repo"
    branch: "sql-queries"
    require_github_source: true
    min_approvers: 1
  database:
    host: "your-db-host"
    port: "5432"
    dbname: "your-database"
    sslmode: "require"
  thresholds:
    max_rows_default: 100
    plan_expiry: "1h"
EOF

# Set environment variables
export SAFESQL_GITHUB_TOKEN="your-github-token"
export DB_USER="your-db-user"
export DB_PASSWORD="your-db-password"
```

### 2. Create a Query

Create a SQL file with metadata comments:

```sql
-- queries/2024/01/JIRA-1234-fix-volume.sql

-- TICKET: JIRA-1234
-- AUTHOR: john.doe@company.com
-- DESCRIPTION: Fix stuck volume in creating state

UPDATE volumes 
SET state = 'error',
    state_details = 'Manual fix - JIRA-1234'
WHERE uuid = 'abc-123-def-456'
  AND state = 'creating';
```

### 3. Push to GitHub

```bash
git checkout sql-queries
git add queries/2024/01/JIRA-1234-fix-volume.sql
git commit -m "JIRA-1234: Fix stuck volume"
git push origin sql-queries

# Create PR and get approval
gh pr create --title "JIRA-1234: Fix stuck volume"
# Wait for approval, then merge
```

### 4. Generate Plan

```bash
safesql plan \
  --github "sql-queries:queries/2024/01/JIRA-1234-fix-volume.sql" \
  --operator john.doe \
  --ticket JIRA-1234
```

Output:
```
╔════════════════════════════════╗
║  EXECUTION PLAN GENERATED      ║
╚════════════════════════════════╝

  Plan ID: plan-20240115-143022-abc123
  Expires: 2024-01-15T15:30:22Z

  Source:
    Type: github
    Repository: your-org/your-repo
    Branch: sql-queries
    Commit: a1b2c3d4e5f6
    PR #42: JIRA-1234: Fix stuck volume
    Approvers: [jane.smith]

  Impact Analysis:
    Total Statements: 1
    Total Rows Affected: 1
    Tables: [volumes]

  Statement 1:
    Type: UPDATE
    Table: volumes
    Rows: 1
    Preview:
      uuid=abc-123-def-456, state=creating, name=vol-1

  Plan saved to: .safesql/plans/plan-20240115-143022-abc123.json

  Next step:
    safesql apply --plan plan-20240115-143022-abc123
```

### 5. Apply Plan

```bash
safesql apply --plan plan-20240115-143022-abc123
```

Output:
```
╔═══════════════════╗
║  VERIFYING PLAN   ║
╚═══════════════════╝

  ✓ Plan not expired (47 minutes remaining)
  ✓ Plan signature valid
  ✓ Commit SHA matches (a1b2c3d4e5f6)
  ✓ State hash matches (no drift)
  ✓ Row count unchanged (1 row)

╔═══════════════════════╗
║  READY TO EXECUTE     ║
╚═══════════════════════╝

  All verifications passed. The following queries will be executed:

  [1] UPDATE volumes SET state = 'error', state_details = '...' WHERE uuid = '...'

  Type 'APPLY' to execute, or 'ABORT' to cancel: APPLY

╔═════════════╗
║  EXECUTING  ║
╚═════════════╝

  Transaction preview:
    Statement 1: 1 rows affected
    Total: 1 rows

  Type 'COMMIT' to finalize, or 'ROLLBACK' to cancel: COMMIT

╔══════════════════════════╗
║  EXECUTION SUCCESSFUL    ║
╚══════════════════════════╝

  Rows affected: 1
  Duration: 12ms
  Audit ID: exec-20240115-143522-xyz789

  Rollback available:
    safesql rollback --audit exec-20240115-143522-xyz789
```

## Commands Reference

### `safesql plan`

Generate an execution plan from a SQL file.

```bash
# From GitHub (recommended)
safesql plan --github "branch:path/to/query.sql" --operator NAME --ticket TICKET

# Full GitHub reference
safesql plan --github "owner/repo@branch:path/to/query.sql" --operator NAME --ticket TICKET

# From local file (requires --force if GitHub source is required)
safesql plan --file query.sql --operator NAME --ticket TICKET --force
```

**Options:**
- `--github`: GitHub reference (required unless --file)
- `--file`: Local file path
- `--operator`: Your name/email (required)
- `--ticket`: JIRA/issue reference (required)
- `--force`: Allow local files when GitHub is required

### `safesql apply`

Execute a plan after verification.

```bash
safesql apply --plan PLAN_ID
```

**Options:**
- `--plan`: Plan ID to apply (required)
- `--force`: Skip warnings (not recommended)

### `safesql show`

Display plan details.

```bash
safesql show --plan PLAN_ID
safesql show --plan PLAN_ID --json
```

**Options:**
- `--plan`: Plan ID to show (required)
- `--json`: Output as JSON

### `safesql audit`

View execution history.

```bash
# Show last 10 entries
safesql audit --last 10

# Show specific entry
safesql audit --id AUDIT_ID

# Show entries for a date
safesql audit --date 2024-01-15

# Output as JSON
safesql audit --last 10 --json
```

**Options:**
- `--id`: Show specific audit entry
- `--last`: Show last N entries
- `--date`: Show entries for date (YYYY-MM-DD)
- `--json`: Output as JSON

### `safesql rollback`

Undo a previous execution.

```bash
# Preview rollback
safesql rollback --audit AUDIT_ID --dry-run

# Execute rollback
safesql rollback --audit AUDIT_ID --operator NAME
```

**Options:**
- `--audit`: Audit ID to rollback (required)
- `--dry-run`: Show SQL without executing
- `--operator`: Operator performing rollback

## SQL File Format

### Required Metadata

```sql
-- TICKET: JIRA-1234       -- Required: Issue/ticket reference
-- AUTHOR: john@email.com  -- Recommended: Author email
-- DESCRIPTION: Brief desc -- Recommended: What this query does
```

### Multi-Statement Queries

```sql
-- TICKET: JIRA-5678
-- DESCRIPTION: Update volume and snapshots

-- Update volume state
UPDATE volumes 
SET state = 'error' 
WHERE uuid = 'abc-123';

-- Update related snapshots
UPDATE snapshots 
SET state = 'error' 
WHERE volume_id = 42;
```

## Error Scenarios

### Missing WHERE Clause

```
╔═════════════════════╗
║  VALIDATION FAILED  ║
╚═════════════════════╝

  ❌ REQUIRE_WHERE: UPDATE statement without WHERE clause - this would affect ALL rows
```

### Commit Changed (Copy-Paste Error)

```
╔══════════════════════╗
║  EXECUTION BLOCKED   ║
╚══════════════════════╝

  ❌ Commit SHA mismatch (file changed since plan)
     Plan: a1b2c3d4e5f6
     Current: x9y8z7w6v5u4

  Create a new plan to capture the current state.
```

### State Drift

```
╔══════════════════════╗
║  EXECUTION BLOCKED   ║
╚══════════════════════╝

  ❌ State drift detected for statement 1 (table: volumes)
     Data has changed since plan creation.

  Create a new plan to capture the current state.
```

### Plan Expired

```
╔═════════════════╗
║  PLAN EXPIRED   ║
╚═════════════════╝

  Plan created: 2024-01-15 14:30:22 UTC
  Plan expired: 2024-01-15 15:30:22 UTC
  Current time: 2024-01-15 16:45:00 UTC

  Create a new plan: safesql plan --github "..."
```

## Best Practices

1. **Always use GitHub source**: Prevents copy-paste errors and provides audit trail
2. **Get PR approvals**: Require at least one reviewer
3. **Include meaningful metadata**: TICKET, AUTHOR, DESCRIPTION help with audits
4. **Use specific WHERE clauses**: Include UUIDs or primary keys
5. **Review plan output carefully**: Check affected rows before applying
6. **Keep plans fresh**: Apply within 1 hour of creation
7. **Don't skip confirmations**: Read what you're committing

## Troubleshooting

### "GitHub token required"

```bash
export SAFESQL_GITHUB_TOKEN="ghp_xxxx"
# Or
export GITHUB_TOKEN="ghp_xxxx"
```

### "Failed to connect to database"

Check environment variables:
```bash
export DB_HOST="your-host"
export DB_USER="your-user"  
export DB_PASSWORD="your-password"
export DB_NAME="your-database"
```

### "Table does not exist"

Verify the table name in your query matches the database schema.

### "Plan not found"

Plans are stored in `.safesql/plans/`. Ensure you're in the correct directory.

