# SafeSQL Usage Guide

## Execution Modes

SafeSQL supports two execution modes:

1. **PR Workflow** (Recommended for Production) - Submit SQL via Pull Requests, generate plans automatically, review before merge, then apply. Details below.

2. **Direct Execution** (Development/Testing) - Execute SQL directly from local files or GitHub references. Described below.

## Quick Start

### Prerequisites

#### GCS Bucket Setup

SafeSQL stores all artifacts (plans, audit logs, rollback SQL) in a GCS bucket. You must create and configure a bucket before using SafeSQL:

**1. Create a GCS bucket:**

```bash
# Choose a unique bucket name
export SAFESQL_GCS_BUCKET="safesql-<your-project>-<environment>"

# Create the bucket
gsutil mb -p <your-gcp-project> -l <region> gs://${SAFESQL_GCS_BUCKET}

# Set lifecycle policy to auto-delete old objects (optional, recommended)
cat > /tmp/lifecycle.json << 'EOF'
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 90}
      }
    ]
  }
}
EOF
gsutil lifecycle set /tmp/lifecycle.json gs://${SAFESQL_GCS_BUCKET}
```

**2. Grant access:**

```bash
# For service accounts (Kubernetes pods)
gcloud storage buckets add-iam-policy-binding gs://${SAFESQL_GCS_BUCKET} \
  --member="serviceAccount:<service-account-email>" \
  --role="roles/storage.objectAdmin"

# For user accounts (jump host)
gcloud storage buckets add-iam-policy-binding gs://${SAFESQL_GCS_BUCKET} \
  --member="user:<your-email>" \
  --role="roles/storage.objectAdmin"
```

**3. Set environment variable:**

```bash
export SAFESQL_GCS_BUCKET="safesql-<your-project>-<environment>"
```

**Note:** The bucket name must be set via `SAFESQL_GCS_BUCKET` environment variable. SafeSQL will not work without it.

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
# Database connection
export DB_HOST="localhost"          # Use localhost since we're port-forwarding
export DB_PORT="5432"                # Or 5433 if using alternative port
export DB_USER="<your-db-user>"
export DB_PASSWORD="<your-db-password>"
export DB_NAME="<your-database-name>"
export DB_SSLMODE="disable"           # SSL not needed for localhost port-forward

# GCS storage (REQUIRED)
export SAFESQL_GCS_BUCKET="safesql-<your-project>-<environment>"

# GitHub integration (REQUIRED for PR workflow)
export SAFESQL_GITHUB_TOKEN="<your-github-token>"
# Or
export GITHUB_TOKEN="<your-github-token>"
```

#### Step 4: Download or Build the Binary

**Option 1: Download from GitHub Release (Recommended)**

```bash
# Download the latest release (Linux AMD64)
curl -L \
  -H "Accept: application/octet-stream" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/VCP-VSA-control-Plane/vsa-control-plane/releases/assets/372289464 \
  -o safesql

chmod +x safesql
```

**Option 2: Build Locally**

```bash
# On your local machine, build Linux binary
cd /path/to/vsa-control-plane
GOOS=linux GOARCH=amd64 go build -o safesql ./tools/safesql

# Copy to jump host
scp safesql <user>@<jump-host>:/home/<user>/safesql
# Or using gcloud
gcloud compute scp safesql <jump-host-name>:/home/<user>/safesql \
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

## PR Workflow (Recommended for Production)

The PR workflow enables review-based SQL execution with automatic plan generation and validation.

### Quick Example

```bash
# 1. Create PR with SQL file (only ONE SQL file per PR)
git checkout -b fix/update-user-status
echo "UPDATE users SET status = 'active' WHERE email = 'user@example.com';" > migrations/update_status.sql
git add migrations/update_status.sql
git commit -m "Update user status"
git push origin fix/update-user-status
gh pr create --title "Update user status" --body "Ticket: JIRA-123"

# 2. Generate plan and commit to PR (creates commit suggestion)
safesql plan --pr 42 --ticket JIRA-123

# 3. Go to PR, click "Commit suggestion" to add plan file (signed with YOUR GPG key)

# 4. Get PR reviewed and approved (need 2+ approvals)

# 5. Apply from open PR (before merging)
safesql apply --pr 42

# 6. If rollback needed
safesql rollback --pr 42
```

### Key Features

- **Automatic validation**: Pre-validates SQL before committing plan
- **Single SQL file per PR**: Enforced validation rule
- **Plan naming**: Automatically named as `<sql-filename>-plan.json`
- **Plan expiry**: Plans expire after 1 hour to ensure fresh database state
- **Plan reuse**: If valid plan exists and SQL hasn't changed, reuses it automatically
- **Commit suggestions**: Creates GitHub review comments with commit suggestions (no direct commits)
- **GPG signing**: You commit via GitHub UI with your own GPG key
- **Approval enforcement**: Requires 2+ approvals before apply
- **Open PR apply**: Apply from open PR (not merged) for better workflow
- **Rollback support**: Rollback SQL generated during plan phase, stored in plan file
- **Audit trail**: Full traceability from PR to execution
- **Robust SQL parsing**: Handles PostgreSQL-specific syntax with proper parser

### Commands

```bash
# Generate plan from PR (creates commit suggestion)
safesql plan --pr <PR_NUMBER> --ticket <TICKET_ID>

# Show plan from PR
safesql show --pr <PR_NUMBER>

# Apply plan from open PR (requires 2+ approvals)
safesql apply --pr <PR_NUMBER>

# Rollback from PR (reads rollback SQL from plan file)
safesql rollback --pr <PR_NUMBER>
```

### Requirements

- PR must contain exactly ONE SQL file
- Plan must be < 1 hour old when applying
- PR must be **open** (not merged) when applying
- PR must have at least **2 approvals** before applying
- GitHub token with proper permissions (see below)

### GitHub Token Permissions

For PR workflow to work, the GitHub token must have the following permissions:

**For Classic Personal Access Tokens:**
- `repo` (Full control of private repositories)
  - Needed to read PR details, list files, read file contents, and post comments

**For Fine-Grained Personal Access Tokens (recommended):**
- **Repository access**: Select the specific repository
- **Permissions**:
  - **Pull requests**: Read and write (to read PR details and post comments)
  - **Contents**: Read (to read SQL files from PR)
  - **Metadata**: Read (automatically included)

**Specifically needed for SafeSQL:**
- **Read access**: Fetch PR details, list PR files, read file contents
- **Write access**: Commit plan files to PR branches

**How to create a token:**

1. Go to GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Give it a descriptive name (e.g., "SafeSQL PR Workflow")
4. Select scopes:
   - ✅ `repo` (check the top-level box, all sub-scopes will be selected)
5. Set expiration (recommended: 90 days or less)
6. Click "Generate token"
7. Copy the token immediately (you won't see it again)

**Using the token:**

```bash
# Set as environment variable
export GITHUB_TOKEN="ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# Or in config file
# .safesql/config.yaml
github:
  token: ${GITHUB_TOKEN}
  repository: "your-org/your-repo"
```

**Security Best Practices:**
- Never commit tokens to git
- Use environment variables or secret managers
- Rotate tokens regularly
- Use fine-grained tokens when possible (GitHub's newer token type)
- Limit token scope to only what's needed

### Commit Signing (Verified Commits)

SafeSQL creates **commit suggestions** in PR reviews. **You click "Commit suggestion"** to commit with your GPG key:

- ✅ One-click commit via GitHub's "Commit suggestion" button
- ✅ Automatically signed with **YOUR GPG key**
- ✅ No need to configure GPG for the SafeSQL token
- ✅ No manual copy-paste required

**Workflow:**

1. Run `safesql plan --pr <number>`
2. SafeSQL checks if a plan already exists:
   - **If plan exists and is valid (< 1 hour old) and SQL hasn't changed**: Reuses existing plan, no action needed
   - **If plan exists but expired or SQL changed**: Creates commit suggestion to UPDATE it
   - **If no plan exists**: Creates commit suggestion to ADD it
3. Go to the PR and find the review comment with commit suggestion
4. Click **"Commit suggestion"** button in the GitHub UI
5. Your commit is automatically signed with your GPG key
6. Get PR reviewed and approved (need 2+ approvals)
7. Run `safesql apply --pr <number>` (PR must still be open)
8. If needed, run `safesql rollback --pr <number>` (rollback SQL is already in plan file)

**Committing via GitHub UI:**

When you click "Commit suggestion" or commit via GitHub's web interface, it automatically signs with your GPG key (if configured) or GitHub's web-flow key.

### Error Handling

**Plan expired (> 1 hour old):** You'll get clear instructions. Run `safesql plan --pr <number>` again to regenerate.

**Multiple SQL files in PR:** Validation will fail. Each PR should contain exactly ONE SQL file.

**SQL validation fails:** The plan will not be committed to the PR. Fix the SQL and try again.

**Insufficient approvals:** Apply will fail if PR has fewer than 2 approvals. Get more reviews.

**PR already merged:** Apply will fail. PR must be open when applying.

**Rollback SQL generation:** Rollback SQL is generated during plan phase and stored in the plan file. No post-apply updates needed.

## Direct Execution (Development/Testing)

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
    safesql rollback --pr 42
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
# Rollback from PR (recommended - reads rollback SQL from plan file)
safesql rollback --pr PR_NUMBER

# Legacy: Rollback from audit ID (deprecated)
safesql rollback --audit AUDIT_ID --operator NAME
```

**Options:**
- `--pr`: PR number to rollback (reads rollback SQL from plan file in PR)
- `--audit`: Audit ID to rollback (legacy, deprecated)
- `--dry-run`: Show SQL without executing (audit-based only)
- `--operator`: Operator performing rollback (audit-based only)

**Note:** PR-based rollback is recommended as it uses the rollback SQL generated during plan phase.

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

## Migration to GCS Storage

**Important:** As of v1.0.1, SafeSQL no longer stores plans and audit logs locally. All artifacts are stored in GCS.

### What Changed

- **Plans**: Previously stored in `~/.safesql/plans/`, now stored in `gs://${SAFESQL_GCS_BUCKET}/plans/`
- **Audit logs**: Previously stored in `~/.safesql/audit/`, now stored in `gs://${SAFESQL_GCS_BUCKET}/audit/`
- **PR plans**: Now stored in `gs://${SAFESQL_GCS_BUCKET}/pr-plans/{pr-number}/`

### Migration Steps

**1. Set up GCS bucket** (see Prerequisites section above)

**2. Set environment variable:**

```bash
export SAFESQL_GCS_BUCKET="safesql-<your-project>-<environment>"
```

**Note:** The `~/.safesql/config.yaml` file is still used for local configuration.

### Troubleshooting GCS Storage

**Error: "GCS bucket is required"**
- Cause: `SAFESQL_GCS_BUCKET` not set
- Fix: `export SAFESQL_GCS_BUCKET="your-bucket-name"`

**Error: "failed to create GCS client"**
- Cause: No GCP credentials or insufficient permissions
- Fix (jump host): `gcloud auth application-default login`
- Fix (Kubernetes): Ensure Workload Identity and IAM roles configured

**Error: "object not found"**
- Cause: Plan doesn't exist in GCS
- Fix: Verify bucket and check: `gsutil ls gs://${SAFESQL_GCS_BUCKET}/plans/`

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