---
name: local-config
description: Generate the env.development.local configuration file for local development. Use when the user asks to create, generate, set up, or configure their local env file, environment file, or local config.
---

# Local Config Generator

Generate a safe `env.development.local` file for local development from the example template.

## Security Rules

- **NEVER** write real passwords, tokens, API keys, or secrets into the generated file.
- **NEVER** read, echo, print, or log the contents of an existing `env.development.local` file to chat.
- **NEVER** embed GCP project IDs, service account credentials, or endpoint URLs that the user provides into your chat responses — write them directly to the file only.
- Use placeholder values (e.g., `CHANGE_ME`, `your-project-id`) for any sensitive field the user hasn't provided.
- If the user provides sensitive values, write them to the file silently — do not repeat them back in chat.

## Sensitive Fields

These fields contain secrets or credentials and must never be shown in chat output:

| Field | Category |
|-------|----------|
| `DB_PASSWORD` | Secret |
| `DB_ADMIN_PASSWORD` | Secret |
| `METRICS_DB_PASSWORD` | Secret |
| `VSA_NODE_PASSWORD` | Secret |
| `GCP_PROJECT_ID` | Infrastructure |
| `SECRET_MANAGER_PROJECT_ID` | Infrastructure |
| `CA_POOL_DEPLOYED_PROJECT_ID` | Infrastructure |
| `GCE_METADATA_HOST` | Infrastructure |
| `GCP_SERVICE_NETWORKING_ENDPOINT_URL` | Infrastructure |
| `GCP_CONSUMER_MGMT_ENDPOINT_URL` | Infrastructure |
| `TEMPORAL_CLIENT_CERT_PATH` | Secret |
| `TEMPORAL_CLIENT_KEY_PATH` | Secret |

## Workflow

### Step 1 — Check for existing file

```bash
test -f env.development.local && echo "EXISTS" || echo "MISSING"
```

- If **EXISTS**: Ask the user whether to overwrite or skip. Do NOT read or display its contents.
- If **MISSING**: Proceed to Step 2.

### Step 2 — Read the example template

Read `env.development.example` to get the current list of variables and default values. This is the source of truth for variable names and structure.

### Step 3 — Gather user inputs

Use the AskQuestion tool to collect configuration choices. Ask only what's needed to produce a working config — don't ask about every field.

**Question 1: Setup mode**

| Option | Description |
|--------|-------------|
| Minimal | Core services only (core-api, google-proxy, worker). Disables metrics, billing, and most feature flags. Best for feature development. |
| Full | All services including metrics and billing. Good for end-to-end testing. |

**Question 2: Database setup**

| Option | Description |
|--------|-------------|
| Local Docker Postgres | `localhost:5432`, user `postgres` |
| Skaffold Postgres | `localhost:5433`, user `postgres` |
| Remote Cloud SQL | User must provide host, port, user |

**Question 3: Region**

| Option | Description |
|--------|-------------|
| `australia-southeast1` | Default dev region |
| `us-central1` | US region |
| `us-east4` | US East region |
| Other | User specifies |

### Step 4 — Generate the file

Build `env.development.local` using the template from `env.development.example` with these adjustments based on user answers:

#### Always set (regardless of mode)

```
ENV=local
ENVIRONMENT=gcp
RUN_MIGRATION_ON_START=true
CLOUD_SQL_IAM_AUTH_ENABLED=false
MSI_ENABLED=false
```

#### Passwords — use safe local-dev defaults

For local development, use `localdev` as the default password for all database fields. The user can change them later. Do NOT use production-strength passwords — this is local-only.

```
DB_PASSWORD=localdev
DB_ADMIN_PASSWORD=localdev
METRICS_DB_PASSWORD=localdev
```

For VSA node password, use `CHANGE_ME` as placeholder:

```
VSA_NODE_PASSWORD=CHANGE_ME
```

#### Minimal mode adjustments

```
METRICS_ENABLED=false
ENABLE_BACKGROUND_TASK=false
ENABLE_VOLUME_METRICS=false
ENABLE_BACKUP_METRICS=false
ENABLE_BACKUP_BILLING_METRICS=false
ENABLE_SFR_METRICS=false
ENABLE_FILES_BACKUP_BILLING=false
ENABLE_CMEK_BACKUP_BILLING=false
ENABLE_CROSS_REGION_BACKUP_BILLING_METRICS=false
ENABLE_AUTO_TIERING_BILLING_METRICS=false
ENABLE_FILES_AUTO_TIERING_BILLING=false
ENABLE_REPLICATION_BILLING_METRICS=false
ENABLE_BIDIRECTIONAL_REPLICATION_BILLING_METRICS=false
ENABLE_FILES_REPLICATION_BILLING_METRICS=false
ENABLE_LARGE_VOLUMES_BILLING=false
ENABLE_BATCH_USAGE_UPDATES=false
```

#### Full mode adjustments

```
METRICS_ENABLED=true
ENABLE_BACKGROUND_TASK=true
ENABLE_VOLUME_METRICS=true
ENABLE_BACKUP_BILLING_METRICS=true
ENABLE_SFR_METRICS=true
ENABLE_FILES_BACKUP_BILLING=true
ENABLE_CMEK_BACKUP_BILLING=true
ENABLE_CROSS_REGION_BACKUP_BILLING_METRICS=true
ENABLE_AUTO_TIERING_BILLING_METRICS=true
ENABLE_FILES_AUTO_TIERING_BILLING=true
ENABLE_REPLICATION_BILLING_METRICS=true
ENABLE_BIDIRECTIONAL_REPLICATION_BILLING_METRICS=true
ENABLE_FILES_REPLICATION_BILLING_METRICS=true
ENABLE_LARGE_VOLUMES_BILLING=true
ENABLE_BATCH_USAGE_UPDATES=true
```

#### Mock service URLs for local development

Point billing/monitoring to mock server by default:

```
ROOT_URL=http://localhost:8080
PERFORMANCE_ROOT_URL=http://localhost:8080
USAGE_ROOT_URL=http://localhost:8080
MONITORING_API_BASE_URL=http://localhost:10000
GCE_METADATA_HOST=localhost:8080
```

#### GCP project and region

Use the region from user's answer. For GCP project fields, use `your-project-id` as placeholder:

```
GCP_PROJECT_ID=your-project-id
GCP_REGION=<selected-region>
LOCAL_REGION=<selected-region>
GOOGLE_REGION=<selected-region>
```

### Step 5 — Write the file

Write the file using the Write tool. Preserve the section comment structure from the example template.

### Step 6 — Report result

Tell the user:
1. The file was created at `env.development.local`
2. Which fields need manual updates (list field names only, not values):
   - `GCP_PROJECT_ID` — their GCP project number
   - `VSA_NODE_PASSWORD` — if they need ONTAP access
   - Database passwords — if they want something other than the default
3. Remind them this file is git-ignored and safe to store local credentials in
4. Do NOT display the file contents or any sensitive values in chat

## Error Handling

| Error | Action |
|-------|--------|
| `env.development.example` missing | Tell the user the example template is missing and cannot generate the config |
| User declines overwrite | Exit gracefully, no changes |
| User provides Cloud SQL details | Write them directly to file, do not echo back |
