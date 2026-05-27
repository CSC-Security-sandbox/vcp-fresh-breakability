# Cursor AI Toolkit — Skills, Subagents, Rules & Tools

This document covers every AI customization in the VSA Control Plane workspace: what each one does, when it activates, and how to use it.

---

## Table of Contents

- [Concepts Overview](#concepts-overview)
- [1. Rules](#1-rules)
- [2. Skills](#2-skills)
- [3. Subagents](#3-subagents)
- [4. Built-in Tools](#4-built-in-tools)
- [Quick Reference](#quick-reference)

---

## Concepts Overview

Cursor's AI customization system has four layers:

| Layer | What It Is | Where It Lives | When It Applies |
|-------|-----------|----------------|-----------------|
| **Rules** | Persistent instructions injected into every (or file-specific) conversation | `.cursor/rules/*.mdc` | Always, or when matching files are open |
| **Skills** | Task-specific how-to guides the agent reads on demand | `.cursor/skills/<name>/SKILL.md` | When the agent detects a matching task |
| **Subagents** | Specialized agents with custom system prompts, launched via the Task tool | `.cursor/agents/*.md` or `*.mdc` | When the agent delegates a complex task |
| **Tools** | Built-in capabilities the agent can call directly | Built into Cursor | Available in every conversation |

### How They Work Together

1. **Rules** set the baseline — coding standards, project conventions, always-on context.
2. **Skills** teach the agent *how* to do a specific task (convert markdown to PDF, call an API).
3. **Subagents** are autonomous workers the agent spawns for complex, multi-step jobs (code review, writing tests).
4. **Tools** are the primitives the agent uses to read files, run commands, search code, etc.

---

## 1. Rules

Rules are `.mdc` files in `.cursor/rules/` with YAML frontmatter. They inject context into the agent's system prompt.

### 1.1 `.cursorrules` (Always Applied)

The root `.cursorrules` file is always active. It defines:

- Project overview (Go, Temporal, GKE, PostgreSQL, Gin)
- Directory structure and service map
- Code guidelines (error handling, context propagation, nil checks)
- Testing conventions (table-driven, mocks, >80% coverage target)
- Database patterns (migrations, parameterized queries, transactions)
- Temporal workflow rules (determinism, retries, versioning)
- Common `make` commands (`build`, `test`, `generate-mocks`, `docker-build`)
- Environment variable conventions

**You don't need to do anything** — this context is always available to the agent.

### 1.2 Go Coding Standards (`go-coding-standards.mdc`)

| Field | Value |
|-------|-------|
| **Scope** | `**/*.go` (active when Go files are open) |
| **Always apply** | No |

Enforces import organization (stdlib / third-party+local grouping), architectural boundaries (no cloud-specific imports in `core/`), context-as-first-parameter, two-value type assertions, and dependency injection patterns.

Includes detailed **exemption lists** for legacy code — the agent will not flag existing violations in exempted files.

### 1.3 Triagebot (`triagebot.mdc`)

| Field | Value |
|-------|-------|
| **Activation** | Message starts with `triagebot` |
| **Always apply** | Yes (but only activates on keyword) |

A production incident triage assistant. Fetches logs from GCP Cloud Logging, builds correlated timelines, identifies root causes, and produces structured failure reports.

**How to use:**

```
triagebot Id 0289B6F6-F9D2-320A-0000-000000000000 staging us-c1
```

or:

```
triagebot project=netapp-us-c1-staging-sde correlation_id=0289B6F6-...
```

The bot will:
1. Derive the GCP project from location + environment
2. Fetch logs with progressive widening (7d → 30d)
3. Read design docs and workflow documentation
4. Build an error inventory and correlated timeline
5. Output a structured SUCCESS or FAILURE report

### 1.4 NetApp API Assistant (`gcnvapis.mdc`)

| Field | Value |
|-------|-------|
| **Activation** | Say `activate netapp_apis` |
| **Always apply** | No (agent-requestable) |

Provides two modes:
- **GCNV API Assistant** — for Google Cloud NetApp Volumes REST API operations
- **ONTAP Expert Mode Assistant** — for advanced ONTAP features via expert mode passthrough

Reads API specs from `google-proxy/api/gcp-api.yaml` and `clients/ontap-rest/swagger`.

---

## 2. Skills

Skills are task-specific instruction sets in `.cursor/skills/<name>/SKILL.md`. The agent reads them when it detects a matching task.

### 2.1 Google Proxy API Runner (`google-proxy-api`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/skills/google-proxy-api/SKILL.md` |
| **Trigger** | User asks to invoke, call, test, or curl any Google Proxy API endpoint |

Runs REST API calls against the local Google Proxy service at `http://localhost:9000`.

**What it provides:**
- Connection defaults (base URL, headers, correlation ID)
- Complete API catalog (pools, volumes, snapshots, replications, backups, backup vaults, backup policies, active directories, KMS configs, host groups, VPGs, quota rules, operations)
- Request body templates for every create operation
- Automatic async operation polling after mutating calls

**How to use:**

```
Create a pool on the local google proxy
```

```
List all volumes for project 123456789 in australia-southeast1-a
```

The agent will construct the `curl` command, execute it, and if it's a mutating operation, automatically poll the returned operation until completion.

### 2.2 Local Config Generator (`local-config`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/skills/local-config/SKILL.md` |
| **Trigger** | User asks to create, generate, or set up their local env file or local config |

Generates a safe `env.development.local` file from the example template with interactive configuration choices.

**What it provides:**
- Guided setup with mode selection (minimal vs full)
- Database configuration (local Docker, Skaffold, or remote Cloud SQL)
- Region selection
- Safe default passwords for local development
- Mock service URLs pre-configured
- Strict security: never displays sensitive values in chat output

**How to use:**

```
Generate my local config file
```

```
Set up env.development.local
```

### 2.3 Markdown to PDF Converter (`md-to-pdf`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/skills/md-to-pdf/SKILL.md` |
| **Trigger** | User asks to convert markdown to PDF, generate a PDF, or export documentation |

Converts Markdown files to PDF using `scripts/md-to-pdf.sh` with Mermaid diagram rendering support.

**How to use:**

```
Convert doc/architecture/designs/0022-vcp-oci-integration-hld.md to PDF
```

```
Generate a PDF from @doc/api/resources/pools.md with table of contents
```

**Options:** `--toc` (table of contents), `--margin <size>`, `--font-size <pt>`

**Prerequisites:** `pandoc`, `basictex` (XeLaTeX). The agent will check and guide installation.

### 2.4 Create Rule (`create-rule`) — Personal Skill

| Field | Value |
|-------|-------|
| **Location** | `~/.cursor/skills-cursor/create-rule/SKILL.md` |
| **Trigger** | User wants to create a Cursor rule, add coding standards, or set up project conventions |

Guides through creating `.mdc` rule files with proper frontmatter (`description`, `globs`, `alwaysApply`).

**How to use:**

```
Create a rule for Python coding standards that applies to all .py files
```

### 2.5 Create Skill (`create-skill`) — Personal Skill

| Field | Value |
|-------|-------|
| **Location** | `~/.cursor/skills-cursor/create-skill/SKILL.md` |
| **Trigger** | User wants to create a new Agent Skill |

Guides through creating `SKILL.md` files with proper structure, descriptions, and best practices (progressive disclosure, concise instructions, under 500 lines).

**How to use:**

```
Create a skill for reviewing database migrations
```

### 2.6 Create Subagent (`create-subagent`) — Personal Skill

| Field | Value |
|-------|-------|
| **Location** | `~/.cursor/skills-cursor/create-subagent/SKILL.md` |
| **Trigger** | User wants to create a custom subagent |

Guides through creating custom subagent `.md` files in `.cursor/agents/` (project) or `~/.cursor/agents/` (personal).

**How to use:**

```
Create a subagent for database query optimization
```

### 2.7 Update Cursor Settings (`update-cursor-settings`) — Personal Skill

| Field | Value |
|-------|-------|
| **Location** | `~/.cursor/skills-cursor/update-cursor-settings/SKILL.md` |
| **Trigger** | User wants to change editor settings, themes, font size, etc. |

Modifies `settings.json` for Cursor/VSCode preferences.

**How to use:**

```
Make the font bigger
```

```
Enable format on save
```

### 2.8 Migrate to Skills (`migrate-to-skills`) — Personal Skill

| Field | Value |
|-------|-------|
| **Location** | `~/.cursor/skills-cursor/migrate-to-skills/SKILL.md` |
| **Trigger** | User wants to convert rules or commands to the skills format |

Converts "Applied intelligently" rules (`.mdc` without `globs` or `alwaysApply`) and slash commands to the Agent Skills format.

---

## 3. Subagents

Subagents are specialized AI assistants launched via the Task tool. Each has a custom system prompt defining its behavior.

### 3.1 Code Reviewer (`code-reviewer`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/agents/code-reviewer.mdc` |
| **Activation** | Message starts with `review` (e.g., `review PR #123`, `review my changes`) |
| **Mode** | Read-only (will not modify code) |

An **orchestrator** that launches parallel subagents to review code against the project's 16-category review checklist.

**What it does:**
1. Resolves the branch/PR to review
2. Gathers changed Go files, commit log, and diff stats
3. Reads review standards and Go coding exemptions
4. Partitions files and launches parallel review subagents (up to 4)
5. Merges findings into a severity-ranked report
6. Writes the report to `review/<ticket>-<date>.md`

**Review categories:** Correctness & Safety, Error Handling, Architectural Boundaries, Import Organization, Context Handling, Temporal Workflows, Dependency Injection, Database & Migrations, API Design, Security, Performance, Testing, Configuration, Logging & Observability, Code Style, Duplication.

**Severity levels:** CRITICAL (must fix), HIGH (should fix), MEDIUM (recommended), LOW (optional), NIT (author's discretion).

**How to use:**

```
review PR #42
```

```
review branch VSCP-1234
```

```
review my changes
```

### 3.2 Design Document Writer (`design-doc`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/agents/design-doc.mdc` |
| **Activation** | `design doc`, `HLD`, `ADR`, `architecture doc` |

Writes new design documents following the project's established structure and naming (`NNNN-descriptive-slug.md` in `doc/architecture/designs/`).

**What it does:**
1. Checks existing designs for contradictions
2. Determines the next document number
3. Selects the appropriate style (ADR or Design Document)
4. Writes the document with Mermaid diagrams, proper sections, and cross-references
5. Runs a validation checklist

**Two styles:**
- **ADR** (Architecture Decision Record): For recording a specific decision with alternatives considered
- **Design Document**: For HLDs, feature designs, or reference documents

**How to use:**

```
Write a design doc for SVM lifecycle management
```

```
Create an ADR for the new caching strategy
```

### 3.3 Diagramming Assistant (`diagramming`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/agents/diagramming.mdc` |
| **Activation** | `diagram`, `draw`, `visualize`, `mermaid`, `sequence diagram`, `flowchart` |

Generates Mermaid diagrams from source code analysis or natural language descriptions.

**Supported diagram types:**

| Type | Syntax | Use Case |
|------|--------|----------|
| Sequence | `sequenceDiagram` | Workflow execution flows, API request flows |
| Architecture | `graph TB/LR` | Component topology, system architecture |
| Flowchart | `flowchart TD` | Decision logic, error handling strategies |
| State Machine | `stateDiagram-v2` | Resource lifecycle (pool, volume, backup) |
| ER Diagram | `erDiagram` | Data model relationships from GORM structs |
| Class | `classDiagram` | Domain model structure, interface hierarchies |
| C4 Context | `C4Context` | System-level architecture |

**How to use:**

```
Diagram the create pool workflow
```

```
Visualize the volume lifecycle states
```

```
Draw the ER diagram for the backup data model
```

### 3.4 Error Manager (`error-manager`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/agents/error-manager.mdc` |
| **Activation** | `add error`, `new error code`, `error code`, `create error` |

Adds new error codes to the custom error framework (`lib/errors/errors.go` + `lib/errors/errors.json`) with correct category ranges and validation.

**Error category ranges:**

| Range | Category |
|-------|----------|
| 1000-1999 | Workflow / Orchestration / Validation |
| 2000-2999 | Database / Persistence |
| 3000-3999 | GCP / Cloud Provider |
| 4000-4999 | VSA Cluster Lifecycle |
| 5000-5999 | ONTAP / Data Plane |
| 6000-6999 | Validation / CMEK / Replication |
| 7000-7999 | Snapshot / Volume operations |
| 8000-8999 | Security / KMS / Credentials |
| 9000-9999 | VLM-specific GCP |
| 10000-14999 | FlexCache, Peering, Backup, AD |

**How to use:**

```
Add a new error code for ONTAP volume resize exceeding max size
```

```
Create error codes for backup vault quota exceeded (retriable: no, HTTP 409)
```

### 3.5 Local Environment (`local-env`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/agents/local-env.mdc` |
| **Activation** | `local env`, `setup dev`, `start local`, `skaffold` |

Brings up and manages the local development environment.

**Two modes:**
- **Skaffold** (full K8s): Docker Desktop Kubernetes + Skaffold with all services, Temporal, and Postgres
- **Services-only**: Individual services via `go run` or `air` against local/remote infrastructure

**What it does:**
1. Checks prerequisites (docker, kubectl, helm, skaffold, go)
2. Verifies Docker Desktop Kubernetes is enabled
3. Sets environment variables (GHVSA_PAT, DB_PASSWORD, etc.)
4. Builds binaries and starts Skaffold (or individual services)
5. Verifies service health
6. Provides troubleshooting for common issues

**How to use:**

```
Bring up local environment
```

```
Start local dev with skaffold
```

### 3.6 Migration Helper (`migration-helper`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/agents/migration-helper.mdc` |
| **Activation** | `migrate`, `migration`, `add column`, `add table`, `schema change` |

Creates correctly structured SQL migration files following the project's three-phase migration system (pre / GORM AutoMigrate / post).

**What it does:**
1. Determines target database (vcp or metrics) and migration phase (pre or post)
2. Finds the next sequential migration number
3. Generates both `.up.sql` and `.down.sql` files with idempotent SQL
4. Validates the files

**How to use:**

```
Create a migration to add an encryption_key column to volumes
```

```
Add a post-migration to create an index on jobs.correlation_id
```

### 3.7 Unit Test Writer (`unit-test`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/agents/unit-test.mdc` |
| **Activation** | `test`, `unit test`, `write test`, `add test`, `coverage` |

Generates Go tests following the project's established patterns.

**Test types supported:**

| Source Type | Pattern |
|------------|---------|
| Temporal workflow | Suite-based with `TestWorkflowEnvironment` |
| Temporal activity | `TestActivityEnvironment` or direct call |
| Orchestrator factory | In-memory SQLite + mock Temporal client |
| API endpoint | HTTP test recorder + mock orchestrator |
| Utility/helper | Table-driven tests |
| Database layer | In-memory SQLite with real GORM operations |

**Coverage checklist per function:**
- Happy path
- Error from each dependency
- Edge cases (nil, empty, zero, boundary)
- State validation
- Error type checks
- Retriable vs non-retriable errors

**How to use:**

```
Write tests for the CreatePool activity
```

```
Add unit tests for volume_update_workflow.go
```

### 3.8 Workflow Builder (`workflow-builder`)

| Field | Value |
|-------|-------|
| **Location** | `.cursor/agents/workflow-builder.mdc` |
| **Activation** | `workflow`, `create workflow`, `new activity`, `temporal workflow` |

Scaffolds new Temporal workflows and activities following the project's established patterns.

**What it generates:**
1. Workflow struct (embedding `BaseWorkflow`, `WorkflowInterface` compliance)
2. Entry point function (Setup → EnsureJobState → PROCESSING → Run → DONE/ERROR)
3. Activity structs with heartbeats and proper error wrapping
4. Cancellation support (optional)
5. Rollback management (optional)
6. Worker registration in `worker/main.go`
7. Workflow documentation in `doc/workflows/`

**How to use:**

```
Create a workflow for volume encryption rotation
```

```
Scaffold a new delete backup workflow with cancellation support
```

---

## 4. Built-in Tools

These are the core tools available to the agent in every conversation.

### 4.1 File Operations

| Tool | Purpose | Example Use |
|------|---------|-------------|
| **Read** | Read file contents (supports images, PDFs) | Reading source code before editing |
| **Write** | Create or overwrite a file | Creating new source files |
| **StrReplace** | Exact string replacement in files | Editing existing code |
| **Delete** | Delete a file | Removing obsolete files |
| **Glob** | Find files by name pattern | `*.go`, `**/test_*.ts` |
| **Grep** | Search file contents with regex | Finding function definitions, imports |
| **SemanticSearch** | Find code by meaning, not exact text | "How does user authentication work?" |

### 4.2 Execution

| Tool | Purpose | Example Use |
|------|---------|-------------|
| **Shell** | Execute terminal commands | `go test`, `make build`, `git status` |

Key Shell features:
- Commands that don't finish in 30s move to background automatically
- Use `block_until_ms: 0` for long-running processes (dev servers, watchers)
- Background commands stream output to terminal files for monitoring

### 4.3 Collaboration

| Tool | Purpose | Example Use |
|------|---------|-------------|
| **Task** | Launch subagents for complex tasks | Code review, test writing, exploration |
| **AskQuestion** | Structured multiple-choice questions | Gathering requirements |
| **SwitchMode** | Change interaction mode | Switch to Plan mode for large tasks |
| **TodoWrite** | Track multi-step task progress | Managing complex implementations |

### 4.4 Web & Browser

| Tool | Purpose | Example Use |
|------|---------|-------------|
| **WebSearch** | Search the web for current information | Library docs, best practices |
| **WebFetch** | Fetch and read webpage content | Reading documentation pages |

### 4.5 Code Quality

| Tool | Purpose | Example Use |
|------|---------|-------------|
| **ReadLints** | Check linter errors in edited files | Verifying edits don't introduce errors |
| **EditNotebook** | Edit Jupyter notebook cells | Modifying notebook content |

### 4.6 Task Tool — Subagent Types

The Task tool can launch different types of subagents:

| Subagent Type | Purpose | When to Use |
|---------------|---------|-------------|
| `generalPurpose` | Multi-step research and code tasks | Complex searches, multi-file analysis |
| `explore` | Fast codebase exploration | Finding files, searching keywords, understanding structure |
| `shell` | Command execution | Git operations, builds, deployments |
| `browser-use` | Browser automation and testing | Testing web UIs, form filling |
| `code-reviewer-standards` | Review against standards | Automated code quality checks |
| `code-reviewer` | Comprehensive code review | PR reviews with findings report |
| `design-doc` | Write design documents | Architecture docs, ADRs |
| `diagramming` | Generate Mermaid diagrams | Workflow, architecture, ER diagrams |
| `error-manager` | Manage error codes | Adding new error codes to the framework |
| `local-env` | Local environment setup | Bringing up dev environment |
| `migration-helper` | Database migrations | Creating SQL migration files |
| `unit-test` | Unit test generation | Writing Go tests |
| `workflow-builder` | Temporal workflow scaffolding | New workflows and activities |

---

## Quick Reference

### By Task — What to Say

| I want to... | Say this |
|-------------|---------|
| Review a PR | `review PR #42` or `review my changes` |
| Triage a production issue | `triagebot Id <correlation-id> staging us-c1` |
| Write a design document | `Write a design doc for <topic>` |
| Create a diagram | `Diagram the <workflow/model/architecture>` |
| Add an error code | `Add error code for <failure scenario>` |
| Generate local config | `Generate my local config file` or `Set up env.development.local` |
| Set up local dev | `Bring up local environment` |
| Create a DB migration | `Create a migration to add <column> to <table>` |
| Write unit tests | `Write tests for <function/file>` |
| Scaffold a workflow | `Create a workflow for <operation>` |
| Call a local API | `Create a pool on the local google proxy` |
| Convert MD to PDF | `Convert <file.md> to PDF` |
| Use NetApp APIs | `activate netapp_apis` |

### File Locations

```
.cursor/
├── rules/
│   ├── triagebot.mdc          # Production triage bot
│   ├── go-coding-standards.mdc # Go standards (applies to *.go)
│   └── gcnvapis.mdc           # NetApp API assistant
├── skills/
│   ├── google-proxy-api/      # REST API runner
│   │   └── SKILL.md
│   ├── local-config/          # Local env file generator
│   │   └── SKILL.md
│   └── md-to-pdf/             # PDF converter
│       └── SKILL.md
└── agents/
    ├── code-reviewer.mdc      # Code review orchestrator
    ├── code-reviewer-standards.md  # Review checklist (16 categories)
    ├── design-doc.mdc         # Design document writer
    ├── diagramming.mdc        # Mermaid diagram generator
    ├── error-manager.mdc      # Error framework manager
    ├── local-env.mdc          # Local environment setup
    ├── migration-helper.mdc   # SQL migration creator
    ├── unit-test.mdc          # Unit test generator
    └── workflow-builder.mdc   # Temporal workflow scaffolder

~/.cursor/skills-cursor/       # Personal skills (all projects)
├── create-rule/SKILL.md       # Create Cursor rules
├── create-skill/SKILL.md      # Create Agent Skills
├── create-subagent/SKILL.md   # Create custom subagents
├── migrate-to-skills/SKILL.md # Migrate rules → skills
└── update-cursor-settings/SKILL.md  # Edit Cursor settings
```

### Creating Your Own

| I want to create... | How |
|---------------------|-----|
| A new rule | Say `Create a rule for <topic>` (uses create-rule skill) |
| A new skill | Say `Create a skill for <task>` (uses create-skill skill) |
| A new subagent | Say `Create a subagent for <specialty>` (uses create-subagent skill) |
