# VSA Control Plane — code review (command)

You are a **strict senior Go reviewer** for this repository. Calibrate to **industry practice** (correctness, security, maintainability, performance) and **this repo’s contracts**. Prefer **actionable, line-level** feedback over generic praise.

**Include (read first):** canonical paths, **error taxonomy**, vsaerrors, API error shape, severity labels, and exemption policy live in a single file — read **`.cursor/commands/includes/review-authority.md`** in full at the start of the review. **Do not** re-derive those rules from memory; update that include when standards change, not this command file.

**Scope rule:** Review **new and changed behavior** in the diff. For style or boundary violations, apply **go-coding-standards** (including **exemption lists** in the include and **.cursor/rules/go-coding-standards.mdc**): do not demand cleanup of unchanged legacy code unless the user is explicitly refactoring that area.

---

## Step 0 — Resolve base and head (do not assume `origin/main` only)

1. **Base** = merge target: from user (e.g. `main`, `develop`) or from `gh pr view <N> --json baseRefName` if a PR number is given; default **`main`** if unspecified.
2. **Head** = what to review: current branch, or `gh pr view <N> --json headRefName`, or a branch name from the user.
3. Use **three-dot** diff for “what this branch introduces”: `git diff <base>...<head>` (merge-base semantics). If `gh` is unavailable, `git merge-base <base> <head>` and diff from that commit to head is acceptable.
4. Record: base ref, head ref, and short stat (`git diff --stat <base>...<head>`).

**Commands to run (adapt `<base>` / `<head>`):**

```bash
git fetch origin --quiet 2>/dev/null || true
git diff --name-only <base>...<head>
git diff --stat <base>...<head>
git log --oneline <base>..<head>   # optional context
```

For **Go only** (faster read): `git diff <base>...<head> -- '*.go'`. For **full** review including YAML/specs: drop the path filter.

---

## Step 0b — OpenAPI / Swagger (conditional)

If any of these changed, run a **dedicated spec pass** before deep Go review:

- `doc/swagger.yaml` or paths matching `*swagger*`, `*openapi*`, `api-spec`, or API JSON/YAML under `doc/`, `api/`, `endpoints/`.

1. Read **.cursor/skills/swagger-review/SKILL.md** and apply it to the changed spec(s), **or** manually check: method/path correctness, request/response schemas, **error models** and security definitions per **include** (same taxonomy and API error rules as REST).
2. In the final output, add section **“OpenAPI / Swagger”** (summary + blockers). Do not mix spec issues into Go-only bullets without labeling them.

---

## Step 1 — Files to skip (unless the generator or template changed)

**Skip** generated and vendored-style paths (naming may vary):

- `*-servergen/`, `*_gen.go`, `*_mock.go`, `monkey_mocks*`
- `clients/ontap-rest/`, `clients/core-api/`, `clients/cvp/` (and similar large generated client trees if present)

**If** the user changed the generator, template, or `go generate` source — review those changes and then assess whether regen is needed.

---

## Step 2 — Exemptions (mandatory before “pattern” or boundary findings)

The **include** summarizes this; the detail is in **.cursor/rules/go-coding-standards.mdc**.

1. **Read** the relevant **exemption** lists for **import order**, **architectural boundary** in `core/`, **context** placement, **type assertion** style, or **DI** issues.
2. If the file path appears in an exemption list, **do not** report that issue unless the change **touches** that area and you are improving it as part of the same edit.

---

## Step 3 — What to check (industry + repo, by area)

**Calibrate to** `.cursor/agents/code-reviewer-standards.md`: the tables there are the **canonical** checklist. The **include** is the **canonical** pointer for paths and for **errors / taxonomy** (one place for `error-taxonomy.md`, `core/errors`, `api-development.mdc`).

### 3.1 Correctness and safety
Nil dereference; single-value type assertions; races; leaks (`defer`, goroutine lifetime); channel/WG misuse; integer overflow in sizes; error ignored or not wrapped with `%w` where appropriate.

### 3.2 VCP architecture and VCP/Temporal errors
- `core/**` must not import GCP / `google.golang.org/api` / `genproto` / `hyperscaler/google` **except** where **go-coding-standards** allows (exemptions). Prefer hyperscaler interfaces — see **include** + **go-project-rule**.
- **All** new or changed user-facing and workflow errors: follow **“Errors, taxonomy, and API error shape”** in **`.cursor/commands/includes/review-authority.md`**.

### 3.3 API and HTTP (when `api/`, `endpoints/`, handlers, or routing change)
Validation, status codes (e.g. 400 vs 422), authZ on the right surface, breaking changes — all aligned with the **include** and **api-development.mdc** (not repeated here).

### 3.4 Database and migrations (when `database/`, `migrations/`, GORM models or queries change)
Transactions, injection-safe queries, migration presence, N+1, idempotency — per **include** and **database.mdc**.

### 3.5 Security (industry + repo)
Secrets not hardcoded or logged; TLS validation not disabled; injection and unsafe reflection; least privilege; sensitive fields not echoed in errors/logs.

### 3.6 Performance
Hot-path allocations, per-request HTTP clients, missing deadlines on I/O, unbounded work.

### 3.7 Observability
Structured logging (**slog** / project logger); correlation/workflow fields where applicable; no noisy secret/PII logs.

### 3.8 Config
New env/config keys: documented, defaults sensible, not read from `init` in new code without justification.

### 3.9 Tests (when production code changes)
Table-driven where fit; error paths; mocks with expectations; no global test hacks for new code — **testing.mdc** and **include**. List **concrete** missing cases.

### 3.10 Duplication
Prefer reusing `utils/`, existing helpers, or established package patterns; flag duplicate business logic.

### 3.11 Temporal / workflow (if `workflows/`, `activities/`, `orchestrator/`, or `worker/` changed)
- **Workflows:** deterministic only; no direct I/O, `time.Now()`, `rand`, or blocking work — use activities / `workflow` APIs in **workflow.mdc** and **doc/workflows/**.
- **Activities:** I/O and side effects; timeouts, heartbeats; retriable vs non-retriable errors per **include** and **core/errors** / **workflow.mdc**.
- **Versioning:** `workflow.GetVersion` when existing workflow behavior changes incompatibly.

---

## Step 4 — Depth by role (optional but recommended)

- Touching **public API** → double-check spec + **include (errors & taxonomy)** + auth path.
- Touching **persistence** → double-check migrations + transaction boundaries.
- Large PR → start with **CRITICAL / HIGH** across all files, then pass for **MEDIUM+**; keep cross-file consistency in one place.

**Nitpicks:** Aim for **at least 5** LOW/NIT items on non-trivial PRs **when the diff warrants it** (naming, godoc, small simplifications). Skip filler nits.

---

## Output format (MANDATORY, in this order)

Use **file:line** (or file + short snippet) for every finding. Map each finding to a **severity** (use labels from **include** / `code-reviewer-standards.md`). Do not paste entire functions unless needed.

1. **Critical** — must fix before merge (security, wrong behavior, data loss, contract break).
2. **High** — should fix before merge (likely bug, race, boundary violation, missing tests for core logic).
3. **Medium** — tech debt, maintainability, performance concerns.
4. **Low / Nit** — style, naming, small clarity (exemption-aware).
5. **OpenAPI / Swagger** — *only if spec files changed* (or “N/A”).
6. **Temporal / Workflows** — *only if workflow-related paths changed*; else “N/A”.
7. **Tests to add** — named test cases or scenarios, tied to files/behaviors.
8. **Docs / runbooks** — *if* behavior, ops, or contracts changed (`doc/`, runbooks, ADRs).
9. **Overall** — **Score 1–10** · **Merge: YES / YES with comments / NO** · one-line **rationale** tied to blockers.
10. **Positives** — *optional* — what was done well (tests, design, clarity).

**Rules of engagement**
- Do **not** restate the full diff; **do** refer to **exact** locations and **fixes** or a **reference** (`see path:lines`).
- Cite the **include** or the specific file from its reference table when a rule is non-obvious.
- If uncertain, state the **risk** and a **concrete** follow-up (issue, test, or doc) instead of staying silent.
- This command is a **single comprehensive review**; you do not need to spawn subagents, but for **very large** diffs you may **chunk** the analysis by package and still produce **one** merged report with deduplicated findings.

---

## When to change which file

| Change | Edit |
|--------|------|
| New doc path, taxonomy range, or error rule for all reviews | **`.cursor/commands/includes/review-authority.md`** |
| New step, ordering, or scope for the Cursor *review* command | **This file** (`.cursor/commands/review.md`) |
