# Shared: review command — authority, references, errors (single source of truth)

Other Cursor command docs and steps should **link here** instead of re-listing paths. Update **this file** when error-taxonomy, rules, or key docs move.

## Reference map

| Topic | Source |
|-------|--------|
| Imports, boundaries, context, general errors, testing | **CODING_GUIDELINES.md** |
| Core vs hyperscaler, import / context / DI / boundary exemptions | **.cursor/rules/go-coding-standards.mdc** (exemption `<details>` blocks) |
| Go project defaults | **.cursor/rules/go-project-rule.mdc** |
| GORM, migrations, DB error band | **.cursor/rules/database.mdc** |
| Table-driven tests, mocks | **.cursor/rules/testing.mdc** |
| Temporal determinism, activities, workflow rules | **.cursor/rules/workflow.mdc**, **doc/workflows/** |
| Full severity grid (16 categories) | **.cursor/agents/code-reviewer-standards.md** |

**Severity labels:** CRITICAL / HIGH / MEDIUM / LOW / NIT — use definitions in `code-reviewer-standards.md` § *Severity Definitions*.

---

## Errors, taxonomy, and API error shape (canonical)

Use this block for **any** review that touches error returns, API handlers, workflows/activities, OpenAPI, or `core/errors`.

| What | Where |
|------|--------|
| **Code ranges** (workflow, DB, GCP, etc.) and **HTTP mapping** | **doc/api/error-taxonomy.md** |
| **NewVCPError**, wrapping, **Temporal** integration | **core/errors/README.md** |
| API **validation**, **envelope** `{ "code", "message" }`, no secrets/PII in `message` | **.cursor/rules/api-development.mdc** |
| Implementation: **vsaerrors** / `core/errors` | `core/errors` package; match taxonomy ranges for the component you are in. |

**Temporal activities:** when surfacing to Temporal, use **`vsaerrors.WrapAsTemporalApplicationError`** (or project conventions in **core/errors** + **workflow.mdc**); mark non-retriable for validation / not-found as required.

**OpenAPI / Swagger:** if spec files change, error models in spec should stay **consistent** with **doc/api/error-taxonomy.md** and **api-development.mdc** (same story as REST handlers).

---

## Optional: Temporal operations

- **doc/guides/temporal-debugging.md** — operational / debugging notes when workflow behavior is in play.

---

## Exemptions (before import / boundary nits)

Always consult **.cursor/rules/go-coding-standards.mdc** for **exemption** lists; do not flag listed legacy paths for the same issue unless the diff refactors that area.
