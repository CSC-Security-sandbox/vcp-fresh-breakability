# Code Review Standards — VSA Control Plane

This is the full checklist applied during code reviews. Each section lists what to check and the expected severity when violated.

---

## 1. Correctness & Safety

| Check | Severity |
|-------|----------|
| Nil pointer dereference — pointer fields dereferenced without nil check | CRITICAL |
| Single-value type assertion (`x.(Type)` without `, ok`) | CRITICAL |
| Dead condition (`&field != nil` always true) | CRITICAL |
| Race condition — shared state accessed without synchronization | CRITICAL |
| Integer overflow / underflow in size calculations | HIGH |
| Missing error return after early-return branch | HIGH |
| Goroutine leak — unbounded goroutines without cancellation | HIGH |
| Channel operations that can deadlock | HIGH |
| Incorrect use of `sync.WaitGroup` (Add after goroutine start) | HIGH |

---

## 2. Error Handling

| Check | Severity |
|-------|----------|
| Error silently ignored (no `_` assignment, no check) | HIGH |
| Error checked but original error not wrapped (`fmt.Errorf` without `%w`) | MEDIUM |
| Error message doesn't add context (just `return err`) | MEDIUM |
| Inconsistent error wrapping (mixed `%v` and `%w` in same package) | LOW |
| Missing `defer cleanup()` after resource acquisition | HIGH |
| Temporal activity error not wrapped with `WrapAsTemporalApplicationError` | HIGH |

### Project-specific error patterns

- Errors in workflows/activities must use `vsaerrors.WrapAsTemporalApplicationError(err)` or the project's error taxonomy (`core/errors/`).
- Check `doc/api/error-taxonomy.md` for error code conventions.

---

## 3. Architectural Boundaries

| Check | Severity |
|-------|----------|
| `cloud.google.com/go` imported in `core/` | HIGH |
| `google.golang.org/api` imported in `core/` | HIGH |
| `hyperscaler/google` imported in `core/` (production code) | HIGH |
| Cloud-specific types in function signatures of `core/` | HIGH |
| Circular dependency between packages | HIGH |
| Business logic in API handler (should be in activity/workflow) | MEDIUM |

**Exemption**: Check `/.cursor/rules/go-coding-standards.mdc` exemption lists before flagging.

---

## 4. Import Organization

| Check | Severity |
|-------|----------|
| Imports not grouped (stdlib / third-party+local) | LOW |
| Duplicate imports (same package imported twice, once aliased) | MEDIUM |
| Unused import | MEDIUM |
| Import alias inconsistent with project conventions | NIT |

**Exemption**: Check exemption list in `go-coding-standards.mdc`.

---

## 5. Context Handling

| Check | Severity |
|-------|----------|
| `context.Context` not first parameter | MEDIUM |
| Context not propagated through call chain (hardcoded `context.Background()` or `context.TODO()` in production code) | HIGH |
| Context cancelled but downstream operations continue | MEDIUM |

**Exemption**: Check exemption list in `go-coding-standards.mdc`.

---

## 6. Temporal Workflows

| Check | Severity |
|-------|----------|
| Non-deterministic operation in workflow code (`time.Now()`, `rand`, `os.Getenv`, HTTP call) | CRITICAL |
| Missing retry policy on activity invocation | HIGH |
| Missing heartbeat in long-running activity | HIGH |
| Workflow timeout not configured or unreasonably large | MEDIUM |
| Workflow versioning not applied when modifying existing workflow | HIGH |
| Activity doing database writes without idempotency guard | HIGH |
| Child workflow started without proper cancellation propagation | MEDIUM |
| Signal/query handler registered after workflow start | MEDIUM |

### Temporal-specific patterns

- Workflows must be deterministic — all I/O and non-deterministic ops go in activities.
- Use `workflow.GetVersion()` when changing existing workflow logic.
- Activities must have `StartToCloseTimeout` and appropriate `HeartbeatTimeout`.
- Check `doc/guides/temporal-debugging.md` and `doc/workflows/` for expected patterns.

---

## 7. Dependency Injection & Testability

| Check | Severity |
|-------|----------|
| New code uses package-level `var` for function variables (monkey-patching) | MEDIUM |
| External dependency accessed directly instead of through interface | MEDIUM |
| Constructor doesn't accept dependencies (not testable) | MEDIUM |
| Test uses monkey-patching instead of DI (new code only) | LOW |

**Note**: Existing code widely uses the monkey-patching pattern. Only flag this for **new** code.

---

## 8. Database & Migrations

| Check | Severity |
|-------|----------|
| Raw SQL string interpolation (SQL injection risk) | CRITICAL |
| Schema change without migration file in `database/` | HIGH |
| Missing transaction for multi-step DB operations | HIGH |
| N+1 query pattern (loop of individual queries) | MEDIUM |
| New column without default or migration for existing rows | MEDIUM |
| Missing index on frequently queried column | LOW |
| Database connection created per-request instead of pooled | HIGH |

---

## 9. API Design

| Check | Severity |
|-------|----------|
| Breaking change to existing API without version bump | CRITICAL |
| Missing input validation at API boundary | HIGH |
| Inconsistent error response format | MEDIUM |
| Missing or wrong HTTP status code | MEDIUM |
| Sensitive data in response without redaction | CRITICAL |
| API endpoint missing authentication/authorization check | CRITICAL |
| Missing rate limiting on public endpoint | LOW |

---

## 10. Security

| Check | Severity |
|-------|----------|
| Hardcoded secret, password, API key, or token | CRITICAL |
| Secret logged or included in error message | CRITICAL |
| Missing input sanitization | HIGH |
| Overly permissive IAM role or service account scope | HIGH |
| TLS/certificate validation disabled | CRITICAL |
| RBAC check missing where required | HIGH |

---

## 11. Performance

| Check | Severity |
|-------|----------|
| Unbounded memory allocation (e.g., `append` in loop without pre-allocation hint) | MEDIUM |
| HTTP client created per-request (should reuse) | MEDIUM |
| Missing caching for repeated expensive operations | LOW |
| Large payload copied unnecessarily (use pointer receiver) | LOW |
| Blocking operation in hot path without timeout | HIGH |
| Missing context deadline on outbound HTTP/gRPC calls | HIGH |

---

## 12. Testing

| Check | Severity |
|-------|----------|
| No tests for new business logic | HIGH |
| Test doesn't assert error conditions | MEDIUM |
| Test uses real external service instead of mock | MEDIUM |
| Table-driven test missing edge cases (empty, nil, boundary) | LOW |
| Test file not alongside source (`_test.go` co-location) | NIT |
| Mock expectations not verified (`AssertExpectations`) | MEDIUM |

---

## 13. Configuration

| Check | Severity |
|-------|----------|
| New environment variable without documentation | MEDIUM |
| New config without default value | MEDIUM |
| Config read at package init instead of structured config object (new code) | LOW |
| Magic number / hardcoded constant that should be configurable | LOW |

---

## 14. Logging & Observability

| Check | Severity |
|-------|----------|
| Sensitive data in log output | CRITICAL |
| Missing structured logging fields (correlation_id, workflow_id) | MEDIUM |
| Log level inappropriate (ERROR for expected conditions, DEBUG for failures) | LOW |
| Missing metrics instrumentation on new critical path | LOW |
| `fmt.Println` / `log.Println` instead of structured logger | MEDIUM |

---

## 15. Code Style & Readability

| Check | Severity |
|-------|----------|
| Exported function/type without doc comment | LOW |
| Function >100 lines without decomposition | MEDIUM |
| Deeply nested code (>3 levels) — prefer early returns | LOW |
| Variable named `i`, `x`, `tmp` outside of trivial loops | NIT |
| Boolean parameter in public API (use options struct) | LOW |
| Duplicate code that should be extracted to shared function | MEDIUM |
| Inconsistent naming with surrounding code | LOW |

---

## 16. Duplication & Consistency

| Check | Severity |
|-------|----------|
| Function duplicated across packages (two implementations of same logic) | HIGH |
| Copy-pasted code block with minor variations | MEDIUM |
| Inconsistent patterns between similar files (e.g., one activity uses DI, sibling doesn't) | LOW |
| New helper that duplicates existing utility in `utils/` | MEDIUM |

---

## Severity Definitions

| Level | Meaning | Action |
|-------|---------|--------|
| **CRITICAL** | Production crash, data loss, security hole, silent wrong behavior | Must fix before merge |
| **HIGH** | Likely bug, race, leak, or architectural violation | Should fix before merge |
| **MEDIUM** | Increases maintenance cost or tech debt | Fix recommended |
| **LOW** | Minor readability or style issue | Optional fix |
| **NIT** | Cosmetic preference | Author's discretion |
