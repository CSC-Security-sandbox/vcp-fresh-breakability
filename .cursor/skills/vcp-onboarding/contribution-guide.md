# Contribution Guide

How to go from onboarding to your first merged PR.

## Before your first code change

- [ ] Local stack runs (`/onboard setup`)
- [ ] Traced volume create once (`/onboard trace volume`)
- [ ] Read `CODING_GUIDELINES.md` (imports, boundaries, errors, testing)
- [ ] Skim `.cursor/rules/go-project-rule.mdc` — applies to all `**/*.go`

## Repo rules that bite new hires

| Rule | Where | Summary |
|------|-------|---------|
| Core boundary | `CODING_GUIDELINES.md`, go-project-rule | No GCP SDK imports in `core/**` |
| Context first | go-project-rule | `ctx context.Context` is first param |
| VCP errors | `core/errors/README.md` | Use `vsaerrors`; see error taxonomy |
| API errors | `doc/api/error-taxonomy.md`, api-development.mdc | HTTP mapping, validation patterns |
| Workflows | workflow.mdc | No I/O in workflows; use activities |
| Tests | testing.mdc | Table-driven, mocks, `AssertExpectations`, no globals |
| Generated code | CODING_GUIDELINES | Do not edit `*_gen.go`; change generators |

## Where to make changes (by task type)

| Task | Likely touch points |
|------|---------------------|
| API request/response | `google-proxy/api/endpoints/`, `doc/swagger.yaml` |
| Business validation | `core/orchestrator/factory/gcp/` |
| New workflow step | `workflows/` + `activities/` + worker registration |
| DB schema | `database/migrations/`, `database/vcp/` |
| GCP integration | `hyperscaler/google/` |
| ONTAP behavior | `core/vsa/`, `ontap-proxy/` |
| Feature flag | `config/*.yaml`, env vars in `common/` |

## Development workflow

```bash
# Create branch
git checkout -b <ticket>-short-description

# Build + test (narrow during dev)
go test ./path/to/package/...

# Lint (project make targets if available)
make lint   # if defined

# Full package tests before push
go test ./core/orchestrator/workflows/...   # example
```

## Testing expectations

- **New behavior** → new or updated tests in same PR
- **Workflows** → `*_workflow_test.go` with mocked activities
- **Activities** → mock `database.Storage`, `hyperscaler.Services`
- **API** → handler tests in `*_endpoint_test.go`

See `.cursor/rules/testing.mdc` for patterns.

## PR hygiene

1. **Small PRs** — one logical change; easier review
2. **Description** — what, why, how to test
3. **Link ticket** — Jira ID in title or body
4. **No drive-by refactors** — go-coding-standards exempts legacy code
5. **OpenAPI** — if spec changed, run swagger-review skill or note in PR

## Code review in this repo

- Self-review: `/review` against `main` before requesting humans
- Review authority: `.cursor/commands/includes/review-authority.md`

## Production bug fixes (PE bar)

Bug fixes are welcome early **when the engineer proves root cause first**. Symptom patches without evidence get rejected in review.

### Before writing code

1. **Correlation ID** (staging or prod) — run `triagebot` or equivalent log bundle
2. **Failed step** — component, layer, operation, primary error (from triage report or Temporal history)
3. **Origin service** — VCP vs CVS vs CVP vs CVN with boundary evidence if cross-service
4. **Design check** — read relevant `doc/workflows/`, `doc/architecture/designs/`, or auto-gen design doc
5. **Hypothesis** — one sentence: "X failed because Y" — must match log + code path

If any step is **Unknown**, gather more evidence before coding.

### While fixing

- Fix the **earliest on-path** cause, not downstream noise
- Add or extend a **test** that would have caught the bug (handler, activity, or unit)
- Do not expand scope — one root cause per PR when possible

### PR description must include

- Correlation ID (or repro steps)
- Failed step (copy from triagebot pinpoint block)
- Why this change addresses the proven cause
- How you verified (test name, staging re-run)

### Full methodology

See [deep-dive.md](deep-dive.md) and `.cursor/rules/triagebot.mdc`.

## Good first tasks (ask your mentor)

| Area | Example starter |
|------|-----------------|
| Tests | Increase coverage on a small activity |
| Docs | Fix drift in `doc/workflows/` vs code |
| Validation | Add handler test for edge case in endpoints |
| Observability | Add structured log field with correlation |
| **Bug fix** | Ticket with **triagebot report** + clear failed step |

Avoid as first PR: new workflow types, schema migrations, cross-service changes.

## API development

If touching `doc/swagger.yaml` or endpoints:

- Read `.cursor/rules/api-development.mdc`
- Do not edit generated server code — change spec/generators
- Error responses must align with `doc/api/error-taxonomy.md`

## Database changes

- Read `.cursor/rules/database.mdc`
- Migrations must be reversible where project requires it
- Coordinate with team for production deploy order

## Getting unblocked

| Blocker | Action |
|---------|--------|
| Don't understand flow | `/onboard trace volume` or `/onboard workflows` |
| Test mock setup | Find similar `*_test.go` in same package |
| Error code choice | `doc/api/error-taxonomy.md` + `core/errors/README.md` |
| ONTAP semantics | `/ontap <feature>` |
| Staging failure | `triagebot` with correlation ID — then [deep-dive.md](deep-dive.md) |

## Definition of done (first PR)

- [ ] Tests pass locally
- [ ] Follows CODING_GUIDELINES for touched lines
- [ ] PR description explains test plan
- [ ] No secrets, debug prints, or commented-out code
- [ ] Mentor assigned reviewer familiar with the area
