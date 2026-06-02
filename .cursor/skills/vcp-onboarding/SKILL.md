---
name: vcp-onboarding
description: Guides VCP engineers through architecture, local setup, workflow traces, deep-dive debugging, root-cause analysis, and contributing fixes. Use when the user runs /onboard, asks about onboarding, understanding workflows, debugging failures, RCA, or how to contribute to VCP.
---

# VCP Onboarding

Hybrid guide: structured curriculum first, then free-form Q&A. Scope is **VCP team broadly** — backend, local dev, GCP setup, technical deep dives, and fixing the real root cause with evidence.

## Activation

- User runs `/onboard` or asks to onboard / get started with VCP.
- Command file: `.cursor/commands/onboard.md` — follow it for topic routing.

## Boundaries

| Topic | Route to |
|-------|----------|
| Prod incident with correlation ID | `triagebot` (run after `/onboard deep-dive`) |
| Deep dives, failure debugging, RCA | [deep-dive.md](deep-dive.md) |
| ONTAP product features | `/ontap` |
| Branch/PR code review | `/review` |
| Live GCNV API calls | `google-proxy-api` skill or `gcnvapis` |
| CVS/CVP/CVN internals | Mention they exist on the request path; triagebot cross-repo mode for deep dives |

## Phases

| Phase | Goal | Primary reference |
|-------|------|-------------------|
| 0 — Assess | Experience, what's done, what they need now | Ask 2–3 short questions on first `/onboard` only |
| 1 — Big picture | Services, request path, data stores | [architecture-map.md](architecture-map.md) |
| 2 — Environment | GCP consumer project, PSA, local stack | [local-dev-checklist.md](local-dev-checklist.md) |
| 3 — Golden path | Trace a real flow end-to-end | [golden-paths.md](golden-paths.md) |
| 4 — Contribute | Standards, tests, evidence-backed fixes | [contribution-guide.md](contribution-guide.md) |
| 5 — Q&A | Free-form; route to references | [workflow-index.md](workflow-index.md), [debugging-playbook.md](debugging-playbook.md), [doc-index.md](doc-index.md) |
| 6 — Deep dive & RCA | How failures work, how to prove root cause, fix the real thing | [deep-dive.md](deep-dive.md) |

**Assess questions (first visit only, keep brief):**
1. Backend experience with Go / Temporal / Kubernetes? (none / some / strong)
2. Environment status: consumer project? local Postgres + Temporal running?
3. Immediate goal: setup / understand architecture / trace a workflow / deep dive a failure / start coding?

Skip questions already answered in the conversation.

## Reference files

| File | Use when |
|------|----------|
| [architecture-map.md](architecture-map.md) | Components, diagrams, request flows |
| [workflow-index.md](workflow-index.md) | Which workflow file handles what |
| [golden-paths.md](golden-paths.md) | Step-by-step code traces |
| [local-dev-checklist.md](local-dev-checklist.md) | GCP + local dev verification |
| [debugging-playbook.md](debugging-playbook.md) | Temporal, logs, triagebot intro |
| [deep-dive.md](deep-dive.md) | How failures work, RCA ladder, triagebot → proven fix |
| [contribution-guide.md](contribution-guide.md) | Coding standards, PR norms, evidence-backed fixes |
| [doc-index.md](doc-index.md) | Curated links into `doc/` |

Read only the references needed for the current topic. Prefer repo docs and code over memory.

## Mandatory output template

Every `/onboard` response MUST include these sections (omit empty subsections, not the section headers):

```markdown
### Where you are
- **Phase**: <0–5 name>
- **Progress**: <checklist items done / remaining, or "returning — topic: X">

### Today's focus
1. <concrete action>
2. <optional second>
3. <optional third>

### Explanation
<Prose + mermaid diagram when tracing a flow. Lead with the "so what" for a new hire.>

### Code & doc pointers
- `path/to/file.go` — <one line why>
- `doc/...` — <one line why>

### Verify
- [ ] <how they know this step is done>

### Next
- `/onboard <suggested topic>` or ask: "<example question>"
```

## Teaching rules

1. **Evidence-based** — cite paths and docs; label gaps **Unknown**.
2. **One level deeper, not ten** — explain the layer they're asking about; offer to go deeper.
3. **Workflows vs activities** — workflows orchestrate (deterministic); activities do I/O (DB, ONTAP, GCP).
4. **LRO pattern** — most mutating APIs return an operation; client polls until `done`.
5. **Core boundary** — `core/` must not import GCP SDKs; cloud code lives in `hyperscaler/google/`.
6. **Do not dump** — prefer one golden path over listing every workflow file.
7. **Root cause before fix** — never teach a workaround as a fix; route to evidence (logs, triagebot, code path). Label unproven claims **Hypothesis** or **Unknown**.
8. **Operational lens** — when explaining flows, note where failures surface (sync API vs LRO vs activity) and what proves customer impact.

## Suggested curriculum (first three weeks)

| Week | Focus | Command |
|------|-------|---------|
| 1 | Architecture + local setup + run services | `/onboard` → `/onboard setup` |
| 1 | Trace volume create end-to-end | `/onboard trace volume` |
| 2 | Workflow catalog for your team's area | `/onboard workflows` |
| 2 | Debug a failed workflow locally | `/onboard debug` |
| 2 | Failure surfaces + RCA — how to find the real cause | `/onboard deep-dive` |
| 3 | Run triagebot on a staging correlation ID | `triagebot` |
| 3 | Pick a task, prove root cause, open PR | `/onboard contribute` |

## Related skills

- **local-config** — generate `env.development.local`
- **google-proxy-api** — invoke Google Proxy REST endpoints locally
- **swagger-review** — when touching OpenAPI specs
