---
name: design-doc
description: Draft VCP architecture design docs under doc/architecture/designs/. Discovery-first workflow — switches to plan mode, asks for high-level requirements, iterates with targeted follow-ups, presents a plan for approval, drafts only after approval. Assumes nothing about scope, design type, data model, APIs, or trade-offs. Drafts implementation-ready docs with full function signatures, struct definitions with GORM/JSON tags, endpoint request/response schemas, pseudo-code, error codes with HTTP mapping, and SQL — honoring the project's go-coding-standards, api-development, database, and testing cursor rules. Phases target ≤20 files / ≤5000 lines per PR as guidance (exceptions allowed). Drafts are concise: no filler, no hedging. Use when the user asks to write, draft, scaffold, or update a design doc, ADR, or architecture proposal.
---

# Design Doc Writer (VCP house style)

Drafts architecture design docs for `doc/architecture/designs/`. Section scaffolds and recurring patterns live in [TEMPLATE.md](TEMPLATE.md).

## When to use

- Writing a new design doc / ADR / architecture proposal.
- Expanding a rough idea into a full design doc.
- Updating an existing design doc to match the house style.
- Adding missing sections (TL;DR, Goals/Non-Goals, Open Questions) to a doc.

## Hard rules

- **No drafting before the plan is approved.** Discovery and planning first; file write only after explicit approval.
- **No invented technical details.** Don't make up package paths, table names, endpoint shapes, error codes, or defaults. If the user hasn't named it, ask.
- **No assumed design type.** Confirm the classification with the user.
- **Re-ask vague answers.** "It should be fast" → "p50 target? Throughput floor?"
- **Push back once on "you decide".** Suggest 2 named options. If still deferred, capture in `Open Questions`.
- **Docs are implementation-ready.** Every new function, struct, endpoint, query, error code is fully specified — see [Technical depth](#technical-depth).
- **Phases are sized for review.** Target ≤5000 +/- lines and ≤20 files per phase, excluding generated. Exceptions allowed with rationale — see [PR sizing](#pr-sizing).
- **Drafts are concise.** See [Drafting style](#drafting-style).

## Workflow

Five phases. Do not skip ahead.

### Phase A — Mode and intake

Switch to plan mode via `SwitchMode` (`target_mode_id: "plan"`). If the user declines, continue but still defer all writing to Phase E.

### Phase B — High-level requirements

Ask once, as a single prompt:

> Before I draft anything, I need a high-level picture:
> 1. **Problem** — what's broken, missing, or painful today?
> 2. **End-state** — what does the world look like once this is implemented?
> 3. **Scope** — services / modules in play; explicitly out of scope?
> 4. **Constraints** — fixed decisions, untouchable components, deadlines, dependencies on other designs.
> 5. **Prior art** — similar designs in this repo or adjacent (CVS, CVP, CVN)?

Re-ask any missing item before moving on.

### Phase C — Type detection and targeted follow-ups

1. Classify the design type (see [Pick a design type](#pick-a-design-type)) and **confirm with the user**: "This looks like a `<Type>`. Sound right, or closer to `<other-type>`?"
2. Ask type-specific follow-ups from [Discovery questions](#discovery-questions) until the [Pre-draft checklist](#pre-draft-checklist) is fully satisfied.
3. Genuinely deferred decisions go to `Open Questions`, not back to the user.

### Phase D — Present the plan

Compact plan in chat. No file write.

```
Plan for design doc:

Filename: doc/architecture/designs/NNNN-<kebab-case-title>.md
Design type: <Structural | Feature | Algorithm | Mixed> — <one-line justification>

Section outline:
  1. <Section> — <content sourced from user answer>
  ...

Technical surface:
  - Structs: <names>
  - Functions: <names + rough signatures>
  - Endpoints: <method + path>
  - Error codes: <constants + range>
  - Algorithms: <which sections have pseudo-code or mermaid>

Phases (each one PR; target ≤20 files / ≤5000 lines excl. generated):
  - Phase 1 — <name> — est. <N files>, <~N lines>, generated: <list or "none">. Mergeable: yes.
  - Phase N — <name> — est. <N files>, <~N lines>. OVER TARGET — rationale: <one line>.

Project rules that shape the draft:
  - <e.g. go-coding-standards.mdc, api-development.mdc>

Open items (to surface in Open Questions): <list>
Stubbed sections: <list>

Approve to proceed, or tell me what to add / drop / reshape.
```

Iterate until explicit approval. Tentative replies ("maybe", "I think so") don't count — ask one more question.

### Phase E — Draft

Only after approval:

1. Switch back to agent mode via `SwitchMode` (`target_mode_id: "agent"`).
2. Read [TEMPLATE.md](TEMPLATE.md).
3. Read any applicable rule files from [Project rules to honor](#project-rules-to-honor) before drafting that area.
4. Pick the filename — next unused `NNNN` in `doc/architecture/designs/`.
5. Draft using only facts the user gave. Anything else goes to `Open Questions` or is omitted — never invented.
6. Apply [Cross-linking](#cross-linking), [Technical depth](#technical-depth), [Drafting style](#drafting-style).
7. Self-review against the [Quality checklist](#quality-checklist).
8. Do not commit.

Reply with a short confirmation block:

```
Draft written to doc/architecture/designs/NNNN-<kebab-case-title>.md.

Design type: <as approved>. Sections: <count>. Open Questions: <count>. Stubbed: <list or "none">.

Next steps: <e.g. resolve Open Questions inline; share for review>.
```

For follow-up edits, name each change explicitly. Don't silently rewrite large sections.

## Pick a design type

| Type | Use when | Specialty sections (see TEMPLATE.md) |
|---|---|---|
| **Structural / Refactor** | Repo/module split, large refactor, build pipeline reorg | Background; Options & Recommendation; Proposed Layout; Dependency Graph; How Components Consume Each Other; External Consumption Contract; Build and Packaging; Failure Modes and Risks; References |
| **Feature / Subsystem** | New capability, new API, new data model, new in-process library | Design Decisions; Data Model; API surface; Algorithm / Check Utilities; Error Taxonomy; Usage Pattern; Migration Impact; Testing Strategy; Rollout and Operations |
| **Algorithm / Behavior** | New scheduling/placement/retry/state-machine; behavior change with little new API | Design Decisions; Algorithm with state diagrams; Behavior matrix; Edge cases; Testing Strategy; Rollout |
| **Mixed** | Part refactor + part new feature | Pick from both menus; keep common skeleton |

If ambiguous, ask: "Is this primarily a refactor of existing code, or a new capability?"

## Discovery questions

Cycle through these in Phase C. **Do not ask all at once.** Pick the next round based on gaps in the user's last answer. Stop when the [Pre-draft checklist](#pre-draft-checklist) is satisfied.

**Common (every design):**

1. **Title** — suggest one; let the user confirm or change.
2. **Goals** — ≥3 measurable outcomes (not "make X better").
3. **Non-Goals** — ≥2 things explicitly NOT done. Re-ask if fewer.
4. **Phasing** — single-shot or 2–4 staged phases? For each phase: what lands, rough file/line estimate excl. generated, independently mergeable? Propose a split if over target.
5. **Open questions / deferred decisions** — anything not yet decided, including known follow-ups.

**Structural / Refactor (add):**

6. **Current state** — paths, file counts, services, dependency shape. Numbers help.
7. **Alternatives** — ≥1 alternative; suggest one if the user has only one option.
8. **Pros / cons** per option.
9. **Recommendation reasoning** — which option, why the cons are acceptable.
10. **Affected pipelines** — Makefile, Dockerfile, Helm, Skaffold, codegen — touched or NOT touched?
11. **Risks** — ≥3 with mitigations.

**Feature / Subsystem (add):**

6. **Design decisions** — identity, storage shape, defaults, caching, API style — choice + reason each.
7. **Data model** — tables / columns / struct fields with names, Go types, GORM tags, indexes, constraints. Nullable? Defaults? Unique?
8. **API surface** — per endpoint or function: HTTP method + full path (or full Go signature with types); request body shape (fields + types + validation); response body for success + each error case; auth model.
9. **Helper / util signatures** — package, name, params with types, returns with types, one-line behavior.
10. **Algorithms** — numbered prose + pseudo-code; edge cases (nil, empty, race, retry).
11. **Identifier resolution** — UUID, name, both?
12. **Error codes** — code (next free in range), constant `Err<PascalCase>`, HTTP status, retryable yes/no, when returned. Or explicit "no new codes".
13. **Migration impact** — existing rows / data behavior, defaults for absent values, AutoMigrate or hand-written SQL.
14. **Testing** — unit (which functions + matrix), integration (which endpoints), mockery interfaces, new test infra.
15. **Rollout** — lifecycle, rollback, breaking-change handling.

**Asking style:**

- `AskQuestion` for one-of-N answers; conversational for open-ended or chained.
- Call out contradictions explicitly.
- Don't accept a function or endpoint name without its full signature / request shape.

## Technical depth

Every section that introduces new technical surface must specify it concretely:

| Surface | Must include |
|---|---|
| **Go function / method** | Full signature (package, name, all params with types, all returns with types). One-line behavior. `vsaerrors` constant per failure case. `context.Context` first; `error` last. |
| **Struct / model** | Full Go declaration with GORM / JSON tags. DB models: column types, nullability, defaults, unique constraints, indexes. JSONB round-trip notes. |
| **REST / RPC endpoint** | Method + full path + auth. Request body schema. Response body for success + each error. Error codes with the matching range from [doc/api/error-taxonomy.md](../../../doc/api/error-taxonomy.md): 1000–1999 workflow, 2000–2999 database, 3000–3999 GCP, 4000–4999 VSA cluster, 5000–5999 ONTAP, 6000–6999 validation, 7000–7999 rate/quota, 8000–8999 security/KMS, 9000–9999 internal. HTTP mapping: validation→422 (syntax→400); duplicate→409; not-found→404; retriable→500; auth→401/403. |
| **Algorithm / decision flow** | Numbered prose steps (the contract). Pseudo-code or Go-flavored snippet. Mermaid `flowchart TD` for non-trivial branches. Edge cases explicit (nil, empty, race, retry). |
| **DB query / write** | SQL snippet with `?` placeholders. Transactional scope, isolation, rollback boundary. Indexes touched / needed. |
| **Error code** | Numeric code, `Err<PascalCase>` constant, HTTP status, one-line meaning, retryable in Temporal yes/no. |
| **Config / env var / feature flag** | Name, type, default, scope (process/pod/cluster). Source (file / Helm value / secret). Startup validation rule. |
| **Cross-component call** | Caller + callee, wire protocol (HTTP / gRPC / in-process), request/response shape, timeout, retry policy, caller fallback on failure. |

If the user describes any of the above without the required detail, ask. Defer to `Open Questions` only when the user explicitly says "TBD" or "decide in implementation".

## PR sizing

Each phase under `Migration Plan` / `Implementation Phases` should be a single, independently mergeable PR. Targets (not gates):

- ≤ 5000 additions + deletions combined.
- ≤ 20 files changed.
- Both exclude auto-generated files.

Auto-generated files (excluded from the count): `*_gen.go`, `*-servergen/`, `*-clientgen/`, files under `[.mockery.yaml](../../../.mockery.yaml)` / `[.monkeyMocks.yaml](../../../.monkeyMocks.yaml)`, `*.pb.go`, `*_grpc.pb.go`, `clients/<service>/swagger-codegen/`, snapshot / golden regenerations. Name any generated files the phase touches in its `What lands` / `Changes` so reviewers skip them.

Exceptions allowed when atomicity is genuinely required (mass rewrites that can't land in pieces, security-critical changes, regenerations). State the rationale in the phase intro: "Phase 3 — atomic import rewrite. Est. 45 files, ~3800 lines. Cannot be split: intermediate states leave the build broken."

Independence (every phase, including over-target): merges and deploys on its own; repo builds and tests pass after each; no half-applied state. Scaffold and first real adoption are separate phases unless the user explicitly justifies combining.

Common splits when over target:

- Scaffold + adoption.
- Per-service or per-package.
- Read path vs write path.
- Interface + mock first; implementations later.
- Generated regen as its own mechanical-only PR.

## Project rules to honor

Read each only when the design actually touches that area.

| If the design introduces… | Honor | The design must… |
|---|---|---|
| Any new Go code | `[.cursor/rules/go-coding-standards.mdc](../../../.cursor/rules/go-coding-standards.mdc)`, `[.cursor/rules/go-project-rule.mdc](../../../.cursor/rules/go-project-rule.mdc)`, `[CODING_GUIDELINES.md](../../../CODING_GUIDELINES.md)` | `context.Context` first param; `error` last return; `vsaerrors.NewVCPError(...)` for VCP errors; `fmt.Errorf("...: %w", err)` for wrapping; slog with `correlation_id` / `workflow_id` / `job_id`; respect the core boundary (no `cloud.google.com/go`, `google.golang.org/api`, `hyperscaler/google` in `core/**`); struct-based config + constructor DI; no globals. |
| New API endpoints / OpenAPI changes | `[.cursor/rules/api-development.mdc](../../../.cursor/rules/api-development.mdc)`, `[doc/api/error-taxonomy.md](../../../doc/api/error-taxonomy.md)` | `{ "code": <number>, "message": "<text>" }` error envelope; codes from the right range; HTTP mapping; validation owner (google-proxy vs core-api); never edit generated code — change generators or specs. |
| DB models / migrations / DAO methods | `[.cursor/rules/database.mdc](../../../.cursor/rules/database.mdc)` | GORM struct tags explicit (`gorm:"column:...;not null;uniqueIndex"`); transactions for multi-table writes; `context.Context` first; AutoMigrate behavior described; error codes 2000–2999. |
| Tests / test infra | `[.cursor/rules/testing.mdc](../../../.cursor/rules/testing.mdc)` | Table-driven layout; mockery mocks (`.mockery.yaml`); `mock.AssertExpectations(t)`; DI / constructor injection, no globals; one behavior per test; AAA. |
| Temporal workflows / scheduler | go rules | Retryable vs non-retryable Temporal errors per call site; activity options / retry policy; `temporal.ApplicationError` for fail-fast. |

Multi-area designs honor all applicable rules. When uncertain, link the rule file from the design itself.

## Cross-linking

Design docs live at `doc/architecture/designs/<name>.md`. From that location:

- **Repo files / dirs** — bracketed link with relative path: `[Makefile](../../../Makefile)`.
- **Other design docs in this folder** — slug only: `[0014 — ONTAP Proxy Rule Engine](0014-ontap-proxy-rule-engine.md)`.
- **Same-doc section jumps** — anchor: `[Migration Plan](#migration-plan)`. Anchor = heading lowercased, hyphenated, punctuation stripped.
- **Inline file mentions in prose** — bracket them: ``the existing `[core/Dockerfile](../../../core/Dockerfile)` ``.

If a path is referenced more than twice, link the first occurrence in each major section.

## Drafting style

Every sentence carries information. Default to fewer words.

**Cut on sight:** throat-clearing ("It is important to note that …", "As mentioned above …", "This section will describe …"); heading restatement ("The data model for this design is …" under `## Data Model`); doc self-justification ("This design is important because …"); hedging ("we believe", "perhaps", "ideally", "essentially"); empty connectives ("Furthermore", "Moreover", "Additionally"); restating what code already says; editorializing ("elegantly", "cleanly", "robust", "seamless").

**Prefer:** short sentences (one clause); bullet lists for 3+ items; tables for comparisons; numbered steps for procedures; code blocks over prose-describing-code; concrete paths / names / numbers over abstract phrases.

**Section-length targets** (exceed only if the content demands it):

- TL;DR: ≤ 7 bullets, each ≤ 1 sentence, `**Bold lead-in.**` shape (e.g. `- **Problem.** <one-line>`).
- Overview: ≤ 4 paragraphs.
- Goal / Non-Goal / Open Question entry: 1 sentence.
- Risk row: 1 line.
- Phase intro: ≤ 3 sentences before the bullets.

If a section needs more, add subsections rather than longer paragraphs. Lead with the answer; uncertain decisions go to `Open Questions`, not into hedged prose. Code blocks must show full context — full struct definitions, full `go.mod` snippets, full SQL — not pseudo-code stubs.

## Pre-draft checklist

Verify each item from what the user has actually said. If any is `Unknown` or `Guessed`, return to Phase C.

**Always:**
- [ ] Title and one-line purpose.
- [ ] Problem and end-state.
- [ ] ≥3 measurable goals.
- [ ] ≥2 non-goals with reason.
- [ ] Phase list with PR size estimate (files + lines, excl. generated) per phase. Over-target phases have a split proposal or a rationale.
- [ ] Each phase is independently mergeable and deployable.
- [ ] ≥1 open question or deferred decision.

**Structural also:**
- [ ] Current state with concrete numbers / paths.
- [ ] ≥2 design options with pros / cons; explicit recommendation + reasoning.
- [ ] Every affected pipeline (Makefile / Dockerfile / Helm / Skaffold / codegen) identified or explicitly not affected.
- [ ] ≥3 risks with mitigations.

**Feature also:**
- [ ] Each design decision named with chosen value + reason.
- [ ] Data model: tables / columns / fields with names, Go types, GORM tags, indexes, constraints.
- [ ] Every endpoint / function has full signature (method + path + request + response, or full Go sig).
- [ ] Identity / auth model.
- [ ] Error codes named (or "no new codes").
- [ ] Algorithm for any non-trivial flow: numbered steps + pseudo-code.
- [ ] Testing approach: unit + integration + mockery interfaces.
- [ ] Rollout: lifecycle, rollback, breaking-change handling.

**Technical depth (every design):**
- [ ] No function named without a full signature.
- [ ] No struct named without a full declaration with tags.
- [ ] No endpoint named without request + response + error codes.
- [ ] No algorithm in prose without numbered steps or pseudo-code.
- [ ] Every cross-component call names caller + callee + wire protocol + timeout / retry.
- [ ] Every new error code references the right numeric range.
- [ ] Every new config / env var has name + type + default + scope.

## Quality checklist

Run after drafting, before delivery.

**Common:**
- [ ] Title present.
- [ ] TL;DR uses `**Bold lead-in.**` bullets and opens with `**Problem.**`.
- [ ] Overview ≤ 4 paragraphs.
- [ ] Goals and Non-Goals both populated (Non-Goals never empty).
- [ ] Each phase shows size estimate in its intro; generated files called out; over-target phases include the reason; phases described as independently mergeable.
- [ ] No section opens by restating its heading or justifying the doc; no hedging or filler.
- [ ] Open Questions non-empty.

**Structural:**
- [ ] Background — Current State has concrete facts.
- [ ] ≥2 Options with Pros/Cons; explicit Recommendation paragraph.
- [ ] Proposed Layout marks NEW / EXISTING (changed) / EXISTING (unchanged).
- [ ] Dependency Graph has prose key invariants underneath.
- [ ] Build and Packaging covers every pipeline the design touches (don't pad).
- [ ] Failure Modes and Risks table present.

**Feature:**
- [ ] Design Decisions table OR Options & Recommendation — not neither.
- [ ] Data Model shows full Go struct(s) with GORM/JSON tags; JSONB/serialized fields have round-trip notes.
- [ ] API surface has `Method | Path | Purpose` table (REST) plus request/response schemas, OR a function-signature list with full types (in-process).
- [ ] Every new function shows `context.Context` first, `error` last.
- [ ] Algorithm has numbered prose AND a mermaid flowchart for non-trivial flows; edge cases explicit.
- [ ] Error Taxonomy table (if new codes) shows code + constant + HTTP + meaning + retryable; links to `[core/errors/errors.json](../../../core/errors/errors.json)` and `[doc/api/error-taxonomy.md](../../../doc/api/error-taxonomy.md)`.
- [ ] Usage Pattern shows a concrete call site with correct error handling.
- [ ] Testing Strategy lists unit (table-driven), integration, mockery-mocked interfaces.
- [ ] Rollout covers lifecycle, rollback, and breaking-change handling (when applicable).

**House style:**
- [ ] All file paths are bracketed markdown links relative to `doc/architecture/designs/`.
- [ ] Inter-section anchors use lowercased-hyphenated heading text.
- [ ] Tables for 3+ row comparisons.
- [ ] Code blocks show full snippets.
- [ ] Diagrams paired with explanatory prose.
- [ ] Every "this will change" claim names the file or package it lands in.
- [ ] No time-sensitive language ("after Q3"); use phase-relative or condition-relative.
