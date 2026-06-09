# PRD: Breakability — Dependency Upgrade Break Classifier

**Status:** Draft v1
**Owner:** Platform / Developer Productivity
**Last updated:** 2026-06-09

---

## 1. Problem

Dependabot (and equivalent) opens a flood of dependency-bump PRs. Today a human must, per PR, read the changelog, reason about semver, figure out whether their code even touches the changed APIs, then build and test. This is slow, repetitive, and inconsistent. Most bumps are safe but still cost human attention; the genuinely risky ones get the same shallow review as the trivial ones.

Commercial reachability tools (e.g. call-graph SCA) are accurate but compute-intensive and slow on monorepos, and they answer "is the vuln reachable" more than "will this upgrade break my build/behavior."

## 2. Goal

Automatically classify each dependency-bump PR as **SAFE**, **BREAKS**, or **NEEDS_REVIEW** with a confidence band (low / medium / high), backed by concrete evidence, and produce a repo-level **merge plan**. Eliminate 80–85% of the manual effort: the large majority of PRs land in SAFE or BREAKS with high confidence and a recommended action, leaving only a small, genuinely ambiguous tail for humans.

Primary ecosystem: **Go**. Secondary (later): **Node/TypeScript**.

### 2.1 Success metrics (how we quantify the goal)

Measured against a **labeled corpus** of historical bump PRs (ground-truth verdicts assigned by humans):

- **Automation rate** = % PRs classified SAFE or BREAKS at confidence ≥ medium. Target ≥ 80%.
- **Precision on SAFE** (no false "safe") ≥ 98%. This is the safety-critical metric — a wrong SAFE auto-merges a break.
- **Recall on BREAKS** ≥ 90%.
- **NEEDS_REVIEW rate** ≤ 15–20%.
- **Median wall-clock per PR** ≤ a few minutes in CI (bounded, no monorepo-wide graph build).
- **False-SAFE incidents in production** = 0 tolerance target; every one is a postmortem and a new corpus fixture.

The single non-negotiable: **never auto-merge a real break.** We bias all ambiguity toward REVIEW or BREAKS, never toward SAFE.

## 3. Non-goals

- Not a vulnerability scanner; we classify *breakage* of upgrades, not CVE severity (though it can consume SCA output as a prioritization input).
- Not a full sound whole-program call-graph engine. We deliberately avoid global static analysis to stay fast.
- Not auto-fixing every break — we draft migration patches but do not guarantee them.
- Not (initially) a general multi-language platform; Go first, designed for extension.

## 4. Verdict taxonomy

| Verdict | Meaning | Default action |
|---|---|---|
| `SAFE` | No structural break; no reachable behavioral break | Auto-merge eligible (high), else review-lite |
| `BREAKS` | Structural or confirmed behavioral break | Block; attach evidence + draft patch |
| `NEEDS_REVIEW` | Ambiguous; signals conflict or undocumented behavioral risk on uncovered code | Human review |

Confidence bands derive from evidence strength (see §7). Auto-merge only on `SAFE` + `high`.

## 5. Architecture overview

Two independent tracks per PR feed an AI reconciler:

```
              ┌─ Deterministic track ────────────────┐
              │ semver · API diff (gorelease/apidiff) │
   PR ──┬────►│ build oracle · test oracle ·          │─┐
        │     │ changelog extraction · testability    │ │
        │     └───────────────────────────────────────┘ │
        │     ┌─ AI track (independent) ──────────────┐  │
        └────►│ changelog comprehension · demand-      │  │
              │ driven reachability · char-test synth ·│  │
              │ differential test reasoning            │  │
              └────────────────────────────────────────┘ │
                                                          ▼
                            ┌─ AI reconciler ────────────────┐
                            │ inputs: both verdicts+evidence  │
                            │ rules: compiler wins ties;      │
                            │ conflict→REVIEW; agree→high     │
                            └──────────────┬─────────────────┘
                                           ▼
                       verdict · confidence · evidence · draft patch
                                           ▼
                       per-PR comment   +   repo-level merge plan
```

**Design principle: replace the global call graph with (a) the compiler as a structural-reachability oracle and (b) demand-driven AI reachability over only the changed symbols.** This is what keeps it fast on monorepos.

## 6. Modular component design

Each step is a **standalone module** with a strict input/output JSON contract, independently buildable and testable, then wired. Modules are pure where possible (input → output, no hidden state) so they can be unit-tested with fixtures.

| # | Module | Input | Output | Independent? |
|---|---|---|---|---|
| M1 | **PR ingestor** | PR ref / Dependabot metadata | `{ecosystem, dep, old_ver, new_ver, semver_class, manifest_paths}` | Yes |
| M2 | **Changelog fetcher** | dep, old→new | raw release notes / CHANGELOG / tags text | Yes |
| M3 | **API-diff (Go)** | dep old/new module versions | `{changed_symbols[], compat: bool}` via `gorelease`/`apidiff` | Yes |
| M4 | **Scope resolver** | bumped module, repo module graph | affected packages = reverse-deps of bump | Yes |
| M5 | **Build oracle** | repo @ PR, scoped packages | `{builds: bool, errors[]}` (`go build ./scoped`) | Yes |
| M6 | **Test oracle** | repo @ PR, scoped packages | `{tests_pass, no_test_files, affected_callsite_coverage}` | Yes |
| M7 | **Testability scorer** | M3 + M6 | `{coverage_of_changed_callsites, has_meaningful_tests}` | Yes |
| M8 | **Changelog comprehension (AI)** | M2 text | structured `breaking_claims[] {symbol, old, new, severity}` | Yes (mockable) |
| M9 | **Demand-driven reachability (AI)** | M3 changed_symbols + repo grep | `{reachable_callsites[], behavioral_reachable: bool}` | Yes (mockable) |
| M10 | **Characterization/differential test synth** | affected callsites, old/new dep | `{behavior_diff: bool, evidence}` | Semi (needs build env) |
| M11 | **Deterministic verdict** | M1,M3,M5,M6,M7 | `{verdict, confidence, evidence}` | Yes |
| M12 | **AI reconciler** | M11 + M8 + M9 + M10 | final `{verdict, confidence, evidence, patch?}` | Wired-test |
| M13 | **PR commenter** | final verdict | posts comment | Yes |
| M14 | **Merge-plan generator** | all PR verdicts in repo | ordered plan, conflict detection | Yes |

**Interdependencies & how we wire-test them:**
- M11 depends on M3/M5/M6/M7 → integration test with real fixture repos (deterministic, no AI).
- M12 (AI reconciler) depends on M8/M9/M10 → these are mocked first (record/replay fixtures of AI output), so M12's *rules* are unit-tested without live model calls. Then a small set of live integration tests exercise the real model path.
- The AI modules (M8/M9/M10) are tested via **golden eval cases**: fixed input → expected structured output, scored with tolerance (not exact-string).

Every module communicates via versioned JSON schemas in a shared contracts dir, so a module can be rewritten/improved internally without breaking neighbors as long as the contract holds.

## 7. Confidence model

Testability is a first-class input. **Coverage of the changed symbols' call sites**, not global coverage, gates confidence.

| Affected-callsite coverage | Behavior changed? (M3/M8) | Verdict / confidence |
|---|---|---|
| High | No | SAFE / high |
| High | Yes, diff test green | SAFE / high |
| High | Yes, diff test shows change | BREAKS / high |
| Low / none | No (structural-only, build green, compat) | SAFE / medium |
| Low / none | Yes, synth/diff test resolves it | follows test / medium |
| Low / none | Yes, no meaningful test possible | NEEDS_REVIEW |
| "no test files" (false-green) | ambiguous | downgrade — never auto-SAFE |

Tiebreak rules: **compiler/build failure always wins** over any "safe" AI opinion. Track disagreement → REVIEW unless one side has a hard signal (build fail = BREAKS, gorelease incompatible + reachable = BREAKS).

## 8. Edge-case handling (explicit)

- **No / poor tests:** compiler still catches all *structural* breaks → SAFE/medium possible with zero tests. For behavioral risk, synthesize **characterization tests** (capture current behavior on old version, diff against new) and run **differential testing** with the old version as oracle; native Go fuzzing for changed functions; extract upstream `Example` tests. Residual undocumented behavioral change on uncovered side-effect-heavy code = honest NEEDS_REVIEW.
- **False-green build/test:** scope to reverse-deps (M4); detect "no test files"; tests that fail to compile against new version count as BREAKS.
- **Flaky tests:** retry N; only persistent failures classify.
- **Reflection / generics / `interface{}` / `go:linkname`:** compiler can't see dynamic dispatch breaks → flag and lower confidence.
- **Indirect/transitive bumps:** usually SAFE (not directly called); verify direct-dep API surface unaffected.
- **Multi-platform / cgo / build tags:** matrix-build targeted OS/arch; a bump may break only one platform.
- **Vendored / generated code:** re-sync `go mod vendor` / regenerate before building.
- **Monorepo partial results:** per-target verdicts, not one repo verdict.
- **Behavioral breaks generally:** changelog (M8) is primary signal → map documented claims to call sites (M9) → confirm via differential test (M10). Undocumented behavioral breaks are the irreducible REVIEW tail.

## 9. Fast local development & test strategy

Goal: tight inner loop so bugs are fixed in few iterations, with no regression and no endless loops.

### 9.1 Fixture corpus (the backbone)
- A versioned set of **real, pinned bump scenarios**: `(repo snapshot, dep, old→new, ground-truth verdict, ground-truth evidence)`.
- Sourced from real historical Dependabot PRs + hand-crafted edge cases (no tests, reflection, behavioral-only break, multi-platform, transitive).
- Each fixture is a self-contained directory; checked into the test corpus, not the product repo.

### 9.2 Run real-repo analysis locally, fast
- **Offline mode:** all network inputs (changelog fetch, registry, model calls) are **record/replay**. First run records to cassettes; subsequent runs replay → deterministic, sub-second, no network, no model cost.
- **Module-local runs:** each module has a CLI entrypoint so you can run `M3` or `M9` alone against a fixture and inspect JSON output. No need to run the whole pipeline to test one step.
- **Scoped builds only:** M4 ensures we only `go build`/`go test` the reverse-deps of the bump, not the whole monorepo — keeps even large fixtures fast.
- **Warm caches:** Go build cache + module cache persisted between runs (locally and in CI) so repeated analysis of the same repo is fast.
- **Pre-pulled deps:** fixture corpus vendors both old and new dependency versions so differential builds need no network.

### 9.3 Avoiding regression and endless loops
- **Eval harness** runs the whole pipeline over the entire fixture corpus and reports per-verdict precision/recall + a diff vs the last run. This is the single "are we better or worse" signal.
- **Golden outputs:** each fixture has expected verdict + key evidence. A change that flips any previously-correct fixture is a **regression gate** — CI fails. This prevents "fixing one case breaks another" oscillation.
- **No-oscillation rule:** every bug fix must add or update a corpus fixture *first* (red), then code goes green. A previously-fixed fixture turning red again is blocked by the gate. This is how we guarantee forward-only progress.
- **AI determinism harness:** AI modules are scored on the corpus with replayed model responses for unit tests, plus a separate, smaller **live eval** run (real model) tracked over time for drift. Live eval is allowed tolerance; replay eval is strict.
- **Per-module health:** each module reports its own accuracy on its slice of the corpus, so you know *which* module regressed, not just that the end-to-end number moved.

### 9.4 Copilot/local vs Cursor-in-CI
- Production target: **Cursor agent in GitHub Actions** for M8/M9/M10/M12.
- Local dev: the AI modules talk to a pluggable model backend behind one interface, so you can run them with **GitHub Copilot / local model / recorded cassettes** during development, then swap to Cursor in CI with no contract change. Same JSON in/out either way.

## 10. GitHub Actions + Cursor integration

Triggered on Dependabot PR open/sync:
1. M1 ingest → M2 changelog, M3 API-diff, M4 scope (parallel).
2. Deterministic job: M5 build, M6 test, M7 testability → M11 verdict. Matrix per affected module / platform.
3. AI job (Cursor agent): M8, M9, M10 over the changed-symbol set only.
4. M12 reconciler merges both → final verdict + draft patch.
5. M13 posts per-PR comment; M14 regenerates repo merge plan.
6. Optional: `SAFE`+`high` → auto-merge; `BREAKS`/`REVIEW` → label + assign.

Bounded work per PR (one/few modules, only changed symbols analyzed) → fast, parallel, monorepo-safe.

### 10.1 Per-PR comment (example)
```
## Breakability: SAFE ✅ (high)
Version: v1.4.2 → v1.5.0 (minor)
API compat (gorelease): compatible
Build: ✅   Tests: ✅ (132 passed, affected-callsite coverage 87%)
Changelog: no breaking markers
Reachability: 0 changed symbols reached from app
Recommendation: auto-merge
```

### 10.2 Merge plan (repo-level)
Aggregates all open bump PRs: grouped by verdict, ordered SAFE-high (batchable) → grouped dep families → BREAKS-with-patch → REVIEW. Detects inter-PR `go.mod` conflicts and sequences them. Re-generated every run.

## 11. MVP definition

**MVP scope (Go only):**
- M1–M7, M11 (full deterministic track).
- M8 changelog comprehension + M9 demand-driven reachability (AI), M12 reconciler.
- M13 per-PR comment, M14 basic merge plan.
- Record/replay local harness + fixture corpus (≥ 30 labeled fixtures spanning the edge cases in §8).
- Eval harness with regression gate.

**MVP explicitly excludes:** M10 characterization/differential synthesis (behavioral confirmation), Node support, auto-merge automation, multi-platform matrix.

**MVP exit / acceptance conditions:**
- Automation rate ≥ 70% on the corpus (pre-behavioral-track).
- SAFE precision ≥ 98% on the corpus.
- Zero false-SAFE on the labeled break fixtures.
- End-to-end per-PR run ≤ a few minutes in CI on a representative repo.
- Regression gate active and green.

## 12. Release plan

| Phase | Adds | Target outcome |
|---|---|---|
| **P0 — Deterministic core** | M1–M7, M11, scope resolver, build/test oracle, corpus + eval harness | Structural breaks classified with high confidence; SAFE precision proven on corpus |
| **P1 — AI reachability (MVP)** | M8, M9, M12, M13, M14, record/replay, Cursor-in-CI | MVP exit conditions met; comments + merge plan live on a pilot repo |
| **P2 — Behavioral confirmation** | M10 characterization + differential + fuzz; testability scoring fully wired | Cut NEEDS_REVIEW toward ≤ 15%; behavioral breaks caught even with no tests |
| **P3 — Automation & scale** | Auto-merge for SAFE/high, multi-platform matrix, monorepo tuning, drift monitoring | 80–85% automation goal hit on real fleet; SLA on per-PR latency |
| **P4 — Node/TS** | TS API-diff, ecosystem adapters | Second ecosystem at parity on structural track |

## 13. How we know it's working (instrumentation)

- Corpus eval dashboard: automation rate, SAFE precision, BREAKS recall, REVIEW rate, per-module accuracy, run-over-run delta.
- Production shadow mode before auto-merge: run on real PRs, post comments, but humans still merge; compare verdict vs human decision to validate precision live before trusting automation.
- Every production false-SAFE or false-BREAKS becomes a new corpus fixture (forward-only learning).
- Latency + cost (model tokens, CI minutes) tracked per PR.

## 14. Risks & mitigations

| Risk | Mitigation |
|---|---|
| False SAFE auto-merges a break | Compiler-wins tiebreak; SAFE-high gate; shadow mode before auto-merge; precision metric is the release blocker |
| AI nondeterminism / drift | Replay cassettes for unit eval; tolerance-scored live eval tracked over time; reconciler rules are deterministic |
| Behavioral breaks with no tests | Characterization + differential testing (old version as oracle); honest REVIEW for the irreducible tail |
| Monorepo slowness | Reverse-dep scoping, warm caches, demand-driven reachability instead of global call graph |
| Endless fix loops / regressions | Fixture-first bug fixes + regression gate + per-module accuracy |
| Changelog missing/uninformative | Fall back to API-diff + differential testing; weight build oracle |

## 15. Open questions

- Threshold tuning for auto-merge (which deps/maintainers are trusted enough for SAFE-high auto-merge?).
- Where to host the fixture corpus and how to keep it fresh from production outcomes.
- Cost ceiling per PR for the AI track at fleet scale.
- Cursor agent vs other agent runners in Actions — capability/cost comparison (deferred, interface-isolated).
