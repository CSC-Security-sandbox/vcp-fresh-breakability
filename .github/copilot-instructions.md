# Breakability Analysis — Repository Instructions for Copilot

This repository contains the **Breakability Analysis** system — a hybrid deterministic + AI pipeline that analyzes Dependabot PRs in monorepos.

## Architecture

- `.github/scripts/build-check.sh` (~2300 lines) — deterministic shell script: discovers PRs, runs baseline builds, builds PR branches, compares errors, outputs structured JSON
- `.github/workflows/breakability-agent.yml` — GitHub Actions workflow (3 jobs: discover → deterministic build × N batches → merge + AI analysis)
- `.github/breakability-prompt.md` (~960 lines) — structured instructions for the AI analysis agent
- `.github/breakability-config.yml` — per-repo config (mode, private registries, infra patterns)
- `.github/scripts/post-fallback-comments.sh` — fallback comments when AI agent fails
- `.github/scripts/merge-results.sh` — merges results from parallel batches

## Key Principles

1. **Hard facts come from deterministic tooling.** The AI agent adds context but CANNOT override build results.
2. If `tsc` fails, the verdict is BUILD_FAILS — no matter what the AI thinks.
3. The `verification_level` and `verification_label` fields in `build-results.json` are the source of truth. Copy them verbatim.
4. **100% PR coverage is mandatory.** Every PR must get a comment. Use fallback if analysis is incomplete.
5. The system runs in advisory mode by default — verdicts are recommendations, not blockers.

## How to Build and Test

- Shell scripts: `bash -n .github/scripts/build-check.sh` for syntax check
- No npm build step needed — the TypeScript CLI in `breakability-check/` is pre-compiled
- Test with: `cd breakability-check && npm test`

## Target Repos

- Sandbox: `CSC-Security-sandbox/ndm-breakability-test` — NestJS monorepo, 8 services, 4 shared libs, 1 Go module
- Rollout: `greenqloud/*` org — 30 Go, 12 Python, 15 Shell, 5 TypeScript repos

## Conventions

- All scripts use `set -euo pipefail`
- Every new variable in `build-check.sh` MUST be initialized (due to `set -u`)
- JSON output uses Python heredoc (`python3 << EOF`) because bash has no native JSON support
- PR comments start with `<!-- breakability-check -->` hidden marker for cleanup
