#!/usr/bin/env bash
# reach_check.sh — run the scoped call-graph reachability prover for one Go PR and
# emit its JSON to stdout. Intended to run in the DETERMINISTIC stage, where the
# repo is already checked out at the PR's version (the same place build+test run).
#
# The result should be baked into the PR artifact under deterministic.reach so the
# AI adjudicator prompt (render_prompt.py) and the policy layer can consume a SOUND
# reachability signal instead of name-grep.
#
# Usage:
#   reach_check.sh <module_dir> <dep_import_prefix> <symbols_csv> [repo_root]
#
# On any failure it emits {"analyzed": false, "error": "..."} and exits 0 — callers
# MUST treat a non-analyzed result as UNKNOWN reachability (conservative), never safe.
set -uo pipefail

MODULE_DIR="${1:?module_dir required}"
DEP_PREFIX="${2:?dep import prefix required}"
SYMBOLS_CSV="${3:-}"
REPO_ROOT="${4:-$(pwd)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REACH_SRC="$SCRIPT_DIR/../breakability/reach"
REACH_BIN="${BRK_REACH_BIN:-/tmp/brk-reach}"

emit_fail() { printf '{"analyzed": false, "error": "%s"}\n' "$1"; exit 0; }

command -v go >/dev/null 2>&1 || emit_fail "go toolchain not found"
[ -z "$SYMBOLS_CSV" ] && emit_fail "no flagged symbols to check"

# Build the prover once (cached at REACH_BIN).
if [ ! -x "$REACH_BIN" ] || [ "$REACH_SRC/main.go" -nt "$REACH_BIN" ]; then
  ( cd "$REACH_SRC" && go build -o "$REACH_BIN" . ) >/tmp/brk-reach-build.log 2>&1 \
    || emit_fail "reach build failed (see /tmp/brk-reach-build.log)"
fi

# Resolve the module directory relative to the repo root.
MOD_PATH="$REPO_ROOT"
[ -n "$MODULE_DIR" ] && [ "$MODULE_DIR" != "." ] && MOD_PATH="$REPO_ROOT/$MODULE_DIR"
[ -f "$MOD_PATH/go.mod" ] || emit_fail "no go.mod at $MOD_PATH"

OUT="$("$REACH_BIN" -module "$MOD_PATH" -dep "$DEP_PREFIX" -symbols "$SYMBOLS_CSV" -tests=false 2>/tmp/brk-reach-run.log)"
RC=$?
if [ $RC -ne 0 ] || [ -z "$OUT" ]; then
  emit_fail "reach run failed rc=$RC (see /tmp/brk-reach-run.log)"
fi
printf '%s\n' "$OUT"
