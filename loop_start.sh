#!/bin/bash
set -euo pipefail
cd /tmp/brk

# First iteration: use seeded review, skip CI wait (already completed)
# Then hand off to loop.sh for subsequent iterations

WORKTREES_DIR="/tmp/brk-worktrees"
REVIEWS_DIR="/tmp/reviews"
BASE_DIR="/tmp/brk"
BRANCH="staging/v7-improvements"
WORKFLOW="breakability-agent.yml"
COPILOT="/Users/hpoornac/Library/Application Support/Code/User/globalStorage/github.copilot-chat/copilotCli/copilot"
ITER=0

log() { echo "$(date '+%H:%M:%S') [bootstrap] $*" | tee -a /tmp/brk/loop.log; }

mkdir -p "$WORKTREES_DIR"
rm -f /tmp/brk/loop.log

# ── Tag for rollback ──
git tag -f "iter0-pre" HEAD
log "Tagged iter0-pre at $(git rev-parse --short HEAD)"
log "Using seeded review (score 7.2, 3 P0s, 5 P1s, 4 P2s)"

# ── Create worktree ──
WORKTREE="$WORKTREES_DIR/iter0"
rm -rf "$WORKTREE"
git worktree add "$WORKTREE" -b "iter0-fixes" HEAD 2>/dev/null || {
  git branch -D "iter0-fixes" 2>/dev/null || true
  git worktree prune 2>/dev/null || true
  git worktree add "$WORKTREE" -b "iter0-fixes" HEAD
}
log "Worktree at $WORKTREE"

# ── Fix using copilot ──
log "Launching copilot fix agent with seeded review..."
cat > "$REVIEWS_DIR/prompt_fix.txt" << FIXEOF
You are a senior engineer. Fix the bugs listed in $REVIEWS_DIR/review_result.md.

WORKING DIRECTORY: $WORKTREE/.github/scripts/
Only edit files there. The main scripts are:
- post-fallback-comments.sh (PR comments + merge plan generation, ~1800 lines)
- merge-results.sh (data aggregation from build results, ~420 lines)
- build-check.sh (per-PR build analysis, ~3900 lines — edit only if needed for go.sum/exit-code fixes)

RULES:
1. Fix ALL P0 issues first, then P1 issues
2. After EACH file edit, run: bash -n <file> to validate syntax
3. Do NOT edit any YAML workflow files
4. When done, run these commands:
   cd $WORKTREE
   bash -n .github/scripts/post-fallback-comments.sh
   bash -n .github/scripts/merge-results.sh
   git add -A
   git commit -m 'iter10: fix 3 P0s + 5 P1s from review (score 7.2)'
5. Do NOT push — just commit locally

Key context for each fix:
P0-1 (CVE misattribution): merge-results.sh around line 280-350 has alert matching. It matches by package ecosystem + version range. The bug is it attributes alerts to PRs that touch the same ecosystem but different packages. Fix: add exact package name match.

P0-2 (MERGE NOW on L2): post-fallback-comments.sh line ~1375-1395, the _sec_safe list includes all PRs with CVE fixes regardless of verification level. Fix: split into _sec_safe_l4 and _sec_safe_l2, show L4 as "MERGE NOW" and L2 as "MERGE AFTER REVIEW (tests not run)".

P0-3 (companion inherits vulns): post-fallback-comments.sh line ~1233-1260. When building companion_blocked, if the blocking PR has verdict=vulns_introduced, the companion should also be marked as vulns_introduced (same target version = same vulns).

P1-1 (exit code classes): build-check.sh error classification section. Add exit_class mapping. In post-fallback-comments.sh where it says "same failures on main (exit=X)", add the class name.

P1-2 (go.sum math): build-check.sh gosum section. The gosum_new_count counts new module names but go.sum line delta can decrease due to go mod tidy. Fix: just show module names, drop the confusing line count delta OR clarify it.

P1-3 (double-hash ##): post-fallback-comments.sh Python heredoc around line 1590. Look for f-strings with #{...} — in a bash heredoc, # doesn't need escaping but inside Python f-strings within bash heredocs you might get double-#. Find and fix.

P1-4 (VULN_NEW_LIST truncated): post-fallback-comments.sh line ~974 where _VULN_IDS_LIST is set. Check if it's using the full list or just first 5.

P1-5 (go vet): This is complex — skip if time-constrained. Just add a note in the comment if go vet output is detected in build output.

Respond with a numbered list of what you fixed.
FIXEOF

"$COPILOT" -p "$(cat $REVIEWS_DIR/prompt_fix.txt)" \
  --add-dir "$REVIEWS_DIR" \
  --add-dir "$WORKTREE" \
  --yolo \
  --effort high \
  > "$REVIEWS_DIR/fix_result.md" 2>/dev/null || true

log "Fix agent done"
tail -40 "$REVIEWS_DIR/fix_result.md"
cp "$REVIEWS_DIR/fix_result.md" "$BASE_DIR/fix_iter0.md"

# ── Merge back if committed ──
if git -C "$WORKTREE" log --oneline -1 | grep -q "iter10"; then
  log "Fixes committed. Merging back..."
  git merge "iter0-fixes" --no-edit
  git worktree remove "$WORKTREE" --force 2>/dev/null || true
  git branch -D "iter0-fixes" 2>/dev/null || true
  log "Merged. HEAD: $(git rev-parse --short HEAD)"

  # Push and trigger CI
  git push origin "$BRANCH"
  log "Pushed"
  gh workflow run "$WORKFLOW" --ref "$BRANCH"
  log "CI triggered. Now run loop.sh for subsequent iterations."
  log "Command: nohup bash /tmp/brk/loop.sh >> /tmp/brk/loop.log 2>&1 &"
else
  log "WARNING: Fix agent did not commit successfully."
  log "Check: $REVIEWS_DIR/fix_result.md"
  git worktree remove "$WORKTREE" --force 2>/dev/null || true
  git branch -D "iter0-fixes" 2>/dev/null || true
fi
