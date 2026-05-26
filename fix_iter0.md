● Read review result file (shell)
  │ cat /tmp/reviews/review_result.md 2>/dev/null | head -200
  └ 19 lines...

● Check script line counts (shell)
  │ wc -l /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/merge-results.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh 2>/dev/null
  └ 5 lines...

● Read merge-results.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/merge-results.sh
  └ L270:370 (101 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L1225:1275 (51 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L1365:1410 (45 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L1275:1375 (101 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L1410:1530 (121 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L1530:1650 (121 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L960:1000 (41 lines read)

● Read build-check.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh
  └ L1:50 (50 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L1000:1080 (81 lines read)

● Find exit code classification in build-check.sh (shell)
  │ grep -n "exit_class\|exit_code_class\|exit.*class\|124\|timeout\|137\|OOM\|e
  │ rror_class" /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh | head
  │ -60
  └ 61 lines...

● Find go.sum section in build-check.sh (shell)
  │ grep -n "gosum\|go\.sum\|go_sum\|gosum_new_count\|sum_new\|new.*dep"
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh | head -60
  └ 36 lines...

● Read build-check.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh
  └ L1005:1055 (51 lines read)

● Read build-check.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh
  └ L2808:2860 (53 lines read)

Now let me look at where "same failures on main" is shown and the P1-3 double-hash issue:

● Find exit code references in post-fallback-comments.sh (shell)
  │ grep -n "same failures\|exit=\|exit_code\|exit_class\|main_exit"
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh | head
  │ -40
  └ 7 lines...

● Find double-hash bug in post-fallback-comments.sh (shell)
  │ grep -n "##[0-9]\|Fix ##\|Fix #{\|double\|f\"Fix\|e\[.num.\]"
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh | head
  │ -30
  └ 20 lines...

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L690:710 (21 lines read)

● Find the Fix # companion text (shell)
  │ grep -n "f\"Fix #\|f'Fix #\|companion.*first\|Fix #{"
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh | head
  │ -20
  └ 4 lines...

● Find #{} patterns that could cause double-hash (shell)
  │ grep -n "#{"
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh | grep -v
  │ "^#\|//" | head -30
  └ 20 lines...

● Find double-hash in post-fallback-comments.sh (shell)
  │ grep -n "##\|'##\|\"##"
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh | head
  │ -30
  └ 31 lines...

● Find VULN_IDS_LIST and VULN_NEW_LIST usage (shell)
  │ grep -n "VULN_NEW_LIST\|VULN_IDS_LIST\|_VULN_IDS_LIST\|VULN_NEW_COUNT"
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh | head
  │ -20
  └ 12 lines...

● Find VULN_NEW_LIST in build-check.sh (shell)
  │ grep -n
  │ "VULN_NEW_LIST\|VULN_IDS_LIST\|VULN_NEW_COUNT\|vuln_new_findings\|vuln_new"
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh | head -40
  └ 8 lines...

Now let me look at how the exit code class is used in post-fallback-comments.sh and what the P1-1 fix needs:

● Read build-check.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh
  └ L260:320 (61 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L218:240 (22 lines read)

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L620:710 (91 lines read)

● Read build-check.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh
  └ L3200:3230 (30 lines read)

● Find gosum usage in post-fallback-comments.sh (shell)
  │ grep -n "gosum\|GOSUM\|go_sum\|transitive"
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh | head
  │ -40
  └ 29 lines...

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L460:495 (36 lines read)

● Read build-check.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh
  └ L3150:3200 (51 lines read)

Now I have a clear picture of all the bugs. Let me make all the fixes.

**P0-1: Fix CVE misattribution in merge-results.sh** — `dependency.package` in the GitHub API is a nested object `{name, ecosystem}`, not a plain string.

● Edit merge-results.sh +1 -1
  └ /tmp/brk-worktrees/iter0/.github/scripts/merge-results.sh

● Edit merge-results.sh +1 -1
  └ /tmp/brk-worktrees/iter0/.github/scripts/merge-results.sh

● Validate merge-results.sh syntax (shell)
  │ bash -n /tmp/brk-worktrees/iter0/.github/scripts/merge-results.sh && echo
  │ "OK"
  └ 2 lines...

**P0-2: Fix MERGE NOW on L2 PRs** — split `_sec_safe` into L4 (tests run) and L2 (tests not run) with different labels:

● Edit post-fallback-comments.sh +10 -4
  └ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh

**P0-3: Fix companion inheriting vulns** — when a companion PR has verdict=vulns_introduced, the blocked PR should also be marked:

● Edit post-fallback-comments.sh +11 -2
  └ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh

● Validate post-fallback-comments.sh syntax after P0 fixes (shell)
  │ bash -n /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  │ && echo "OK"
  └ 2 lines...

**P1-1: Add exit code class name to L1 verification text:**

● Edit post-fallback-comments.sh +5
  └ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh

● Edit post-fallback-comments.sh +1 -1
  └ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh

**P1-2: Fix go.sum math** — count unique module names, not raw lines; drop confusing line-delta display:

● Edit build-check.sh +1 -1
  └ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh

Now drop the confusing go.sum line-delta context in post-fallback-comments.sh:

● Edit post-fallback-comments.sh -3
  └ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh

**P1-3: Fix double-hash `##N`** — `companions` already contains `#`, so `Fix #{companions}` → `Fix ##N`:

● Edit post-fallback-comments.sh +1 -1
  └ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh

**P1-4: Fix VULN_NEW_LIST truncated** — remove `[:5]`:

✗ Edit build-check.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh
  └ No match found

● Find exact VULN_NEW_LIST line (shell)
  │ grep -n "VULN_NEW_LIST"
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh | head -10
  └ 1 line...

● Find VULN_NEW_LIST in build-check.sh (shell)
  │ grep -n "VULN_NEW_LIST\|vuln_new_list\|new_list"
  │ /tmp/brk-worktrees/iter0/.github/scripts/build-check.sh | head -10
  └ 1 line...

● Find VULN_NEW_LIST in all scripts (shell)
  │ grep -n "VULN_NEW_LIST\|vuln_new"
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh | head
  │ -10
  └ 11 lines...

The P1-4 bug is in `post-fallback-comments.sh` at line 271:

● Read post-fallback-comments.sh
  │ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh
  └ L268:275 (8 lines read)

● Edit post-fallback-comments.sh +1 -1
  └ /tmp/brk-worktrees/iter0/.github/scripts/post-fallback-comments.sh

Now validate all three files and commit:

● Validate all three scripts syntax (shell)
  │ cd /tmp/brk-worktrees/iter0
  │ bash -n .github/scripts/post-fallback-comments.sh && echo
  │ "post-fallback-comments.sh OK"
  │ bash -n .github/scripts/merge-results.sh && echo "merge-results.sh OK"
  │ bash -n .github/scripts/build-check.sh && echo "build-check.sh OK"
  └ 4 lines...

All syntax checks pass. Now commit:

● Commit all fixes (shell)
  │ cd /tmp/brk-worktrees/iter0
  │ git add -A
  │ git commit -m 'iter10: fix 3 P0s + 5 P1s from review (score 7.2)
  │
  │ P0-1 (CVE misattribution): merge-results.sh — fix alert matching to use…
  └ 35 lines...

All done. Here's what was fixed:

1. **P0-1 (CVE misattribution) — `merge-results.sh`**: GitHub's Dependabot API returns `dependency.package` as `{name, ecosystem}` object, not a string. Fixed both alert-matching sites to use `.get("package", {}).get("name", "")` — ensuring CVEs are only attributed to PRs upgrading the exact matching package.

2. **P0-2 (MERGE NOW on L2) — `post-fallback-comments.sh`**: Split `_sec_safe` into `_sec_safe_l4` (tests verified) and `_sec_safe_l2` (build-only). L4 PRs show **"MERGE NOW"**, L2/L3 PRs now show **"MERGE AFTER REVIEW (tests not run — verify manually)"**.

3. **P0-3 (companion inherits vulns) — `post-fallback-comments.sh`**: When building `companion_blocked`, check if the blocking PR has `verdict=vulns_introduced` and propagate that verdict to the companion entry (same target version = same CVEs).

4. **P1-1 (exit code classes) — `post-fallback-comments.sh`**: Added `_L1_MAIN_CLASS` (computed from exit code: 124→timeout, 137→OOM/killed) and appended it to the L1 line: `"same failures on main (exit=N, timeout)"`.

5. **P1-2 (go.sum math) — `build-check.sh` + `post-fallback-comments.sh`**: Fixed `GOSUM_NEW_COUNT` to count unique module names (`awk+sort -u`) instead of raw lines (go.sum has 2 lines/module). Dropped the confusing `"go.sum: N→M lines"` delta display that contradicted the count when `go mod tidy` removes lines.

6. **P1-3 (double-hash `##`) — `post-fallback-comments.sh`**: `companions` already contains `"#5, #12"`, so `Fix #{companions}` produced `Fix ##5`. Changed to `Fix {companions}`.

7. **P1-4 (VULN_NEW_LIST truncated) — `post-fallback-comments.sh`**: Removed `[:5]` slice — all CVE IDs now appear in the "New CVEs:" line, not just the first 5.

