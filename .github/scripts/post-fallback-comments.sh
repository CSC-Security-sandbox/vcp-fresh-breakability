#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# post-fallback-comments.sh — Graceful degradation for breakability analysis
#
# If the AI agent crashes or misses PRs, this script posts structured build
# analysis comments for every PR that lacks one. Comments are generated
# directly from the deterministic JSON results — no AI required.
#
# Comment quality by scenario:
#   - Trivially safe (patch/transitive/actions/docker): brief SAFE one-liner
#   - Build pass with new errors or major bump: BUILD ANALYSIS with details
#   - Build fail: BUILD_FAILS with error excerpt and remediation note
#   - Build not run (skip/infra_error): REVIEW with infrastructure context
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

RESULTS_FILE="/tmp/build-results.json"

if [[ ! -f "$RESULTS_FILE" ]]; then
  echo "No build-results.json found — nothing to do"
  exit 0
fi

OWNER_REPO=$(gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null || echo "unknown/unknown")
OWNER="${OWNER_REPO%%/*}"
REPO="${OWNER_REPO##*/}"

POSTED=0
SKIPPED=0
FAILED=0

# Read advisory mode from results metadata
BC_MODE=$(python3 -c "
import json
with open('$RESULTS_FILE') as f:
    data = json.load(f)
print(data.get('metadata', {}).get('mode', 'advisory'))
" 2>/dev/null || echo "advisory")

ADVISORY_FOOTER=""
if [[ "$BC_MODE" == "advisory" ]]; then
  ADVISORY_FOOTER="
> 🔬 **Advisory mode** — This analysis is informational. No merges are blocked."
fi

echo "Checking for PRs that need fallback comments (mode: $BC_MODE)..."

# Get all PR numbers from build-results.json
PR_NUMBERS=$(python3 -c "
import json
with open('$RESULTS_FILE') as f:
    data = json.load(f)
for num in sorted(data.get('prs', {}).keys(), key=int):
    print(num)
")

for PR_NUM in $PR_NUMBERS; do
  # Skip if PR already has a breakability comment
  HAS_COMMENT=$(gh api "repos/$OWNER/$REPO/issues/$PR_NUM/comments" \
    --jq '[.[] | select(.body | contains("<!-- breakability-check -->") or contains("<!-- breakability-agent -->"))] | length' \
    2>/dev/null || echo "0")

  if [[ "$HAS_COMMENT" -gt 0 ]]; then
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  # Extract all fields for this PR in one python call
  PR_FIELDS=$(python3 -c "
import json, sys
with open('$RESULTS_FILE') as f:
    data = json.load(f)
pr = data['prs'].get('$PR_NUM', {})
build = pr.get('build', {})
test = pr.get('test', {})
print(json.dumps({
    'package':      pr.get('package', '?'),
    'from':         pr.get('from', '?'),
    'to':           pr.get('to', '?'),
    'bump':         pr.get('bump', '?'),
    'dep_type':     pr.get('dep_type', '?'),
    'dep_relation': pr.get('dep_relation', '?'),
    'ecosystem':    pr.get('ecosystem', '?'),
    'verdict':      build.get('verdict', '?'),
    'install_method': build.get('install_method', ''),
    'install_ok':   build.get('install_ok', False),
    'new_errors':   build.get('new_errors', []),
    'output_tail':  build.get('output_tail', ''),
    'test_ran':     test.get('ran', False),
    'test_exit':    test.get('exit', -1),
    'verification_label': pr.get('verification_label', ''),
    'files_importing': pr.get('files_importing', []),
    'cves':         pr.get('cves', []),
    'error_class':  build.get('error_class', ''),
}))
" 2>/dev/null || echo '{}')

  PKG=$(echo "$PR_FIELDS"     | python3 -c "import json,sys; print(json.load(sys.stdin).get('package','?'))")
  FROM=$(echo "$PR_FIELDS"    | python3 -c "import json,sys; print(json.load(sys.stdin).get('from','?'))")
  TO=$(echo "$PR_FIELDS"      | python3 -c "import json,sys; print(json.load(sys.stdin).get('to','?'))")
  BUMP=$(echo "$PR_FIELDS"    | python3 -c "import json,sys; print(json.load(sys.stdin).get('bump','?'))")
  DEP_TYPE=$(echo "$PR_FIELDS"  | python3 -c "import json,sys; print(json.load(sys.stdin).get('dep_type','?'))")
  DEP_REL=$(echo "$PR_FIELDS"   | python3 -c "import json,sys; print(json.load(sys.stdin).get('dep_relation','?'))")
  ECOSYSTEM=$(echo "$PR_FIELDS" | python3 -c "import json,sys; print(json.load(sys.stdin).get('ecosystem','?'))")
  VERDICT=$(echo "$PR_FIELDS"   | python3 -c "import json,sys; print(json.load(sys.stdin).get('verdict','?'))")
  INSTALL_METHOD=$(echo "$PR_FIELDS" | python3 -c "import json,sys; print(json.load(sys.stdin).get('install_method',''))")
  INSTALL_OK=$(echo "$PR_FIELDS"     | python3 -c "import json,sys; print(json.load(sys.stdin).get('install_ok',False))")
  VER_LABEL=$(echo "$PR_FIELDS"      | python3 -c "import json,sys; print(json.load(sys.stdin).get('verification_label',''))")
  NEW_ERR_COUNT=$(echo "$PR_FIELDS"  | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('new_errors',[])))")
  FILES_COUNT=$(echo "$PR_FIELDS"    | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('files_importing',[])))")

  # CVE extraction — core security data
  CVE_LIST=$(echo "$PR_FIELDS" | python3 -c "
import json, sys
d = json.load(sys.stdin)
cves = d.get('cves', [])
if cves:
    print(','.join(cves))
else:
    print('')
" 2>/dev/null || echo "")
  CVE_COUNT=$(echo "$PR_FIELDS" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('cves',[])))")

  # Build security line for comment templates
  CVE_LINE=""
  if [[ "$CVE_COUNT" -gt 0 && "$CVE_COUNT" != "0" ]]; then
    # Format: 🔴 2 CVEs: CVE-2024-1234, CVE-2024-5678
    CVE_LINE="
🔴 **Security: $CVE_COUNT CVE(s) fixed by this upgrade:** $CVE_LIST"
  fi

  # Build "How we checked" checklist from verification_label
  HOW_CHECKED=""
  case "$VER_LABEL" in
    L4*)
      HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ✅ Project builds / type-checks clean
- ✅ Automated tests pass
- ✅ No new errors introduced vs. main
</details>"
      ;;
    L3*)
      HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ✅ Project builds / type-checks clean
- ⬜ Tests not configured or not run
- ✅ No new errors introduced vs. main
</details>"
      ;;
    L2*)
      # L2 = build/type-check passes, but tests fail or weren't run
      TEST_EXIT_RAW=$(echo "$PR_FIELDS" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('test_exit',-1))" 2>/dev/null || echo "-1")
      TEST_RAN_RAW=$(echo "$PR_FIELDS" | python3 -c "import json,sys; print(json.load(sys.stdin).get('test_ran',False))" 2>/dev/null || echo "False")
      if [[ "$TEST_RAN_RAW" == "True" && "$TEST_EXIT_RAW" != "0" && "$TEST_EXIT_RAW" != "-1" ]]; then
        HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ✅ Project builds / type-checks clean
- ❌ Automated tests fail (exit=$TEST_EXIT_RAW — may be pre-existing, also fails on main)
- ✅ No new build errors introduced vs. main
</details>"
      else
        HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ✅ Project builds / type-checks clean
- ⬜ Tests not configured or not run
- ✅ No new build errors introduced vs. main
</details>"
      fi
      ;;
    L1*)
      HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ⬜ Build verification limited
</details>"
      ;;
    *)
      if [[ -n "$VER_LABEL" ]]; then
        HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ⬜ Limited verification performed
</details>"
      fi
      ;;
  esac

  # Excerpt of build output (first 10 lines of errors for context)
  BUILD_EXCERPT=$(echo "$PR_FIELDS" | python3 -c "
import json, sys
d = json.load(sys.stdin)
tail = d.get('output_tail', '')
lines = [l for l in tail.splitlines() if 'error' in l.lower() or 'Error' in l][:8]
if lines:
    print('\n'.join(lines))
else:
    print(tail[:300] if tail else '')
" 2>/dev/null || echo "")

  # Find merge plan issue number
  MERGE_PLAN_NUM=$(gh issue list --label "merge-plan" --state open --json number -q '.[0].number' 2>/dev/null || echo "")
  PLAN_LINE=""
  if [[ -n "$MERGE_PLAN_NUM" ]]; then
    PLAN_LINE="
📋 Merge plan: #$MERGE_PLAN_NUM"
  fi

  # ── Classify and generate comment ─────────────────────────────────────────
  COMMENT=""

  # PRs with breakability:skip label were intentionally excluded from analysis.
  # Don't post any comment — the developer already opted out of analysis.
  if [[ "$VERDICT" == "skipped" ]]; then
    echo "  PR #$PR_NUM has breakability:skip label — skipping fallback comment"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  if [[ "$ECOSYSTEM" == "actions" ]]; then
    # GitHub Actions — always safe, no app code affected
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · dev (CI) · $BUMP

GitHub Actions workflow dependency. No application code affected.

Verification: **NA** — CI-only change${CVE_LINE}${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR*"

  elif [[ "$ECOSYSTEM" == "docker" && "$BUMP" != "major" ]]; then
    # Docker non-major — typically safe
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · production · $BUMP

Docker base image $BUMP bump. No application source changes.${CVE_LINE}${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR*"

  elif [[ "$VERDICT" == "pass" && "$BUMP" == "patch" && "$FILES_COUNT" -lt 5 ]]; then
    # Patch bump, build passes, low usage surface — simple safe
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · $DEP_TYPE · patch

Build: ✅ passes · Verification: **${VER_LABEL:-L1}** · Usage: $FILES_COUNT file(s)

$BUMP bump with passing build. No new type errors introduced.${CVE_LINE}${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR*"

  elif [[ "$VERDICT" == "pass" && "$DEP_REL" == "transitive" ]]; then
    # Transitive dep, build passes
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · transitive · $BUMP

Build: ✅ passes · Verification: **${VER_LABEL:-L1}**

Transitive dependency — your code does not import it directly. Build passes.${CVE_LINE}${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR*"

  elif [[ "$VERDICT" == "pass" ]]; then
    # Build passes — general case
    NEW_ERR_NOTE=""
    if [[ "$NEW_ERR_COUNT" -gt 0 ]]; then
      NEW_ERR_NOTE=" · ⚠️ $NEW_ERR_COUNT new error(s) found"
    fi
    COMMENT="<!-- breakability-check -->
## 🔍 BUILD ANALYSIS — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP

Build: ✅ passes · Verification: **${VER_LABEL:-L1}** · Usage: $FILES_COUNT file(s)$NEW_ERR_NOTE${CVE_LINE}

### Summary (deterministic fallback — no AI analysis)
- Package: \`$PKG\` $FROM → $TO ($BUMP bump)
- Type: $DEP_TYPE / $DEP_REL
- Build passes on PR branch
- New type errors: $NEW_ERR_COUNT

**Recommendation:** Review changelog for $BUMP bump breaking changes. Build passes — merge when ready.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR. Full AI analysis was not performed.*"

  elif [[ "$VERDICT" == "fail" ]]; then
    # Build fails — most important fallback to get right
    EXCERPT_BLOCK=""
    if [[ -n "$BUILD_EXCERPT" ]]; then
      EXCERPT_BLOCK="
\`\`\`
${BUILD_EXCERPT}
\`\`\`"
    fi
    COMMENT="<!-- breakability-check -->
## ❌ BUILD_FAILS — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP

Build: ❌ fails on PR branch, ✅ passes on main · Usage: $FILES_COUNT file(s)${CVE_LINE}

### Build errors (excerpt)$EXCERPT_BLOCK

### What to do
1. Check the full build output in the Actions run for this PR
2. Review the \`$PKG\` $FROM → $TO changelog for breaking changes
3. Fix type errors or update your code to match the new API
4. Re-run the breakability analysis after your fix

**Do not merge — build is broken.** ($BUMP bump)${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR.*"

  elif [[ "$VERDICT" == "pre_existing" ]]; then
    # Pre-existing failures
    COMMENT="<!-- breakability-check -->
## ⚙️ UNVERIFIED — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP

Build: ⚙️ same errors on main and PR branch — pre-existing failure, not caused by this upgrade${CVE_LINE}

### What this means
The build fails on both \`main\` and this PR with the same errors. This upgrade does **not** introduce new failures. However, build verification could not confirm compatibility because the baseline is broken.

**Recommendation:** Fix pre-existing build failures on \`main\` first, then re-analyze. This upgrade is likely safe but unconfirmed.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR.*"

  elif [[ "$VERDICT" == "pre_existing_plus_new" ]]; then
    # Pre-existing + new errors
    COMMENT="<!-- breakability-check -->
## ❌ BUILD_FAILS — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP

Build: ❌ new errors introduced by this PR (on top of pre-existing failures)${CVE_LINE}

This upgrade introduces **$NEW_ERR_COUNT new error(s)** not present on \`main\`. Fix required before merging.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR.*"

  elif [[ "$INSTALL_METHOD" == "infra_error" ]]; then
    # Infrastructure blocked analysis
    COMMENT="<!-- breakability-check -->
## 🔍 REVIEW — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP

Build: ⚠️ blocked by infrastructure error — build verification could not run${CVE_LINE}

### What happened
The build check was blocked by an infrastructure issue (private registry, network timeout, or missing dependency not caused by this upgrade). **This is not a build failure from the upgrade.**

**Recommendation:** Verify infrastructure health, then re-run. If infrastructure is healthy, review manually.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR.*"

  else
    # Catch-all: skip/unknown verdict
    COMMENT="<!-- breakability-check -->
## 🔍 REVIEW — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP

Build analysis status: \`$VERDICT\` (verification: ${VER_LABEL:-unknown})${CVE_LINE}

Automated build analysis was not conclusive for this PR. Manual review recommended.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> ⚠️ *Fallback comment — AI agent did not run or did not cover this PR.*"
  fi

  if [[ -n "$COMMENT" ]]; then
    if gh pr comment "$PR_NUM" --body "$COMMENT" 2>/dev/null; then
      echo "  Posted fallback for PR #$PR_NUM ($PKG ${FROM}→${TO}, $VERDICT)"
      POSTED=$((POSTED + 1))
    else
      echo "  ⚠️  Failed to post fallback for PR #$PR_NUM"
      FAILED=$((FAILED + 1))
    fi
  fi
done

echo ""
echo "Fallback: posted $POSTED, skipped $SKIPPED (already had comments), failed $FAILED"

# ── Regenerate merge plan issue ──────────────────────────────────────────────
# The merge plan must always reflect the latest build-results.json.
# If the AI agent generated a previous plan, it may be stale after a
# deterministic rerun. This section creates/updates the merge plan issue
# from the current data so PR comments and the plan never contradict.
echo ""
echo "════════════ MERGE PLAN ════════════"

MERGE_PLAN_BODY=$(python3 << 'PYEOF'
import json, sys, subprocess
from datetime import datetime, timezone

with open("/tmp/build-results.json") as f:
    data = json.load(f)

prs = data.get("prs", {})
cross = data.get("cross_pr_deps", [])
security = data.get("security_posture", {})

# Count total open PRs (not just Dependabot) for completeness note
try:
    result = subprocess.run(
        ["gh", "pr", "list", "--state", "open", "--json", "number", "-q", "length"],
        capture_output=True, text=True, timeout=30
    )
    total_open_prs = int(result.stdout.strip()) if result.returncode == 0 else 0
except:
    total_open_prs = 0

non_dependabot_count = max(0, total_open_prs - len(prs))

# Categorize PRs
safe = []        # pass verdicts
blocked = []     # fail / pre_existing_plus_new
review = []      # pre_existing / error / infra_error
skipped = []     # skip (actions, docker, etc.)
not_analyzed = [] # anything else

for num, pr in sorted(prs.items(), key=lambda x: int(x[0])):
    v = pr.get("build", {}).get("verdict", "?")
    pkg = pr.get("package", "?")
    fr = pr.get("from", "?")
    to = pr.get("to", "?")
    bump = pr.get("bump", "?")
    dep_type = pr.get("dep_type", "?")
    ver = pr.get("verification_label", "?")
    cves = pr.get("cves", [])
    eco = pr.get("ecosystem", "?")
    entry = {"num": num, "pkg": pkg, "from": fr, "to": to, "bump": bump, "dep_type": dep_type, "ver": ver, "cves": cves, "eco": eco, "verdict": v}

    if v == "skip":
        skipped.append(entry)
    elif v in ("pass",):
        safe.append(entry)
    elif v in ("fail", "pre_existing_plus_new"):
        blocked.append(entry)
    elif v in ("pre_existing", "error"):
        review.append(entry)
    else:
        not_analyzed.append(entry)

# Build markdown
lines = []
lines.append("<!-- breakability-merge-plan -->")
lines.append(f"# 📋 Breakability Merge Plan")
lines.append(f"")
lines.append(f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} (deterministic)")
lines.append(f"**PRs analyzed:** {len(prs)} Dependabot PRs")
if non_dependabot_count > 0:
    lines.append(f"**Not analyzed:** {non_dependabot_count} non-Dependabot PR(s) (out of scope — this tool only analyzes Dependabot dependency upgrades)")
lines.append(f"")

# Summary table
lines.append("## Summary")
lines.append("")
lines.append(f"| Category | Count |")
lines.append(f"|----------|-------|")
lines.append(f"| ✅ Safe to merge | {len(safe)} |")
lines.append(f"| ❌ Fix required | {len(blocked)} |")
lines.append(f"| ⚠️ Manual review | {len(review)} |")
lines.append(f"| ⏭️ Skipped (CI/Docker) | {len(skipped)} |")
if not_analyzed:
    lines.append(f"| ❓ Not analyzed | {len(not_analyzed)} |")
lines.append("")

# CVE highlight
all_cves = []
for cat in [safe, blocked, review, skipped]:
    for e in cat:
        if e["cves"]:
            all_cves.append(e)
if all_cves:
    lines.append("## 🔴 Security — CVEs Fixed by These Upgrades")
    lines.append("")
    for e in all_cves:
        cve_str = ", ".join(e["cves"])
        lines.append(f"- **PR #{e['num']}** `{e['pkg']}` {e['from']}→{e['to']} — {cve_str}")
    lines.append("")

# Safe to merge
if safe:
    lines.append("## ✅ Safe to Merge")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Verification |")
    lines.append("|----|---------|---------|----|-------------|")
    for e in safe:
        cve_badge = f" 🔴 {','.join(e['cves'])}" if e['cves'] else ""
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {e['bump']} | {e['ver']}{cve_badge} |")
    lines.append("")

# Cross-PR deps
if cross:
    lines.append("## 🔗 Coordinated Upgrades (merge together)")
    lines.append("")
    for group in cross:
        reason = group.get("reason", "related")
        pr_nums = group.get("prs", [])
        lines.append(f"- **{reason}:** PRs {', '.join(f'#{p}' for p in pr_nums)}")
    lines.append("")

# Blocked
if blocked:
    lines.append("## ❌ Fix Required — Do Not Merge")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Issue |")
    lines.append("|----|---------|---------|----|-------|")
    for e in blocked:
        issue = "Build fails" if e["verdict"] == "fail" else "New errors on top of pre-existing"
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {e['bump']} | {issue} |")
    lines.append("")

# Review
if review:
    lines.append("## ⚠️ Manual Review Needed")
    lines.append("")
    for e in review:
        reason = "Pre-existing build failure (not caused by upgrade)" if e["verdict"] == "pre_existing" else "Build error / infrastructure issue"
        lines.append(f"- **PR #{e['num']}** `{e['pkg']}` {e['from']}→{e['to']} — {reason}")
    lines.append("")

# Skipped
if skipped:
    lines.append("## ⏭️ Skipped (CI / Docker)")
    lines.append("")
    for e in skipped:
        lines.append(f"- PR #{e['num']} `{e['pkg']}` ({e['eco']})")
    lines.append("")

# Security posture
if security:
    lines.append("## 🛡️ Repository Security Posture")
    lines.append("")
    open_alerts = security.get("open_alerts", 0)
    fixable = security.get("alerts_fixable_by_merging", 0)
    lines.append(f"- Open Dependabot alerts: **{open_alerts}**")
    if fixable:
        lines.append(f"- Alerts fixable by merging these PRs: **{fixable}**")
    by_sev = security.get("by_severity", {})
    if by_sev:
        sev_str = ", ".join(f"{s}: {c}" for s, c in sorted(by_sev.items()))
        lines.append(f"- By severity: {sev_str}")
    lines.append("")

lines.append("---")
lines.append("> 🔬 *Deterministic merge plan — generated from build-results.json. Refer to individual PR comments for full details.*")

print("\n".join(lines))
PYEOF
)

if [[ -n "$MERGE_PLAN_BODY" && "$MERGE_PLAN_BODY" != *"Traceback"* ]]; then
  # Find existing merge plan issue by title pattern (labels vary across repos)
  EXISTING_ISSUE=$(gh issue list --state open --json number,title \
    -q '.[] | select(.title | test("[Mm]erge [Pp]lan")) | .number' \
    2>/dev/null | head -1 || echo "")

  if [[ -n "$EXISTING_ISSUE" ]]; then
    # Update existing issue with latest data
    gh issue edit "$EXISTING_ISSUE" --body "$MERGE_PLAN_BODY" 2>/dev/null && \
      echo "  Updated merge plan issue #$EXISTING_ISSUE" || \
      echo "  ⚠️  Failed to update merge plan issue #$EXISTING_ISSUE"
  else
    # Create new merge plan issue — use "dependencies" label (exists in most repos)
    NEW_ISSUE=$(gh issue create \
      --title "📋 Breakability Merge Plan — $(date -u +%Y-%m-%d)" \
      --body "$MERGE_PLAN_BODY" \
      --label "dependencies" 2>/dev/null || echo "")
    if [[ -n "$NEW_ISSUE" ]]; then
      echo "  Created merge plan issue: $NEW_ISSUE"
    else
      echo "  ⚠️  Failed to create merge plan issue"
    fi
  fi
else
  echo "  ⚠️  Merge plan generation failed — skipping issue update"
fi
