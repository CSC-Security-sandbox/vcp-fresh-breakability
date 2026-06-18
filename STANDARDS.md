# Breakability Comment & Merge Plan Standards

**LAST UPDATED:** 2026-06-18 08:30 IST  
**STATUS:** LOCKED - DO NOT REGRESS FROM THESE STANDARDS

## 🎯 Gold Standard Examples

**NDM Repository (Node.js/TypeScript):**
- **PR #1:**   https://github.com/CSC-Security-sandbox/ndm-breakability-test/pull/1
- **PR #12:**  https://github.com/CSC-Security-sandbox/ndm-breakability-test/pull/12
- **PR #208:** https://github.com/CSC-Security-sandbox/ndm-breakability-test/pull/208 (MOST COMPLETE)

**These 3 comments are the BASELINE. Never post a comment below this quality.**

---

## ✅ Required Comment Sections (ALL MUST BE PRESENT)

### 1. Overall Verdict Header
```markdown
## [🟢/🟡/🟠/🔴] Breakability Analysis — [VERDICT]

**Package:** `name` X.Y.Z → A.B.C  
**Bump Type:** [patch/minor/major] · **Dep Type:** [dependency/devDependency] · **Priority:** P[0-2]  
**Verdict:** [✅ SAFE TO MERGE | ⚠️ REVIEW THEN MERGE | ❌ BLOCKED] · **Confidence:** [HIGH/MEDIUM/LOW]

**Headline:** [One-line summary]

**Recommendation:** [Clear action for developer]
```

**CRITICAL:** Verdict in header MUST match policy reasoning section. No contradictions like "SAFE" but "Risk: High".

---

### 2. Signal Summary Table
```markdown
### 📊 Signal Summary

| Layer | Result | Confidence | Evidence |
|-------|--------|------------|----------|
| 🔧 Build | [✅/⚠️/❌] **STATUS** | [HIGH/MED/LOW] | [brief evidence] |
| 🧪 Test | [✅/⚠️/❌] **STATUS** | [HIGH/MED/LOW] | [brief evidence] |
| 📝 API Diff | [✅/⚠️/❌] **STATUS** | [HIGH/MED/LOW] | [brief evidence] |
| 📋 Changelog | [✅/⚠️/❌] **STATUS** | [HIGH/MED/LOW] | [brief evidence] |
| 🔍 Reachability | [✅/⚠️/❌] **STATUS** | [HIGH/MED/LOW] | [brief evidence] |
| 🔬 Behavioral Probe | [✅/⚠️/⬜] **STATUS** | [HIGH/MED/LOW/N/A] | [brief evidence] |
| 🤖 AI Arbiter | [✅/⚠️/❌/⬜] **STATUS** | [HIGH/MED/LOW/N/A] | [brief evidence] |

**Signal Agreement:** X/Y signals agree → [VERDICT]
```

---

### 3. Build Analysis
```markdown
### 🔧 Build Analysis
**Status:** [✅ PASS | ⚠️ PRE-EXISTING | ❌ FAIL] | **Verification Level:** L[1-4]

**What we checked:**
- ✅ Dependencies resolved
- ✅ TypeScript/Go compilation
- ✅ Build artifacts generated
- [Additional checks]

**Build Output:**
[Code block with relevant logs]

**Confidence:** [HIGH/MEDIUM/LOW] — [Explanation]
```

---

### 4. Test Analysis
```markdown
### 🧪 Test Analysis
**Status:** [✅ PASS | ⚠️ SKIPPED | ❌ FAIL] | **Coverage:** [X/Y tests]

**What we checked:**
- [Test suite execution details]
- [Coverage maintained]
- [Regressions detected]

**Test Output:**
[Code block with relevant results]

**Confidence:** [HIGH/MEDIUM/LOW] — [Explanation]
```

---

### 5. API Diff Analysis
```markdown
### 📝 API Diff Analysis
**Status:** [✅ COMPATIBLE | ⚠️ MINOR-CHANGES | ❌ BREAKING] | **Tool:** [npm-apidiff/go-apidiff]

**What we checked:**
- Removed exports: **X** [list if any]
- Changed signatures: **Y** [list if any]
- Added exports: **Z** [list if safe]

**API Changes:**
```typescript
// REMOVED:
- export function foo()

// CHANGED:
~ export function bar(x: string): void

// ADDED:
+ export function baz()
```

**Confidence:** [HIGH/MEDIUM/LOW] — [Explanation]
```

---

### 6. Changelog Analysis
```markdown
### 📋 Changelog Analysis
**Status:** [✅ CLEAN | ⚠️ BEHAVIORAL | ❌ BREAKING] | **Source:** [GitHub Releases/CHANGELOG.md]

**Key Changes (from X → Y):**
- [Bullet points of important changes]
- 🚨 BREAKING: [if any]
- ✨ NEW: [if any]

**M8 Classification:** [BREAKING/BEHAVIORAL/ADDITIVE/BUGFIX]

**Confidence:** [HIGH/MEDIUM/LOW] — [Explanation]

**Independent verification:**
- GitHub Releases: [URL]
- CHANGELOG: [URL]
```

**CRITICAL:** Always include reference links to changelog sources.

---

### 7. Reachability Analysis
```markdown
### 🔍 Reachability Analysis
**Status:** [✅ NOT-REACHED | ⚠️ REACHED-UNKNOWN | ❌ REACHED-RELEVANT]

**What we checked:**
- Import scan: **X files** import this package
- Static analysis: [Details]

**Files Importing This Package:**
```
src/module/file1.ts
  Line 3: import { foo } from 'package';
  Line 12: const result = foo();

src/module/file2.ts
  Line 5: import { bar } from 'package';
```

**Callsite Impact:**
- Symbol `foo()` called at: `src/module/file1.ts:12`
- Symbol `bar` accessed at: `src/module/file2.ts:15`

**Confidence:** [HIGH/MEDIUM/LOW] — [Explanation]
```

**CRITICAL:** Show file:line details for callsites, not just file names.

---

### 8. Behavioral Probe (npm only)
```markdown
### 🔬 Behavioral Probe
**Status:** [✅ SAME-BEHAVIOR | ⚠️ DIFFERENT | ⬜ NOT-APPLICABLE] | **Method:** npm runtime-shape diff

**What we checked:**
- Installed versions: `package@old` vs `package@new`
- Export shape comparison: [SHA256 match/mismatch]

**Probe Results:**
```bash
# Probe commands (reproducible):
$ npm install --no-save --ignore-scripts package@old
$ npm install --no-save --ignore-scripts package@new
$ node npm-runtime-shape-probe.mjs

Old (X.Y.Z):
  shape_sha256: abc123...
  keys: N exports
  
New (A.B.C):
  shape_sha256: def456...
  keys: M exports

Match: [✅ YES | ❌ NO]
```

**Runtime Observations:**
- [Details of what changed]

**Confidence:** [HIGH/MEDIUM/LOW] — [Explanation]

**Independent verification:**
```bash
# You can reproduce locally:
[Bash commands to reproduce]
```
```

**CRITICAL:** Show SHA256 hashes and provide reproduction commands.

---

### 9. AI Arbiter Layer
```markdown
### 🤖 AI Arbiter Layer
**Status:** [✅ SAFE | ⚠️ REVIEW | ❌ BREAK | ⬜ NOT-APPLICABLE]

**Why [applied/skipped]:** [Explanation]

[IF APPLIED:]
**AI Reasoning:**
```
Question: [What was asked]

AI Response: [What AI determined and why]
```

**Verdict:** [SAFE/REVIEW/BREAK] — [Explanation]

**Confidence:** [HIGH/MEDIUM/LOW] — [Explanation]
```

---

### 10. Policy Decision
```markdown
### 🧮 Policy Decision
**How the verdict was reached:**

1. **Build Signal:** [STATUS] → [contributes SAFE/NEUTRAL/BLOCK]
2. **Test Signal:** [STATUS] → [contributes SAFE/NEUTRAL/BLOCK]
3. **API Diff:** [STATUS] → [contributes SAFE/NEUTRAL/BLOCK]
4. **Changelog:** [STATUS] → [escalates/confirms X]
5. **Reachability:** [STATUS] → [OVERRIDES/confirms X]
6. **Probe:** [STATUS] → [corroborates X]
7. **AI Arbiter:** [STATUS] → [confirms X]

**Final Verdict Logic:**
```
IF [condition]:
    → [VERDICT] ([reason])
ELIF [condition]:
    → [VERDICT] ([reason])
ELSE:
    → [VERDICT] ([reason])
```

**Applied rule:** [Which rule above was used]

**Confidence Calculation:**
- Build confidence: [HIGH/MEDIUM/LOW] ([why])
- Signal agreement: [X/Y signals agree]
- Zero-false-green guarantee: ✅ [Explanation]

**Risk Assessment:**
- Breaking change risk: [NONE/LOW/MEDIUM/HIGH] ([why])
- Regression risk: [NONE/LOW/MEDIUM/HIGH] ([why])
- Security risk: [NONE/LOW/MEDIUM/HIGH] ([why])
```

**CRITICAL:** Show step-by-step logic, not just final verdict. Explain WHY contradictions resolve (e.g., NOT-REACHED overrides BREAKING).

---

### 11. Final Recommendation
```markdown
### 🎯 Final Recommendation

**Action:** [✅ MERGE | ⚠️ REVIEW-THEN-MERGE | ❌ FIX-THEN-MERGE]

**Why this is [safe/needs review/blocked]:**
[Synthesize all layers into coherent narrative]

**Next steps:**
1. [Action item 1]
2. [Action item 2]

**Evidence strength:** [HIGH/MEDIUM/LOW] ([which layers provided decisive evidence])

[IF REVIEW NEEDED:]
**What to review:**
1. [Specific thing to check]
2. [Specific thing to check]

**Estimated review time:** [X minutes]
```

---

### 12. Independent Verification Resources
```markdown
### 📚 Independent Verification Resources

**For developers who want to verify this analysis:**

1. **Changelog Source:**
   - GitHub Releases: [URL]
   - CHANGELOG.md: [URL]
   - [Other sources]

2. **API Diff Tool:**
   ```bash
   # Run locally:
   npx npm-diff-ts package@old package@new
   
   # Or for Go:
   go-apidiff old-module new-module
   ```

3. **Behavioral Probe (reproduce):**
   ```bash
   cd /tmp && npm init -y
   npm install package@old
   node -e "console.log(Object.keys(require('package')))"
   npm install package@new
   node -e "console.log(Object.keys(require('package')))"
   ```

4. **Reachability Check:**
   ```bash
   git grep -n "from 'package'" src/
   git grep -n "require('package')" src/
   ```

5. **Callsite Inspection:**
   ```bash
   cat [file] | grep -A5 -B5 "[symbol]"
   ```

6. **Analysis Run Logs:**
   - GitHub Actions: [URL to run]
   - Build results: [Available in artifacts]
   - Probe output: [Available in logs]
```

**CRITICAL:** Always provide reference links and reproduction commands.

---

### 13. Footer
```markdown
---

📋 **Merge Plan:** [#issue-number](URL)  
🔗 **Analysis Run:** [Actions](URL)  
🔬 **Mode:** Deterministic + [AI/Probe] · **Model:** [if AI used] · **Analyzed:** [timestamp]

---

**💡 About this analysis:**
This comment was generated by the [NDM/VCP] Breakability Pipeline, which combines 7 independent evidence layers to provide high-confidence merge recommendations. The goal is to reduce developer review time by 85% while maintaining zero false-greens (never auto-clearing truly breaking changes).
```

---

## 🚫 What NOT To Do (Anti-Patterns)

### ❌ Contradictions
**NEVER:**
- Say "LIKELY SAFE" in headline but "Risk: HIGH" in body
- Say "SAFE TO MERGE" but list "Breaking Changes" without explaining override
- Show multiple warning signals without explaining final verdict

**ALWAYS:**
- If signals conflict, explain resolution in Policy Decision section
- If NOT-REACHED overrides BREAKING, state it explicitly
- If probe DIFFERENT but still SAFE, explain why

### ❌ Missing Evidence
**NEVER:**
- Skip showing code blocks for API changes
- Say "see changelog" without providing link
- Say "callsites found" without showing file:line
- Say "probe ran" without showing SHA256 hashes

**ALWAYS:**
- Show actual code/output samples
- Provide clickable reference links
- Show file:line for callsites
- Show SHA256 hashes for probe results

### ❌ Weak Confidence
**NEVER:**
- Say "might be safe" or "probably works"
- Use vague terms like "some changes"
- Say "needs review" without specifics

**ALWAYS:**
- State HIGH/MEDIUM/LOW confidence with reason
- Quantify changes ("3 exports removed")
- If REVIEW needed, list exactly what to review

---

## 📋 Merge Plan Standards

### Required Structure

```markdown
# 📋 Breakability Merge Plan

**Generated:** [timestamp] (deterministic [+ AI if used])  
**PRs analyzed:** X Dependabot PRs  
**Not analyzed:** Y non-Dependabot PRs (out of scope)

> ⏱️ **Snapshot** generated at [timestamp]. PR states may have changed since analysis.

## ⚡ What to Do Next

> **TLDR:** Jump to [Developer Action Summary](#developer-action-summary)

- 🛑 **Fix first:** X PR(s) have blocking issues
- 🔐 **Priority merge:** Y PR(s) fix CVEs
- 🔴 **Review required:** Z PR(s) need careful review

<details><summary><strong>📊 Technical Details</strong></summary>

## Summary by Verification Level

| Category | Count |
|----------|-------|
| ✅ Safe to merge — tests pass (L4) | X |
| ✅ Build passes — review recommended (L2/L3) | Y |
| ⚠️ Review required | Z |
| ❌ Fix required | A |
| 🚫 Cancelled / Incomplete | B |

## Breakability Summary

🔴 **High:** X · 🟠 **Medium:** Y · 🟡 **Low:** Z · 🟢 **None:** A

</details>

## Developer Action Summary

**Plain-English merge guidance:**

1. **[ACTION] — [PRs]:** [explanation]
2. **[ACTION] — [PRs]:** [explanation]

## 🔴 Security — CVEs Fixed

| PR | Package | Version | CVE(s) | Severity | Status |
|----|---------|---------|--------|----------|--------|
| [links to each PR] | | | | | |

## ✅ Safe to Merge (L4 verified)

| PR | Package | Version | Bump | Verification |
|----|---------|---------|------|-------------|
| [table rows] | | | | |

## ⚠️ Review Required

| PR | Package | Version | Bump | Merge Risk | Why Review |
|----|---------|---------|------|------------|------------|
| [table rows] | | | | | |

## ❌ Fix Required

| PR | Package | Version | Issue |
|----|---------|---------|-------|
| [table rows] | | | |

## 🚫 Cancelled / Incomplete

- PR #X `package` — analysis incomplete [reason if known]

---
> 🔬 *Deterministic merge plan — generated from build-results.json*
```

### ❌ Merge Plan Anti-Patterns

**NEVER:**
- Group 95%+ of PRs in "Cancelled/Incomplete" (means the run didn't actually analyze them)
- Create messy tables with inconsistent columns
- Use vague status like "might need review"
- Skip CVE section if CVEs exist

**ALWAYS:**
- Only list PRs in "Cancelled" that were actually requested but failed
- Keep tables clean with consistent structure
- Be decisive: SAFE/REVIEW/BLOCKED, never "maybe"
- Highlight CVE fixes prominently

---

## 🔒 Lock These Standards

**Save locations:**
1. `/Users/hpoornac/.copilot/session-state/.../STANDARDS.md` (this file)
2. `/tmp/showcase_comment_template.md` (full template)
3. `/tmp/showcase_pr208_uuid.md` (complete example)

**Before posting ANY comment:**
1. Check against PR#208 standard
2. Verify all 13 sections present
3. Ensure reference links included
4. Confirm no contradictions
5. Validate file:line callsites shown

**Before posting ANY merge plan:**
1. Check section structure
2. Verify tables are clean
3. Ensure CVE section present if applicable
4. Confirm no 95%+ cancelled (real run, not stub)

---

**Last reviewed:** 2026-06-18 08:30 IST  
**Status:** ✅ LOCKED — Do not regress below this quality
