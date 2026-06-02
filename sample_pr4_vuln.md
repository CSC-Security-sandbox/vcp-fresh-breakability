<!-- breakability-check -->
## 🚨 SECURITY RISK — `golang.org/x/oauth2` 0.32.0 → 0.36.0 · production · major ⚠️ (0.x unstable — treat as breaking)

Build: ✅ passes · Verification: **L4_tests_pass** · Usage: 1 file(s)

### 🚨 This PR introduces **13 NEW vulnerability(ies)** not present on `main`

**New CVEs:** GO-2025-4007,GO-2025-4008,GO-2025-4009,GO-2025-4010,GO-2025-4011,GO-2025-4012,GO-2025-4013,GO-2025-4014,GO-2025-4155,GO-2025-4175,GO-2026-4337,GO-2026-4340,GO-2026-4341
⚠️ **Reachable from your code:** [GO-2025-4007](https://pkg.go.dev/vuln/GO-2025-4007), [GO-2025-4008](https://pkg.go.dev/vuln/GO-2025-4008), [GO-2025-4009](https://pkg.go.dev/vuln/GO-2025-4009), [GO-2025-4010](https://pkg.go.dev/vuln/GO-2025-4010), [GO-2025-4011](https://pkg.go.dev/vuln/GO-2025-4011), [GO-2025-4012](https://pkg.go.dev/vuln/GO-2025-4012), [GO-2025-4013](https://pkg.go.dev/vuln/GO-2025-4013), [GO-2025-4014](https://pkg.go.dev/vuln/GO-2025-4014), [GO-2025-4155](https://pkg.go.dev/vuln/GO-2025-4155), [GO-2025-4175](https://pkg.go.dev/vuln/GO-2025-4175), [GO-2026-4337](https://pkg.go.dev/vuln/GO-2026-4337), [GO-2026-4340](https://pkg.go.dev/vuln/GO-2026-4340), [GO-2026-4341](https://pkg.go.dev/vuln/GO-2026-4341)

Pre-existing on main: 8 (unaffected by this PR).

**Recommendation:** Do **NOT** merge until these vulnerabilities are addressed. Options:
1. Bump to a later fixed version that patches these CVEs, or
2. Close this PR and wait for an upstream fix, or
3. If the vulnerable paths are not reachable from your code, document the risk and override with `breakability:override-security` label.

📋 Merge plan: #79
> 🚨 **Security:** This PR introduces **13 new vulnerability(ies)** not present on main: GO-2025-4007,GO-2025-4008,GO-2025-4009,GO-2025-4010,GO-2025-4011,GO-2025-4012,GO-2025-4013,GO-2025-4014,GO-2025-4155,GO-2025-4175,GO-2026-4337,GO-2026-4340,GO-2026-4341 (+ 8 pre-existing on main). **Review before merge.**

<details><summary>🔍 How we checked (verification: L4_tests_pass)</summary>

- ✅ Dependency resolved — `go get`/`npm install` exit 0
- ✅ Build passes — `targeted build (automations/tstctl module): 1 dirs` — exit 0, 0 new error(s)
- ✅ Tests pass (exit=0) — no regressions vs main
- ✅ Diffed error output: PR introduces 0 new diagnostics
- ℹ️ go.sum: 1 new transitive deps: golang.org/x/oauth2
- 🚨 govulncheck: **13 NEW vulnerability(ies) introduced by this PR** — GO-2025-4007,GO-2025-4008,GO-2025-4009,GO-2025-4010,GO-2025-4011,GO-2025-4012,GO-2025-4013,GO-2025-4014,GO-2025-4155,GO-2025-4175,GO-2026-4337,GO-2026-4340,GO-2026-4341 (+ 8 pre-existing on main)
</details>
<details><summary>📂 Files importing this package (1 file(s))</summary>

- `automations/tstctl/common/git.go`
</details>
<details><summary>🖥️ Build output (last lines)</summary>

```
  targeted build (automations/tstctl module): 1 dirs
    dirs: ./common/...
```
</details>

🔗 [View analysis run](https://github.com/CSC-Security-sandbox/vcp-vsa-breakability-test/actions/runs/26515416643)
> 🔬 *Deterministic analysis — govulncheck diffed against `main` baseline*
